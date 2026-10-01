import type { AnyTRPCRouter, inferRouterContext } from "@trpc/server"
import {
  callTRPCProcedure,
  getTRPCErrorFromUnknown,
  getTRPCErrorShape,
  isTrackedEnvelope,
  TRPCError,
} from "@trpc/server"
import { isObservable, observableToAsyncIterable } from "@trpc/server/observable"

import type { MultiplexMessage, SubscriptionRequest } from "../protocol"
import { encodeMessage, parseRequest, UNKNOWN_CONNECTION_STATUS } from "../protocol"

export interface MultiplexServerOptions<TRouter extends AnyTRPCRouter> {
  router: TRouter
  /**
   * How long a request waits for its subscriptions to start before responding.
   *
   * Waiting lets cookies set by middleware reach the response;
   * the limit keeps a hanging procedure from blocking the stream and further subscription changes.
   * @default 5000
   */
  startTimeoutMs?: number
  /** Called for every subscription error before it is sent to the client. */
  onError?: (opts: MultiplexErrorHandlerOptions<inferRouterContext<TRouter>>) => void
}

export interface MultiplexErrorHandlerOptions<TContext> {
  error: TRPCError
  path: string
  input: unknown
  ctx: TContext
  req: Request
}

export interface MultiplexHandleOptions<TRouter extends AnyTRPCRouter> {
  req: Request
  /** Called once per request that starts subscriptions; all of them share the context. */
  createContext: () => Promise<inferRouterContext<TRouter>>
}

export interface MultiplexServer<TRouter extends AnyTRPCRouter> {
  /** Handle a request to the multiplex endpoint. */
  handle: (opts: MultiplexHandleOptions<TRouter>) => Promise<Response>
}

interface Connection<TContext> {
  readonly id: string
  /** Host of the request that opened the connection; updates from other hosts do not see it. */
  readonly host: string
  readonly stream: ReadableStream<Uint8Array>
  start: (request: SubscriptionRequest, ctx: TContext, req: Request) => Promise<void>
  stop: (id: string) => void
  close: () => void
}

const STREAM_HEADERS = {
  "content-type": "text/event-stream",
  "cache-control": "no-cache, no-transform",
  // Ask nginx-like proxies not to buffer the stream.
  "x-accel-buffering": "no",
}

const ABORTED = Symbol("aborted")

/**
 * Create a server that runs tRPC subscriptions of many client operations over a single HTTP stream.
 *
 * Open connections live in the memory of this server instance.
 * An `update` request that reaches another instance gets {@link UNKNOWN_CONNECTION_STATUS},
 * and the client reopens the stream with its whole subscription set.
 *
 * Keepalive pings and the client inactivity timeout come from the router `sse` config.
 */
export function createMultiplexServer<TRouter extends AnyTRPCRouter>(
  opts: MultiplexServerOptions<TRouter>,
): MultiplexServer<TRouter> {
  type TContext = inferRouterContext<TRouter>

  const { router, onError, startTimeoutMs = 5000 } = opts
  const config = router._def._config
  const connections = new Map<string, Connection<TContext>>()

  function createConnection(host: string): Connection<TContext> {
    const id = crypto.randomUUID()
    const closed = new AbortController()
    const subscriptions = new Map<string, AbortController>()
    const encoder = new TextEncoder()
    let controller!: ReadableStreamDefaultController<Uint8Array>

    const stream = new ReadableStream<Uint8Array>({
      start(streamController) {
        controller = streamController
      },
      cancel() {
        // A cancelled stream rejects `controller.close()`, so only the subscriptions are released.
        release()
      },
    })

    function send(message: MultiplexMessage) {
      if (!closed.signal.aborted) {
        controller.enqueue(encoder.encode(encodeMessage(message)))
      }
    }

    send({
      type: "connected",
      connectionId: id,
      reconnectAfterInactivityMs: config.sse?.client?.reconnectAfterInactivityMs,
    })

    const ping = config.sse?.ping?.enabled
      ? setInterval(() => send({ type: "ping" }), config.sse.ping.intervalMs ?? 1000)
      : undefined

    function release() {
      if (closed.signal.aborted) {
        return false
      }

      closed.abort()
      clearInterval(ping)
      connections.delete(id)
      subscriptions.clear()
      return true
    }

    function close() {
      if (release()) {
        controller.close()
      }
    }

    function stop(subscriptionId: string) {
      subscriptions.get(subscriptionId)?.abort()
      subscriptions.delete(subscriptionId)
    }

    async function start(request: SubscriptionRequest, ctx: TContext, req: Request) {
      // A repeated id replaces the subscription, e.g. after a retryable error.
      stop(request.id)

      const subscription = new AbortController()
      subscriptions.set(request.id, subscription)
      const signal = AbortSignal.any([closed.signal, subscription.signal])
      let input: unknown

      function fail(cause: unknown) {
        if (subscriptions.get(request.id) === subscription) {
          subscriptions.delete(request.id)
        }
        // Release listeners the procedure bound to the signal.
        subscription.abort()

        const error = getTRPCErrorFromUnknown(cause)
        onError?.({ error, path: request.path, input, ctx, req })

        const shape = getTRPCErrorShape({
          config,
          ctx,
          error,
          input,
          path: request.path,
          type: "subscription",
        })
        send({ type: "error", id: request.id, error: config.transformer.output.serialize(shape) })
      }

      try {
        input = config.transformer.input.deserialize(request.input)
        const result: unknown = await callTRPCProcedure({
          router,
          path: request.path,
          getRawInput: async () => input,
          ctx,
          type: "subscription",
          signal,
          batchIndex: 0,
        })

        if (signal.aborted) {
          return
        }

        const iterable = isObservable(result) ? observableToAsyncIterable(result, signal) : result
        if (!isAsyncIterable(iterable)) {
          throw new TRPCError({
            code: "INTERNAL_SERVER_ERROR",
            message: `Subscription ${request.path} did not return an observable or an AsyncIterable`,
          })
        }

        send({ type: "started", id: request.id })

        // The request that started the subscription completes now, while its events keep flowing into the stream.
        pump(request.id, iterable, signal).catch(cause => {
          if (!signal.aborted) {
            fail(cause)
          }
        })
      } catch (cause) {
        if (!signal.aborted) {
          fail(cause)
        }
      }
    }

    async function pump(
      subscriptionId: string,
      iterable: AsyncIterable<unknown>,
      signal: AbortSignal,
    ) {
      const iterator = iterable[Symbol.asyncIterator]()
      let done = false

      try {
        while (true) {
          const result = await nextOrAbort(iterator, signal)
          if (result === ABORTED) {
            return
          }

          if (result.done) {
            done = true
            subscriptions.delete(subscriptionId)
            send({ type: "stopped", id: subscriptionId })
            return
          }

          const { value } = result
          send(
            isTrackedEnvelope(value)
              ? {
                  type: "data",
                  id: subscriptionId,
                  eventId: value[0],
                  data: config.transformer.output.serialize(value[1]),
                }
              : {
                  type: "data",
                  id: subscriptionId,
                  data: config.transformer.output.serialize(value),
                },
          )
        }
      } finally {
        // Run the generator `finally` on abort and on errors thrown here, e.g. by serialization.
        if (!done) {
          iterator.return?.().catch(() => {})
        }
      }
    }

    return { id, host, stream, start, stop, close }
  }

  async function handle({ req, createContext }: MultiplexHandleOptions<TRouter>) {
    if (req.method !== "POST") {
      return new Response(null, { status: 405, headers: { allow: "POST" } })
    }

    // A cross-origin page can send a credentialed `text/plain` POST without a preflight and attach
    // a subscription with the victim's session to the attacker's stream; JSON requires a preflight.
    if (
      req.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase() !== "application/json"
    ) {
      return new Response(null, { status: 415 })
    }

    const request = parseRequest(await req.json().catch(() => undefined))
    if (!request) {
      return new Response(null, { status: 400 })
    }

    const host = new URL(req.url).host

    if (request.type === "update") {
      const connection = connections.get(request.connectionId)
      if (connection?.host !== host) {
        return new Response(null, { status: UNKNOWN_CONNECTION_STATUS })
      }

      request.remove.forEach(id => connection.stop(id))

      if (request.add.length) {
        const ctx = await createContext()
        await waitForStart(
          request.add.map(subscription => connection.start(subscription, ctx, req)),
        )
      }

      return new Response(null, { status: 204 })
    }

    const ctx = await createContext()
    const connection = createConnection(host)
    connections.set(connection.id, connection)
    req.signal.addEventListener("abort", () => connection.close(), { once: true })

    // Messages are queued in the stream until the response is read, so `started` events are not lost.
    await waitForStart(
      request.subscriptions.map(subscription => connection.start(subscription, ctx, req)),
    )

    return new Response(connection.stream, { headers: STREAM_HEADERS })
  }

  async function waitForStart(starts: Promise<void>[]) {
    let timer: ReturnType<typeof setTimeout> | undefined
    await Promise.race([
      Promise.all(starts),
      new Promise(resolve => {
        timer = setTimeout(resolve, startTimeoutMs)
      }),
    ])
    clearTimeout(timer)
  }

  return { handle }
}

/**
 * Wait for the next value unless the signal aborts first.
 *
 * A silent subscription never settles `next()`, so abort has to win on its own.
 * The abort listener is removed after every step: racing against one long-lived promise
 * would retain a reaction per event for the whole life of the subscription.
 */
async function nextOrAbort<T>(iterator: AsyncIterator<T>, signal: AbortSignal) {
  if (signal.aborted) {
    return ABORTED
  }

  return await new Promise<IteratorResult<T> | typeof ABORTED>((resolve, reject) => {
    const onAbort = () => resolve(ABORTED)
    signal.addEventListener("abort", onAbort, { once: true })
    iterator
      .next()
      .then(resolve, reject)
      .finally(() => signal.removeEventListener("abort", onAbort))
  })
}

function isAsyncIterable(value: unknown): value is AsyncIterable<unknown> {
  return typeof value === "object" && value !== null && Symbol.asyncIterator in value
}
