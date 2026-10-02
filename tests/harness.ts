import { EventEmitter, on } from "node:events"

import { createTRPCClient } from "@trpc/client"
import { initTRPC, tracked, TRPCError } from "@trpc/server"
import { multiplexLink } from "trpc-multiplex/client"
import type { MultiplexServerOptions } from "trpc-multiplex/server"
import { createMultiplexServer } from "trpc-multiplex/server"

/** Context of the test router: the user of the request that started the subscription. */
export interface Context {
  user: string
}

/**
 * A real multiplex server on a random port, wrapped so tests can control it and see what the client does with it.
 *
 * The tests run a real tRPC client against it and check behavior from both ends:
 * what the subscriptions receive, and how many streams the client opens and keeps.
 */
export interface TestServer {
  /** Endpoint to pass to the link. */
  url: string
  /** Event source of the router's procedures: emitting a topic sends an event to the subscriptions of it. */
  emitter: EventEmitter
  /** Number of `open` requests, i.e. streams the client has opened. */
  openedStreams: () => number
  /** Number of streams currently held open by the client. */
  activeStreams: () => number
  /** Change the user of the context created for the following requests, as a login would. */
  setUser: (user: string) => void
  /** Make the next `createContext` throw the error. */
  failNextContext: (error: unknown) => void
  /** End all open streams as a server restart would. */
  dropStreams: () => void
}

/** Run by `afterEach` of every test file. */
export const cleanups: (() => void)[] = []

/** Router with a procedure for every subscription behavior the tests need: endless, tracked, finite, failing, hanging, etc. */
export function createRouter(opts: { reconnectAfterInactivityMs?: number } = {}) {
  const t = initTRPC.context<Context>().create({
    sse: { client: { reconnectAfterInactivityMs: opts.reconnectAfterInactivityMs } },
  })

  let flakyStarts = 0

  return t.router({
    /** Fails to start twice with a retryable error, then works. */
    flaky: t.procedure.subscription(async function* () {
      if (++flakyStarts <= 2) {
        throw new TRPCError({ code: "SERVICE_UNAVAILABLE" })
      }
      yield "started"
    }),

    events: t.procedure
      .input((value: unknown) => value as { topic: string; lastEventId?: string })
      .subscription(async function* ({ input, ctx, signal }) {
        for await (const [payload] of on(emitter(ctx), input.topic, { signal })) {
          yield { user: ctx.user, payload: payload as unknown }
        }
      }),

    tracked: t.procedure
      .input((value: unknown) => value as { lastEventId?: string })
      .subscription(async function* ({ input, ctx, signal }) {
        yield tracked("resumed", input.lastEventId ?? null)
        for await (const [id] of on(emitter(ctx), "tracked", { signal })) {
          yield tracked(id as string, id as string)
        }
      }),

    finite: t.procedure.subscription(async function* () {
      yield 1
      yield 2
    }),

    unserializable: t.procedure.subscription(async function* ({ ctx }) {
      try {
        yield 1n
      } finally {
        emitter(ctx).emit("cleaned-up")
      }
    }),

    hanging: t.procedure.subscription(() => new Promise<never>(() => {})),

    forbidden: t.procedure.subscription(() => {
      throw new TRPCError({ code: "UNAUTHORIZED" })
    }),

    /** Reports its start, so a test can tell that the server ran it. */
    announced: t.procedure.subscription(async function* ({ ctx, signal }) {
      emitter(ctx).emit("announced")
      await new Promise(resolve => signal?.addEventListener("abort", resolve))
      yield "never"
    }),

    cleanup: t.procedure.subscription(async function* ({ ctx, signal }) {
      try {
        yield "ready"
        await new Promise(resolve => signal?.addEventListener("abort", resolve))
      } finally {
        emitter(ctx).emit("cleaned-up")
      }
    }),
  })
}

export type Router = ReturnType<typeof createRouter>

// The emitter is passed through the context so that every test has its own event source.
const emitters = new WeakMap<Context, EventEmitter>()
export function emitter(ctx: Context) {
  return emitters.get(ctx)!
}

/** Start a multiplex server for the router; it is stopped after the test. */
export function startServer(
  router: Router,
  opts: {
    /** Server that handles `update` requests instead, as another instance behind a load balancer would. */
    routeUpdatesTo?: ReturnType<typeof createMultiplexServer<Router>>
    startTimeoutMs?: number
    /** Number of first `open` requests to reject as an unavailable server would. */
    rejectedOpens?: number
    /** Delay of every `createContext`, e.g. to let the client time out meanwhile. */
    contextDelayMs?: number
    /** Delay of the response to the first `update` request, after the server has applied it. */
    firstUpdateDelayMs?: number
    onError?: MultiplexServerOptions<Router>["onError"]
  } = {},
): TestServer {
  const events = new EventEmitter()
  const multiplex = createMultiplexServer({
    router,
    startTimeoutMs: opts.startTimeoutMs,
    onError: opts.onError,
  })
  const streams = new Set<() => void>()
  let user = "alice"
  let contextError: { error: unknown } | undefined
  let opened = 0
  let rejectedOpens = opts.rejectedOpens ?? 0
  let updateDelayMs = opts.firstUpdateDelayMs ?? 0

  const httpServer = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = (await req
        .clone()
        .json()
        .catch(() => ({}))) as { type?: string }
      if (body.type === "open" && rejectedOpens > 0) {
        rejectedOpens--
        return new Response(null, { status: 503 })
      }

      const target = body.type === "update" && opts.routeUpdatesTo ? opts.routeUpdatesTo : multiplex

      const response = await target.handle({
        req,
        createContext: async () => {
          if (opts.contextDelayMs) {
            await Bun.sleep(opts.contextDelayMs)
          }
          if (contextError) {
            const { error } = contextError
            contextError = undefined
            throw error
          }
          const ctx = { user }
          emitters.set(ctx, events)
          return ctx
        },
      })

      if (body.type === "update" && updateDelayMs) {
        await Bun.sleep(updateDelayMs)
        updateDelayMs = 0
      }

      if (body.type !== "open" || !response.body) {
        return response
      }

      opened++
      return new Response(breakable(response.body!, streams), response)
    },
  })

  cleanups.push(() => httpServer.stop(true))

  return {
    url: httpServer.url.href,
    emitter: events,
    openedStreams: () => opened,
    activeStreams: () => streams.size,
    setUser: value => {
      user = value
    },
    failNextContext: error => {
      contextError = { error }
    },
    dropStreams: () => {
      streams.forEach(drop => drop())
    },
  }
}

/** Proxy the stream so that a test can end it and observe when the client stops reading it. */
function breakable(source: ReadableStream<Uint8Array>, streams: Set<() => void>) {
  const reader = source.getReader()
  let drop: () => void

  return new ReadableStream<Uint8Array>({
    start(controller) {
      drop = () => {
        streams.delete(drop)
        controller.close()
        void reader.cancel()
      }
      streams.add(drop)
    },
    async pull(controller) {
      const { done, value } = await reader.read()
      if (done) {
        streams.delete(drop)
        controller.close()
      } else {
        controller.enqueue(value)
      }
    },
    async cancel(reason) {
      streams.delete(drop)
      await reader.cancel(reason)
    },
  })
}

/** Create a tRPC client that subscribes through `multiplexLink` to the server. */
export function connect(
  server: Pick<TestServer, "url">,
  retryDelayMs: (attemptIndex: number) => number = () => 0,
  requestTimeoutMs?: number,
) {
  return createTRPCClient<Router>({
    links: [multiplexLink({ url: server.url, retryDelayMs, requestTimeoutMs })],
  })
}

/** Poll until the condition holds; fail after the timeout. */
export async function until(condition: () => boolean, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error("Condition was not met in time")
    }
    await Bun.sleep(5)
  }
}
