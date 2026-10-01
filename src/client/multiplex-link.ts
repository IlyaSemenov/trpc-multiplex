import type { Operation, OperationResultEnvelope, TRPCLink } from "@trpc/client"
import { TRPCClientError } from "@trpc/client"
import type { TransformerOptions, TRPCConnectionState } from "@trpc/client/unstable-internals"
import { getTransformer } from "@trpc/client/unstable-internals"
import type { Observer } from "@trpc/server/observable"
import { observable } from "@trpc/server/observable"
import type { TRPCErrorShape } from "@trpc/server/rpc"
import type {
  AnyClientTypes,
  inferClientTypes,
  InferrableClientTypes,
} from "@trpc/server/unstable-core-do-not-import"
import { retryableRpcCodes } from "@trpc/server/unstable-core-do-not-import"

import type { MultiplexMessage, MultiplexRequest, SubscriptionRequest } from "../protocol"
import { decodeMessages, UNKNOWN_CONNECTION_STATUS } from "../protocol"

export type MultiplexLinkOptions<TRoot extends AnyClientTypes> = {
  /** URL of the multiplex endpoint served by `createMultiplexServer`. */
  url: string
  /**
   * Delay before a reconnect or a subscription restart, by 0-based index of the consecutive attempt.
   * @default exponential backoff from 1 to 30 seconds
   */
  retryDelayMs?: (attemptIndex: number) => number
} & TransformerOptions<TRoot>

type AnyClientError = TRPCClientError<any>
type ConnectionState = TRPCConnectionState<AnyClientError>

interface ClientSubscription {
  readonly id: string
  readonly op: Operation
  readonly observer: Observer<OperationResultEnvelope<unknown, AnyClientError>, AnyClientError>
  state: ConnectionState
  /** Id of the last `tracked()` event, sent back on restart so the procedure can resume. */
  lastEventId?: string
  /** Pending restart after a retryable error. */
  retryTimer?: ReturnType<typeof setTimeout>
  /**
   * Consecutive retryable errors since the subscription last delivered data.
   * Not reset on `started`: a generator procedure that throws immediately fails after it has started.
   */
  failedAttempts: number
  /** Remove the subscription from the client set and release its resources. */
  dispose: () => void
}

interface Connection {
  readonly abort: AbortController
  /** Subscriptions the server has been asked to run on this connection. */
  readonly sent: Set<string>
  /** Server-assigned id, known once the stream is connected. */
  id?: string
  updating: boolean
  inactivityMs?: number
  watchdog?: ReturnType<typeof setTimeout>
}

function defaultRetryDelayMs(attemptIndex: number) {
  return Math.min(1000 * 2 ** attemptIndex, 30_000)
}

/**
 * Terminating link that runs all subscriptions of a tRPC client over a single HTTP stream.
 *
 * Use it for subscriptions only, e.g. as the `true` branch of `splitLink`.
 * Each change of the subscription set is a short request with the current cookies,
 * so the stream itself never needs to be reopened while it is alive.
 */
export function multiplexLink<TInferrable extends InferrableClientTypes>(
  opts: MultiplexLinkOptions<inferClientTypes<TInferrable>>,
): TRPCLink<TInferrable> {
  return () => {
    const client = createMultiplexClient(opts)

    return ({ op }) =>
      observable(observer => {
        if (op.type !== "subscription") {
          throw new Error("multiplexLink only supports subscriptions")
        }

        return client.subscribe(op, observer)
      })
  }
}

function createMultiplexClient(opts: MultiplexLinkOptions<AnyClientTypes>) {
  const transformer = getTransformer(opts.transformer)
  const retryDelayMs = opts.retryDelayMs ?? defaultRetryDelayMs
  const subscriptions = new Map<string, ClientSubscription>()
  let lastSubscriptionId = 0
  let connection: Connection | undefined
  let flushTimer: ReturnType<typeof setTimeout> | undefined
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined
  let failedAttempts = 0

  function subscribe(op: Operation, observer: ClientSubscription["observer"]) {
    const subscription: ClientSubscription = {
      id: String(++lastSubscriptionId),
      op,
      observer,
      state: { type: "state", state: "connecting", error: null },
      failedAttempts: 0,
      dispose: () => {
        clearTimeout(subscription.retryTimer)
        op.signal?.removeEventListener("abort", onAbort)
        subscriptions.delete(subscription.id)
        scheduleFlush()
      },
    }

    function onAbort() {
      complete(subscription)
    }

    subscriptions.set(subscription.id, subscription)
    observer.next({ result: subscription.state })
    scheduleFlush()

    if (op.signal?.aborted) {
      complete(subscription)
    } else {
      op.signal?.addEventListener("abort", onAbort, { once: true })
    }

    return subscription.dispose
  }

  /** Finish a subscription the way `httpSubscriptionLink` does when its stream ends. */
  function complete(subscription: ClientSubscription) {
    subscription.dispose()
    subscription.observer.next({ result: { type: "stopped" } })
    setState(subscription, { type: "state", state: "idle", error: null })
    subscription.observer.complete()
  }

  /** Batch subscription changes of the current task, e.g. unmount and mount on navigation, into one request. */
  function scheduleFlush() {
    flushTimer ??= setTimeout(() => {
      flushTimer = undefined
      flush()
    })
  }

  /** Bring the server subscription set in line with the client one. */
  function flush() {
    if (!connection) {
      if (subscriptions.size && !reconnectTimer) {
        open()
      }
      return
    }

    if (!subscriptions.size) {
      close()
      return
    }

    // Changes made meanwhile are flushed again once the stream is connected or the running update completes.
    if (!connection.id || connection.updating) {
      return
    }

    const add = [...subscriptions.values()].filter(
      subscription => !connection!.sent.has(subscription.id) && !subscription.retryTimer,
    )
    const remove = [...connection.sent].filter(id => !subscriptions.has(id))
    if (!add.length && !remove.length) {
      return
    }

    add.forEach(subscription => connection!.sent.add(subscription.id))
    remove.forEach(id => connection!.sent.delete(id))
    void update(connection, {
      type: "update",
      connectionId: connection.id,
      add: add.map(toRequest),
      remove,
    })
  }

  async function update(target: Connection, request: MultiplexRequest) {
    target.updating = true

    const response = await post(request).catch((error: unknown) => error)
    if (target !== connection) {
      return
    }

    target.updating = false

    if (response instanceof Response && response.ok) {
      flush()
    } else if (response instanceof Response && response.status === UNKNOWN_CONNECTION_STATUS) {
      // The request reached a server that does not hold the stream: reopen it with the whole set.
      close()
      flush()
    } else {
      disconnect(toClientError(response))
    }
  }

  function open() {
    const target: Connection = {
      abort: new AbortController(),
      sent: new Set(),
      updating: false,
    }
    connection = target

    subscriptions.forEach(subscription => {
      clearTimeout(subscription.retryTimer)
      subscription.retryTimer = undefined
      target.sent.add(subscription.id)
    })

    void read(target, {
      type: "open",
      subscriptions: [...subscriptions.values()].map(toRequest),
    })
  }

  async function read(target: Connection, request: MultiplexRequest) {
    let error: AnyClientError | null = null

    try {
      const response = await post(request, target.abort.signal)
      if (!response.ok || !response.body) {
        throw toClientError(response)
      }

      for await (const message of decodeMessages(response.body)) {
        if (target !== connection) {
          return
        }

        resetWatchdog(target)
        receive(target, message)
      }
    } catch (cause) {
      error = toClientError(cause)
    }

    // A stream closed by the server is a disconnect as well: the server is restarting or shutting down.
    if (target === connection) {
      disconnect(error)
    }
  }

  function receive(target: Connection, message: MultiplexMessage) {
    if (message.type === "ping") {
      return
    }

    if (message.type === "connected") {
      target.id = message.connectionId
      target.inactivityMs = message.reconnectAfterInactivityMs
      failedAttempts = 0
      resetWatchdog(target)
      flush()
      return
    }

    const subscription = subscriptions.get(message.id)
    if (!subscription || !target.sent.has(message.id)) {
      return
    }

    switch (message.type) {
      case "started": {
        subscription.observer.next({ result: { type: "started" } })
        setState(subscription, { type: "state", state: "pending", error: null })
        break
      }

      case "data": {
        subscription.failedAttempts = 0
        const data = transformer.output.deserialize(message.data)
        if (message.eventId === undefined) {
          subscription.observer.next({ result: { data } })
        } else {
          subscription.lastEventId = message.eventId
          subscription.observer.next({
            result: { id: message.eventId, data: { id: message.eventId, data } },
          })
        }
        break
      }

      case "error": {
        const shape = transformer.output.deserialize(message.error) as TRPCErrorShape
        const error = TRPCClientError.from({ error: shape })
        target.sent.delete(subscription.id)

        if (retryableRpcCodes.includes(shape.code)) {
          setState(subscription, { type: "state", state: "connecting", error })
          subscription.retryTimer = setTimeout(
            () => {
              subscription.retryTimer = undefined
              scheduleFlush()
            },
            retryDelayMs(subscription.failedAttempts++),
          )
        } else {
          subscription.dispose()
          subscription.observer.error(error)
        }
        break
      }

      case "stopped": {
        target.sent.delete(subscription.id)
        complete(subscription)
        break
      }
    }
  }

  function resetWatchdog(target: Connection) {
    clearTimeout(target.watchdog)
    if (!target.inactivityMs) {
      return
    }

    const ms = target.inactivityMs
    target.watchdog = setTimeout(() => {
      if (target === connection) {
        disconnect(new TRPCClientError(`Timeout of ${ms}ms reached while waiting for a response`))
      }
    }, ms)
  }

  /** Drop the current stream and reconnect after a growing delay. */
  function disconnect(error: AnyClientError | null) {
    close()
    subscriptions.forEach(subscription =>
      setState(subscription, { type: "state", state: "connecting", error }),
    )

    reconnectTimer = setTimeout(() => {
      reconnectTimer = undefined
      flush()
    }, retryDelayMs(failedAttempts++))
  }

  function close() {
    if (!connection) {
      return
    }

    clearTimeout(connection.watchdog)
    connection.abort.abort()
    connection = undefined
  }

  function setState(subscription: ClientSubscription, state: ConnectionState) {
    if (state.state === subscription.state.state && !state.error && !subscription.state.error) {
      return
    }

    subscription.state = state
    subscription.observer.next({ result: state })
  }

  function toRequest(subscription: ClientSubscription): SubscriptionRequest {
    return {
      id: subscription.id,
      path: subscription.op.path,
      input: transformer.input.serialize(
        inputWithLastEventId(subscription.op.input, subscription.lastEventId),
      ),
    }
  }

  async function post(request: MultiplexRequest, signal?: AbortSignal) {
    return await fetch(opts.url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(request),
      signal,
    })
  }

  return { subscribe }
}

/** Pass the last `tracked()` event id the same way `httpSubscriptionLink` does. */
function inputWithLastEventId(input: unknown, lastEventId: string | undefined) {
  if (!lastEventId || (input != null && typeof input !== "object")) {
    return input
  }

  return { ...(input as object | null | undefined), lastEventId }
}

function toClientError(cause: unknown): AnyClientError {
  if (cause instanceof TRPCClientError) {
    return cause
  }

  if (cause instanceof Response) {
    return new TRPCClientError(`Multiplex request failed with status ${cause.status}`)
  }

  return TRPCClientError.from(cause instanceof Error ? cause : new Error(String(cause)))
}
