import { TRPC_ERROR_CODES_BY_KEY } from "@trpc/server/rpc"

import type { MultiplexMessage, MultiplexRequest, SubscriptionRequest } from "../protocol"
import { decodeMessages, UNKNOWN_CONNECTION_STATUS } from "../protocol"

const { BAD_GATEWAY, GATEWAY_TIMEOUT, INTERNAL_SERVER_ERROR, SERVICE_UNAVAILABLE } =
  TRPC_ERROR_CODES_BY_KEY

/** Codes of errors that restart the subscription, the same as `httpSubscriptionLink` retries. */
const RETRYABLE_CODES: readonly number[] = [
  BAD_GATEWAY,
  SERVICE_UNAVAILABLE,
  GATEWAY_TIMEOUT,
  INTERNAL_SERVER_ERROR,
]

/** Options shared by the tab and the shared worker, e.g. through a common config module. */
export interface MultiplexTransportOptions {
  /** URL of the multiplex endpoint served by `createMultiplexServer`. */
  url: string
  /**
   * Delay before a reconnect or a subscription restart, by 0-based index of the consecutive attempt.
   * @default exponential backoff from 1 to 30 seconds
   */
  retryDelayMs?: (attemptIndex: number) => number
  /**
   * How long to wait for the stream to connect and for each subscription change to be accepted.
   * On timeout the stream is dropped and reopened, as after a network error.
   * Pass `Infinity` to wait forever.
   * @default 15000
   */
  requestTimeoutMs?: number
}

/**
 * Error of a subscription in a form that survives `postMessage`.
 *
 * `shape` is the server error shape serialized by the transformer; `cause` is dropped on the way from a worker.
 */
export type SerializedError =
  | { type: "shape"; shape: unknown }
  | { type: "message"; message: string; cause?: unknown }

/** Event of a subscription; data and error shapes stay serialized, the tab deserializes them. */
export type SubscriptionEvent =
  | { type: "state"; state: "connecting" | "pending"; error: SerializedError | null }
  | { type: "started" }
  | { type: "data"; data: unknown; eventId?: string }
  /** The subscription failed for good and is removed. */
  | { type: "error"; error: SerializedError }
  /** The procedure finished and the subscription is removed. */
  | { type: "stopped" }
  /**
   * The subscription starts over without its last event id, after `restartSubscriptions()`.
   * Events received before this one belong to the previous run.
   */
  | { type: "restarted"; restartId: string | null }

/** Subscription request a tab makes; the multiplexer assigns it an id on the connection. */
export type TabSubscriptionRequest = Omit<SubscriptionRequest, "id">

export interface MultiplexerOptions extends MultiplexTransportOptions {
  /** Id of the last restart already reflected in the subscriptions' last event ids. */
  restartId: string | null
}

export interface Multiplexer {
  /**
   * Start a subscription and return the function that removes it.
   *
   * `restartId` is the restart the request's `lastEventId` belongs to; a different one means a restart was missed,
   * so the subscription starts over and emits `restarted` first.
   */
  subscribe: (
    request: TabSubscriptionRequest,
    listener: (event: SubscriptionEvent) => void,
    restartId: string | null,
  ) => () => void
  /** Start every subscription over with the context of a new request; repeated ids are ignored. */
  restart: (restartId: string | null) => void
  readonly restartId: string | null
}

interface Subscription {
  /** Id on the server; replaced on restart so events of the previous run are recognized and dropped. */
  serverId: string
  readonly path: string
  readonly input: unknown
  readonly listener: (event: SubscriptionEvent) => void
  /** Id of the last `tracked()` event, sent back on restart so the procedure can resume. */
  lastEventId?: string
  /** Pending restart after a retryable error. */
  retryTimer?: ReturnType<typeof setTimeout>
  /**
   * Consecutive retryable errors since the subscription last delivered data.
   * Not reset on `started`: a generator procedure that throws immediately fails after it has started.
   */
  failedAttempts: number
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
 * Subscription engine behind `multiplexLink` and the shared worker.
 *
 * It never deserializes data, so it needs no transformer and its events can be posted to another context.
 */
export function createMultiplexer(opts: MultiplexerOptions): Multiplexer {
  const retryDelayMs = opts.retryDelayMs ?? defaultRetryDelayMs
  const requestTimeoutMs = opts.requestTimeoutMs ?? 15_000
  const subscriptions = new Map<string, Subscription>()
  let restartId = opts.restartId
  let lastServerId = 0
  let connection: Connection | undefined
  let flushTimer: ReturnType<typeof setTimeout> | undefined
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined
  let failedAttempts = 0

  function nextServerId() {
    return String(++lastServerId)
  }

  function subscribe(
    request: TabSubscriptionRequest,
    listener: (event: SubscriptionEvent) => void,
    requestRestartId: string | null,
  ) {
    const subscription: Subscription = {
      serverId: nextServerId(),
      path: request.path,
      input: request.input,
      listener,
      lastEventId: request.lastEventId,
      failedAttempts: 0,
    }

    subscriptions.set(subscription.serverId, subscription)
    if (requestRestartId !== restartId) {
      subscription.lastEventId = undefined
      listener({ type: "restarted", restartId })
    }
    scheduleFlush()

    return () => {
      clearTimeout(subscription.retryTimer)
      if (subscriptions.get(subscription.serverId) === subscription) {
        subscriptions.delete(subscription.serverId)
        scheduleFlush()
      }
    }
  }

  function restart(nextRestartId: string | null) {
    if (nextRestartId === restartId) {
      return
    }

    restartId = nextRestartId
    const restarted = [...subscriptions.values()]
    subscriptions.clear()

    // New server ids make the next flush remove the previous runs and add new ones in a single request.
    for (const subscription of restarted) {
      clearTimeout(subscription.retryTimer)
      subscription.retryTimer = undefined
      subscription.failedAttempts = 0
      subscription.lastEventId = undefined
      subscription.serverId = nextServerId()
      subscriptions.set(subscription.serverId, subscription)
    }

    // Listeners may unsubscribe any subscription, so they run once the whole set is registered again.
    for (const subscription of restarted) {
      if (subscriptions.get(subscription.serverId) === subscription) {
        subscription.listener({ type: "restarted", restartId })
        subscription.listener({ type: "state", state: "connecting", error: null })
      }
    }

    scheduleFlush()
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
      subscription => !connection!.sent.has(subscription.serverId) && !subscription.retryTimer,
    )
    const remove = [...connection.sent].filter(id => !subscriptions.has(id))
    if (!add.length && !remove.length) {
      return
    }

    add.forEach(subscription => connection!.sent.add(subscription.serverId))
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

    const timeout = withTimeout(target.abort.signal)
    const response = await post(request, timeout.signal).catch((error: unknown) => error)
    timeout.clear()
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
      // The outcome of a timed out update is unknown, so the stream is reopened with the current set.
      disconnect(timeout.expired ? timeoutError() : toSerializedError(response))
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
      target.sent.add(subscription.serverId)
    })

    void read(target, {
      type: "open",
      subscriptions: [...subscriptions.values()].map(toRequest),
    })
  }

  async function read(target: Connection, request: MultiplexRequest) {
    let error: SerializedError | null = null
    // Covers the whole way to the `connected` message, so a proxy that buffers the stream is noticed too.
    target.watchdog = startTimer(requestTimeoutMs, () => {
      if (target === connection) {
        disconnect(timeoutError())
      }
    })

    try {
      const response = await post(request, target.abort.signal)
      if (!response.ok || !response.body) {
        throw response
      }

      for await (const message of decodeMessages(response.body)) {
        if (target !== connection) {
          return
        }

        if (target.id) {
          resetWatchdog(target)
        }
        receive(target, message)
      }
    } catch (cause) {
      error = toSerializedError(cause)
    }

    // A successful response that never connects comes from something else at the URL, e.g. an SPA fallback page.
    if (!error && !target.id) {
      error = {
        type: "message",
        message: "The multiplex endpoint responded without opening a stream",
      }
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
        subscription.listener({ type: "started" })
        subscription.listener({ type: "state", state: "pending", error: null })
        break
      }

      case "data": {
        subscription.failedAttempts = 0
        if (message.eventId !== undefined) {
          subscription.lastEventId = message.eventId
        }
        subscription.listener({ type: "data", data: message.data, eventId: message.eventId })
        break
      }

      case "error": {
        const error: SerializedError = { type: "shape", shape: message.error }
        target.sent.delete(message.id)

        if (RETRYABLE_CODES.includes(message.code)) {
          subscription.listener({ type: "state", state: "connecting", error })
          subscription.retryTimer = setTimeout(
            () => {
              subscription.retryTimer = undefined
              scheduleFlush()
            },
            retryDelayMs(subscription.failedAttempts++),
          )
        } else {
          subscriptions.delete(message.id)
          scheduleFlush()
          subscription.listener({ type: "error", error })
        }
        break
      }

      case "stopped": {
        target.sent.delete(message.id)
        subscriptions.delete(message.id)
        scheduleFlush()
        subscription.listener({ type: "stopped" })
        break
      }
    }
  }

  function resetWatchdog(target: Connection) {
    clearTimeout(target.watchdog)
    target.watchdog = startTimer(target.inactivityMs, () => {
      if (target === connection) {
        disconnect(timeoutError(target.inactivityMs))
      }
    })
  }

  /** Drop the current stream and reconnect after a growing delay. */
  function disconnect(error: SerializedError | null) {
    close()
    subscriptions.forEach(subscription =>
      subscription.listener({ type: "state", state: "connecting", error }),
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

  function toRequest(subscription: Subscription): SubscriptionRequest {
    return {
      id: subscription.serverId,
      path: subscription.path,
      input: subscription.input,
      lastEventId: subscription.lastEventId,
    }
  }

  /** Abort when `signal` aborts or the request timeout expires, without `AbortSignal.any` missing in older Safari. */
  function withTimeout(signal: AbortSignal) {
    const controller = new AbortController()
    const abort = () => controller.abort()
    signal.addEventListener("abort", abort, { once: true })
    const result = {
      signal: controller.signal,
      expired: false,
      clear: () => {
        clearTimeout(timer)
        signal.removeEventListener("abort", abort)
      },
    }
    const timer = startTimer(requestTimeoutMs, () => {
      result.expired = true
      abort()
    })
    return result
  }

  function timeoutError(ms = requestTimeoutMs): SerializedError {
    return { type: "message", message: `Timeout of ${ms}ms reached while waiting for a response` }
  }

  async function post(request: MultiplexRequest, signal?: AbortSignal) {
    return await fetch(opts.url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(request),
      signal,
    })
  }

  return {
    subscribe,
    restart,
    get restartId() {
      return restartId
    },
  }
}

/** `setTimeout` that treats a missing, zero, or infinite delay as "never" instead of "now". */
function startTimer(ms: number | undefined, callback: () => void) {
  return ms && Number.isFinite(ms) ? setTimeout(callback, ms) : undefined
}

function toSerializedError(cause: unknown): SerializedError {
  if (cause instanceof Response) {
    return { type: "message", message: `Multiplex request failed with status ${cause.status}` }
  }

  if (cause instanceof Error) {
    return { type: "message", message: cause.message, cause }
  }

  return { type: "message", message: String(cause) }
}
