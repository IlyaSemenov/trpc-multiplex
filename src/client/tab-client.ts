import type { Operation, OperationResultEnvelope } from "@trpc/client"
import { TRPCClientError } from "@trpc/client"
import type { TRPCConnectionState } from "@trpc/client/unstable-internals"
import type { TRPCCombinedDataTransformer, TRPCDataTransformer } from "@trpc/server"
import type { Observer } from "@trpc/server/observable"
import type { TRPCErrorShape } from "@trpc/server/rpc"
import type { AnyClientTypes } from "@trpc/server/unstable-core-do-not-import"

import type { TabMessage, WorkerMessage } from "../worker-protocol"
import {
  isRestartMessage,
  RESTART_CHANNEL,
  WORKER_NAME,
  WORKER_PROTOCOL_VERSION,
} from "../worker-protocol"

import type { MultiplexLinkOptions } from "./multiplex-link"
import type { SerializedError, SubscriptionEvent, TabSubscriptionRequest } from "./multiplexer"
import { createMultiplexer } from "./multiplexer"
import { readRestartId } from "./restart"

export type TabClientOptions = MultiplexLinkOptions<AnyClientTypes>

/** How many times a tab starts a new worker after the previous one died before it runs its subscriptions itself. */
const MAX_WORKER_RESTARTS = 3

type AnyClientError = TRPCClientError<any>
type ConnectionState = TRPCConnectionState<AnyClientError>
type SubscriptionObserver = Observer<
  OperationResultEnvelope<unknown, AnyClientError>,
  AnyClientError
>

/** Where the tab's subscriptions run: a multiplexer in this tab or the shared worker. */
interface Transport {
  subscribe: (id: string, request: TabSubscriptionRequest, restartId: string | null) => void
  unsubscribe: (id: string) => void
}

interface Entry {
  readonly id: string
  readonly observer: SubscriptionObserver
  readonly request: TabSubscriptionRequest
  /** Last `tracked()` event id, kept in the tab so another transport can resume the subscription. */
  lastEventId?: string
  state: ConnectionState
  dispose: () => void
}

/**
 * Subscriptions of one tRPC client in a tab.
 *
 * The tab keeps every subscription with its last event id, so it can replay them into a new transport:
 * after a suspended page resumes, after the worker dies, or when falling back to running in the tab.
 */
export function createTabClient(opts: TabClientOptions) {
  const transformer = combineTransformer(opts.transformer)
  const entries = new Map<string, Entry>()
  let lastId = 0
  /** Restart the entries' last event ids belong to. */
  let restartId = readRestartId()
  let transport: Transport | undefined
  let mode: "idle" | "worker" | "local" = "idle"
  let workerFailed = false
  let workerRestarts = 0
  let suspended = false
  let closeSession: (() => void) | undefined
  let stopLifecycle: (() => void) | undefined
  let stopTimer: ReturnType<typeof setTimeout> | undefined

  function subscribe(op: Operation, observer: SubscriptionObserver) {
    start()

    const entry: Entry = {
      id: String(++lastId),
      observer,
      request: { path: op.path, input: transformer.input.serialize(op.input) },
      state: { type: "state", state: "connecting", error: null },
      dispose: () => {
        op.signal?.removeEventListener("abort", onAbort)
        if (entries.delete(entry.id)) {
          transport?.unsubscribe(entry.id)
          scheduleStop()
        }
      },
    }

    function onAbort() {
      complete(entry)
    }

    entries.set(entry.id, entry)
    observer.next({ result: entry.state })
    transport?.subscribe(entry.id, entry.request, restartId)

    if (op.signal?.aborted) {
      complete(entry)
    } else {
      op.signal?.addEventListener("abort", onAbort, { once: true })
    }

    return entry.dispose
  }

  function start() {
    if (mode !== "idle") {
      return
    }

    // Web Locks are the only way the worker learns that a tab is gone, so the worker needs them too.
    if (opts.worker && !workerFailed && typeof navigator !== "undefined" && navigator.locks) {
      mode = "worker"
      stopLifecycle = listenToLifecycle()
      connectWorker()
    } else {
      useLocal()
    }
  }

  /** Release the transport once no subscription is left, but keep it over an unmount and mount within one task. */
  function scheduleStop() {
    if (entries.size) {
      return
    }

    clearTimeout(stopTimer)
    stopTimer = setTimeout(() => {
      if (!entries.size) {
        detach()
        stopLifecycle?.()
        stopLifecycle = undefined
        suspended = false
        mode = "idle"
      }
    })
  }

  function attach(next: Transport) {
    // A restart broadcast while this tab was suspended or switching transports invalidates its cursors.
    const storedRestartId = readRestartId()
    if (storedRestartId !== restartId) {
      restartId = storedRestartId
      entries.forEach(entry => (entry.lastEventId = undefined))
    }

    transport = next
    entries.forEach(entry =>
      next.subscribe(entry.id, { ...entry.request, lastEventId: entry.lastEventId }, restartId),
    )
  }

  function detach() {
    closeSession?.()
    closeSession = undefined
    transport = undefined
  }

  function useLocal() {
    detach()
    stopLifecycle?.()
    stopLifecycle = undefined
    // The client does not try a failed worker again.
    workerFailed ||= mode === "worker"
    mode = "local"

    const multiplexer = createMultiplexer({
      url: opts.url,
      retryDelayMs: opts.retryDelayMs,
      requestTimeoutMs: opts.requestTimeoutMs,
      restartId: readRestartId(),
    })
    const unsubscribes = new Map<string, () => void>()
    let channel: BroadcastChannel | undefined

    // Only in a browser tab: on a server an open channel would keep the process alive.
    if (typeof document !== "undefined" && typeof BroadcastChannel !== "undefined") {
      channel = new BroadcastChannel(RESTART_CHANNEL)
      channel.onmessage = ({ data }) => {
        if (isRestartMessage(data)) {
          multiplexer.restart(data.restartId)
        }
      }
    }

    closeSession = () => channel?.close()

    attach({
      subscribe: (id, request, requestRestartId) =>
        unsubscribes.set(
          id,
          multiplexer.subscribe(request, event => deliver(id, event), requestRestartId),
        ),
      unsubscribe: id => {
        unsubscribes.get(id)?.()
        unsubscribes.delete(id)
      },
    })
  }

  /** Open a session with the shared worker; any failure before or during it switches to running in the tab. */
  function connectWorker() {
    let worker: SharedWorker
    try {
      worker = opts.worker!(WORKER_NAME)
    } catch {
      useLocal()
      return
    }

    const { port } = worker
    const lock = `trpc-multiplex:tab:${crypto.randomUUID()}`
    const watch = new AbortController()
    let releaseLock: (() => void) | undefined
    let ready = false
    let closed = false

    const session: Transport = {
      subscribe: (id, request, requestRestartId) =>
        post({ type: "subscribe", id, ...request, restartId: requestRestartId }),
      unsubscribe: id => post({ type: "unsubscribe", id }),
    }

    function post(message: TabMessage) {
      port.postMessage(message)
    }

    function close() {
      if (closed) {
        return
      }

      closed = true
      clearTimeout(timer)
      watch.abort()
      worker.removeEventListener("error", fail)
      port.onmessage = null
      // The explicit goodbye lets a live worker drop the subscriptions without waiting for the lock.
      post({ type: "bye" })
      port.close()
      releaseLock?.()
    }

    function fail() {
      if (!closed) {
        useLocal()
      }
    }

    port.onmessage = ({ data }: MessageEvent<WorkerMessage>) => {
      switch (data.type) {
        case "ready": {
          if (ready) {
            return
          }

          ready = true
          clearTimeout(timer)

          // Granted only once the worker is gone: start over with a new one.
          navigator.locks
            .request(data.lock, { signal: watch.signal }, () => {
              if (closed) {
                return
              }

              // A worker the browser keeps killing, e.g. for memory, would otherwise be started again in a loop.
              if (++workerRestarts > MAX_WORKER_RESTARTS) {
                useLocal()
              } else {
                detach()
                connectWorker()
              }
            })
            .catch(() => {})

          // A worker started after a restart broadcast has missed it.
          const storedRestartId = readRestartId()
          if (storedRestartId !== null && storedRestartId !== data.restartId) {
            post({ type: "restart", restartId: storedRestartId })
          }

          attach(session)
          break
        }

        case "reject": {
          fail()
          break
        }

        case "event": {
          if (ready) {
            deliver(data.id, data.event)
          }
          break
        }
      }
    }

    worker.addEventListener("error", fail)
    closeSession = close
    const timer = setTimeout(fail, opts.workerTimeoutMs ?? 10_000)

    // The worker watches this lock to notice the tab is gone, so it is taken before the tab introduces itself.
    void navigator.locks.request(
      lock,
      () =>
        new Promise<void>(resolve => {
          releaseLock = resolve
          if (closed) {
            resolve()
            return
          }

          post({
            type: "hello",
            version: WORKER_PROTOCOL_VERSION,
            url: new URL(opts.url, location.href).href,
            lock,
            restartId: readRestartId(),
          })
        }),
    )
  }

  /**
   * Leave the worker while the page is hidden in the back/forward cache or frozen.
   *
   * Such a page keeps its Web Lock, so the worker would keep its subscriptions running for nobody.
   */
  function listenToLifecycle() {
    if (typeof document === "undefined") {
      return
    }

    const page = document

    function suspend() {
      if (!suspended) {
        suspended = true
        detach()
      }
    }

    function resume() {
      if (suspended) {
        suspended = false
        connectWorker()
      }
    }

    function onPageShow(event: PageTransitionEvent) {
      if (event.persisted) {
        resume()
      }
    }

    addEventListener("pagehide", suspend)
    addEventListener("pageshow", onPageShow)
    page.addEventListener("freeze", suspend)
    page.addEventListener("resume", resume)

    return () => {
      removeEventListener("pagehide", suspend)
      removeEventListener("pageshow", onPageShow)
      page.removeEventListener("freeze", suspend)
      page.removeEventListener("resume", resume)
    }
  }

  function deliver(id: string, event: SubscriptionEvent) {
    const entry = entries.get(id)
    if (!entry) {
      return
    }

    switch (event.type) {
      case "restarted": {
        // Each subscription gets its own marker, so the first one drops the cursors of all of them:
        // the tab may switch transports before the rest arrive.
        if (event.restartId !== restartId) {
          restartId = event.restartId
          entries.forEach(other => (other.lastEventId = undefined))
        }
        entry.lastEventId = undefined
        break
      }

      case "state": {
        setState(
          entry,
          event.state === "connecting"
            ? {
                type: "state",
                state: "connecting",
                error: event.error && toClientError(event.error),
              }
            : { type: "state", state: "pending", error: null },
        )
        break
      }

      case "started": {
        entry.observer.next({ result: { type: "started" } })
        break
      }

      case "data": {
        const data = transformer.output.deserialize(event.data)
        if (event.eventId === undefined) {
          entry.observer.next({ result: { data } })
        } else {
          entry.lastEventId = event.eventId
          entry.observer.next({
            result: { id: event.eventId, data: { id: event.eventId, data } },
          })
        }
        break
      }

      case "error": {
        entry.dispose()
        entry.observer.error(toClientError(event.error))
        break
      }

      case "stopped": {
        complete(entry)
        break
      }
    }
  }

  /** Finish a subscription the way `httpSubscriptionLink` does when its stream ends. */
  function complete(entry: Entry) {
    entry.dispose()
    entry.observer.next({ result: { type: "stopped" } })
    setState(entry, { type: "state", state: "idle", error: null })
    entry.observer.complete()
  }

  function setState(entry: Entry, state: ConnectionState) {
    if (state.state === entry.state.state && !state.error && !entry.state.error) {
      return
    }

    entry.state = state
    entry.observer.next({ result: state })
  }

  function toClientError(error: SerializedError): AnyClientError {
    if (error.type === "shape") {
      const shape = transformer.output.deserialize(error.shape) as TRPCErrorShape
      return TRPCClientError.from({ error: shape })
    }

    return TRPCClientError.from(
      error.cause instanceof Error ? error.cause : new Error(error.message),
    )
  }

  return { subscribe }
}

/** Resolve the `transformer` option the way the links of tRPC do. */
function combineTransformer(option: TabClientOptions["transformer"]): TRPCCombinedDataTransformer {
  // The other member of the option type is a compile-time error for a router without a transformer.
  const transformer = option as TRPCDataTransformer | TRPCCombinedDataTransformer | undefined
  if (!transformer) {
    const identity: TRPCDataTransformer = { serialize: data => data, deserialize: data => data }
    return { input: identity, output: identity }
  }

  return "input" in transformer ? transformer : { input: transformer, output: transformer }
}
