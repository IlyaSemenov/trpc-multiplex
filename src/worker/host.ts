import type {
  Multiplexer,
  MultiplexTransportOptions,
  SubscriptionEvent,
} from "../client/multiplexer"
import { createMultiplexer } from "../client/multiplexer"
import type { TabMessage, WorkerMessage } from "../worker-protocol"
import { WORKER_PROTOCOL_VERSION } from "../worker-protocol"

export interface WorkerHostOptions extends MultiplexTransportOptions {
  /** Base for a relative `url`, i.e. the location of the worker script. */
  baseUrl: string
  /** Web Lock the worker holds for its whole life. */
  lock: string
  locks: LockManager
}

/** Serve subscriptions of tab sessions, each connected over its own port, from a single multiplexer. */
export function createWorkerHost(opts: WorkerHostOptions) {
  const url = new URL(opts.url, opts.baseUrl).href
  let multiplexer: Multiplexer | undefined

  function connect(port: MessagePort) {
    let session: Map<string, () => void> | undefined
    let dropped = false

    function post(message: WorkerMessage) {
      port.postMessage(message)
    }

    /** Release everything of the session; repeated calls, e.g. a goodbye and then the released lock, are no-ops. */
    function drop() {
      if (dropped) {
        return
      }

      dropped = true
      session?.forEach(unsubscribe => unsubscribe())
      session = undefined
      port.onmessage = null
      port.close()
    }

    port.onmessage = ({ data }: MessageEvent<TabMessage>) => {
      switch (data.type) {
        case "hello": {
          if (data.version !== WORKER_PROTOCOL_VERSION || data.url !== url) {
            post({
              type: "reject",
              reason: `Expected protocol ${WORKER_PROTOCOL_VERSION} and ${url}`,
            })
            drop()
            return
          }

          multiplexer ??= createMultiplexer({ ...opts, url, restartId: data.restartId })
          session = new Map()
          // Granted once the tab is closed or crashed.
          void opts.locks.request(data.lock, drop)
          post({ type: "ready", lock: opts.lock, restartId: multiplexer.restartId })
          break
        }

        case "subscribe": {
          if (!session || !multiplexer) {
            return
          }

          const { id } = data
          session.get(id)?.()
          session.set(
            id,
            multiplexer.subscribe(
              { path: data.path, input: data.input, lastEventId: data.lastEventId },
              event => post({ type: "event", id, event: toCloneable(event) }),
              data.restartId,
            ),
          )
          break
        }

        case "unsubscribe": {
          session?.get(data.id)?.()
          session?.delete(data.id)
          break
        }

        case "restart": {
          multiplexer?.restart(data.restartId)
          break
        }

        case "bye": {
          drop()
          break
        }
      }
    }
  }

  return {
    connect,
    restart: (restartId: string) => multiplexer?.restart(restartId),
  }
}

/** Drop the original error object, which does not survive `postMessage` in every browser. */
function toCloneable(event: SubscriptionEvent): SubscriptionEvent {
  if (event.type === "state" && event.error?.type === "message") {
    return { ...event, error: { type: "message", message: event.error.message } }
  }

  if (event.type === "error" && event.error.type === "message") {
    return { ...event, error: { type: "message", message: event.error.message } }
  }

  return event
}
