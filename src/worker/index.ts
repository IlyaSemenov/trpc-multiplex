import type { MultiplexTransportOptions } from "../client/multiplexer"
import { isRestartMessage, RESTART_CHANNEL } from "../worker-protocol"

import { createWorkerHost } from "./host"

export type MultiplexWorkerOptions = MultiplexTransportOptions

interface SharedWorkerScope {
  location: { href: string }
  navigator: { locks?: LockManager }
  addEventListener: (type: "connect", listener: (event: MessageEvent) => void) => void
}

/**
 * Run the subscriptions of all tabs of this origin in the shared worker that calls it.
 *
 * Call it from the worker script the app passes to the `worker` option of `multiplexLink`, with the same transport options as the link.
 */
export function startMultiplexWorker(opts: MultiplexWorkerOptions) {
  const scope = globalThis as unknown as SharedWorkerScope
  const { locks } = scope.navigator
  const lock = `trpc-multiplex:worker:${crypto.randomUUID()}`

  if (!locks) {
    // Without Web Locks closed tabs cannot be noticed: refuse, and every tab runs its subscriptions itself.
    scope.addEventListener("connect", ({ ports: [port] }) => {
      port!.postMessage({ type: "reject", reason: "Web Locks are not available in the worker" })
      port!.close()
    })
    return
  }

  const host = createWorkerHost({ ...opts, baseUrl: scope.location.href, lock, locks })

  // Tabs watch this lock to notice the worker's death, so it is held before any tab is served.
  const acquired = new Promise<void>(resolve => {
    void locks.request(lock, () => {
      resolve()
      return new Promise(() => {})
    })
  })

  scope.addEventListener("connect", ({ ports: [port] }) => {
    // Messages wait in the port until the host starts listening.
    void acquired.then(() => host.connect(port!))
  })

  new BroadcastChannel(RESTART_CHANNEL).onmessage = ({ data }) => {
    if (isRestartMessage(data)) {
      host.restart(data.restartId)
    }
  }
}
