import type { TRPCLink } from "@trpc/client"
import type { TransformerOptions } from "@trpc/client/unstable-internals"
import { observable } from "@trpc/server/observable"
import type {
  AnyClientTypes,
  inferClientTypes,
  InferrableClientTypes,
} from "@trpc/server/unstable-core-do-not-import"

import type { MultiplexTransportOptions } from "./multiplexer"
import { createTabClient } from "./tab-client"

export type { MultiplexTransportOptions } from "./multiplexer"

export type MultiplexLinkOptions<TRoot extends AnyClientTypes> = MultiplexTransportOptions &
  TransformerOptions<TRoot> & {
    /**
     * Create the shared worker that runs the subscriptions of all tabs of the origin over a single stream,
     * passing `name` to the `SharedWorker` constructor.
     *
     * Without it, or where the browser cannot run the worker, each tab runs its subscriptions over its own stream.
     */
    worker?: (name: string) => SharedWorker
    /**
     * How long to wait for the worker to accept the tab before running the subscriptions in the tab.
     * @default 10000
     */
    workerTimeoutMs?: number
  }

/**
 * Terminating link that runs all subscriptions of a tRPC client over a single HTTP stream,
 * or with `worker`, the subscriptions of all tabs of the origin over a single stream held by a shared worker.
 *
 * Use it for subscriptions only, e.g. as the `true` branch of `splitLink`.
 * Each change of the subscription set is a short request with the current cookies,
 * so the stream itself never needs to be reopened while it is alive.
 */
export function multiplexLink<TInferrable extends InferrableClientTypes>(
  opts: MultiplexLinkOptions<inferClientTypes<TInferrable>>,
): TRPCLink<TInferrable> {
  return () => {
    // The client touches browser APIs only on its first subscription, so the link is safe during server-side rendering.
    const client = createTabClient(opts)

    return ({ op }) =>
      observable(observer => {
        if (op.type !== "subscription") {
          throw new Error("multiplexLink only supports subscriptions")
        }

        return client.subscribe(op, observer)
      })
  }
}
