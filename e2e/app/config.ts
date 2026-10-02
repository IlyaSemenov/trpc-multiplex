import type { MultiplexTransportOptions } from "trpc-multiplex/client"

/** Transport options shared by the tab and the worker. */
export const transport: MultiplexTransportOptions = {
  url: "/api/trpc-multiplex",
  retryDelayMs: () => 0,
}
