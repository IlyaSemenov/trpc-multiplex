// Compile-only checks of the options of `multiplexLink`.

import type { MultiplexTransportOptions } from "trpc-multiplex/client"

/** Signature of a fetch wrapper such as `fetch-cookie`, without the static members of the global `fetch`. */
declare const wrappedFetch: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>

export const options: MultiplexTransportOptions[] = [
  { url: "https://example.com/api/trpc-multiplex", fetch },
  { url: "https://example.com/api/trpc-multiplex", fetch: wrappedFetch },
]
