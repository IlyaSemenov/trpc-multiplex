# trpc-multiplex

Run many tRPC subscriptions over one HTTP stream per tab, or per browser through a shared worker.

tRPC runs subscriptions over HTTP with its built-in `httpSubscriptionLink`, which opens a separate `EventSource` per subscription.
Behind an HTTP/1.1 proxy, a browser keeps at most 6 connections per domain across all tabs, so a few tabs with a few subscriptions each exhaust the limit and the site stops loading.

Instead of a connection per subscription, `trpc-multiplex` runs many subscriptions over one server-sent event stream:

- By default, one stream per tab for all subscriptions of a tRPC client.
- With a shared worker, one stream per browser for all subscriptions of all tabs of the site.

## Install

```sh
npm install trpc-multiplex @trpc/client @trpc/server
```

The package requires tRPC 11.

## Usage

Server, with any framework that speaks the Fetch API:

```ts
import { createMultiplexServer } from "trpc-multiplex/server"

const multiplex = createMultiplexServer({ router })

export function handler(req: Request) {
  return multiplex.handle({ req, createContext: () => createContext(req) })
}
```

Make sure your server adapter aborts `req.signal` when the client disconnects; otherwise subscriptions of a closed stream keep running.

Client:

```ts
import { createTRPCClient, httpBatchLink, splitLink } from "@trpc/client"
import { multiplexLink } from "trpc-multiplex/client"

const client = createTRPCClient<AppRouter>({
  links: [
    splitLink({
      condition: op => op.type === "subscription",
      true: multiplexLink({ url: "/api/trpc-multiplex" }),
      false: httpBatchLink({ url: "/api/trpc" }),
    }),
  ],
})
```

Pass the same `transformer` to `multiplexLink` as to the other links.

## Shared worker

To run the subscriptions of all tabs of the origin over a single stream, create a `SharedWorker` and pass its factory to the `worker` option of `multiplexLink`.
Without it, each tab opens its own stream, so enough open tabs exhaust the connection limit again.

Put the transport options into a module that both the tab and the worker import:

```ts
// trpc-multiplex.config.ts
import type { MultiplexTransportOptions } from "trpc-multiplex/client"

export const transport: MultiplexTransportOptions = { url: "/api/trpc-multiplex" }
```

```ts
// trpc-worker.ts
import { startMultiplexWorker } from "trpc-multiplex/worker"

import { transport } from "./trpc-multiplex.config"

startMultiplexWorker(transport)
```

```ts
import { multiplexLink } from "trpc-multiplex/client"

import { transport } from "./trpc-multiplex.config"

const subscriptionLink = multiplexLink({
  ...transport,
  transformer: superjson,
  worker: import.meta.env.PROD
    ? name =>
        new SharedWorker(new URL("./trpc-worker.ts", import.meta.url), { type: "module", name })
    : undefined,
})
```

- Keep `new SharedWorker(new URL("…", import.meta.url), { type: "module", name })` in this exact form: Vite and other bundlers recognize it and emit the worker script as a separate hashed chunk.
- Pass the `name` the link provides: it contains the version of the protocol between tabs and the worker, so tabs of incompatible releases never share a worker.
- Leave `worker` unset in development: a shared worker would outlive hot reloads and keep running stale code until every tab is closed.
- In Nuxt, use `!import.meta.dev` instead of `import.meta.env.PROD`; the link touches browser APIs only on its first subscription, so it is safe to create during server-side rendering.
- Use an absolute path for `url`: the worker resolves a relative one against its own script URL, and a tab whose URL differs from the worker's runs its subscriptions itself.
- When the browser has no shared worker or the worker keeps failing, the link runs the subscriptions in the tab over a stream of its own.
- Tabs share a stream only when they share a worker: tabs of releases with incompatible worker protocols and tabs in different storage partitions each use their own stream.

## Session changes

After a request that changes the session completes, e.g. login, logout, or impersonation, call `restartSubscriptions()`:

```ts
import { restartSubscriptions } from "trpc-multiplex/client"

await logout()
restartSubscriptions()
```

New subscriptions start with the current cookies, but running ones keep the context they started with.
`restartSubscriptions()` restarts the subscriptions of all tabs of the origin, in every worker and in every tab that runs its subscriptions itself, with the current cookies.
Restarted subscriptions start without their last `tracked()` event id, as new ones, so no cursor of the previous user is reused.
Only the server can guarantee that a session ends: `restartSubscriptions()` does not reach a tab that is closed or hung, or a client of another browser.

## Outside the browser

To run clients outside the browser, e.g. many virtual users with their own cookies in one load test process, pass each client its own `fetch` and an absolute `url`:

```ts
import makeFetchCookie from "fetch-cookie"

const fetchWithCookies = makeFetchCookie(fetch)

const client = createTRPCClient<AppRouter>({
  links: [
    splitLink({
      condition: op => op.type === "subscription",
      true: multiplexLink({
        url: "https://example.com/api/trpc-multiplex",
        fetch: fetchWithCookies,
      }),
      false: httpBatchLink({ url: "https://example.com/api/trpc", fetch: fetchWithCookies }),
    }),
  ],
})
```

- Use an absolute `url`: outside the browser there is no page to resolve a relative one against.
- The link sends every request through `fetch`: the one that opens the stream and the ones that add and remove subscriptions.
- Each client runs its subscriptions over a stream of its own and closes it once its last subscription ends.

## Behavior

- Subscription events, `tracked()` ids, errors, and completion follow the semantics of `httpSubscriptionLink`.
- Configure keepalive pings and the client inactivity timeout in the router `sse` config (`sse.ping`, `sse.client.reconnectAfterInactivityMs`).
- Requests wait for their subscriptions to start, so cookies set by middleware reach the response, but no longer than `startTimeoutMs` (5 seconds by default).
  Keep the client `requestTimeoutMs` above it plus the time to create the context.

## Security

Do not enable CORS for the multiplex endpoint: the connection id lets its holder attach subscriptions to the stream, and every attached subscription runs with the cookies of the request that added it.
The server accepts only `application/json` requests, which a cross-origin page cannot send without a CORS preflight.

## Multiple server instances

With several server instances, use sticky sessions so that subscription changes reach the instance that holds the stream.
Open streams live in the memory of the instance that accepted them; without sticky sessions the client still works but reopens the stream more often.

## Roadmap

- Fallback for buffering proxies: when the first byte of the stream does not arrive in time, switch to long polling.
- Load balancer routing by connection: the client generates the connection id and sends it in a header with every request, so a header-hash policy (Caddy `lb_policy header`, nginx `hash`, HAProxy `balance hdr()`) keeps a connection on one instance without sticky sessions.
