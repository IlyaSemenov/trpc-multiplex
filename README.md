# trpc-multiplex

Multiplex multiple tRPC subscriptions over a single HTTP stream.

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

`req.signal` must abort when the client disconnects, otherwise subscriptions of a closed stream keep running.

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

By default each tab opens its own stream, so enough open tabs exhaust the connection limit again.
With the `worker` option, `multiplexLink` runs the subscriptions of all tabs of the origin in a `SharedWorker` that holds a single stream.

The app creates the worker itself, so the bundler sees the worker script as an app module.
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
import { multiplexLink, restartSubscriptions } from "trpc-multiplex/client"

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
- Without `worker` each tab runs its subscriptions over its own stream, e.g. in development, where a shared worker would outlive hot reloads and keep running stale code until every tab is closed.
- In Nuxt, use `!import.meta.dev` instead of `import.meta.env.PROD`; the link touches browser APIs only on its first subscription, so it is safe to create during server-side rendering.
- The transformer stays in the tab: the worker forwards inputs, data, and errors serialized.
- Use an absolute path for `url`: the worker resolves a relative one against its own script URL, and a tab whose URL differs from the worker's runs its subscriptions itself.

### Fallback

The link silently runs the subscriptions in the tab when the browser has no `SharedWorker` (e.g. Chrome on Android) or no Web Locks, when creating the worker throws, when the worker script fails to load, or when the worker does not accept the tab within `workerTimeoutMs`.
Subscriptions move to the tab with their last `tracked()` event ids, so they resume.
The client keeps running its subscriptions in the tab and does not try the worker again.

### Tab lifecycle

- A closed or crashed tab releases a Web Lock that the worker waits for, and the worker stops the tab's subscriptions.
- A page that enters the back/forward cache or is frozen leaves the worker and stops its subscriptions; when the page is shown again, it rejoins and resumes `tracked()` subscriptions.
- A client with no subscriptions left leaves the worker; its next subscription joins it again.
- When the worker dies, its tabs start a new one and move their subscriptions there.
- A tab whose JavaScript hangs is not detected: its subscriptions keep running until the tab is closed.
- The stream is shared by the tabs that share a worker: tabs of different releases with incompatible worker protocols, tabs in different storage partitions, and tabs that fell back each use their own stream.

### Session changes

Every subscription starts with the cookies of the request that added it, so new subscriptions see the current session in every tab.
Running subscriptions keep the context they started with.
After a request that changes the session completes, e.g. login, logout, or impersonation, call `restartSubscriptions()`:

```ts
await logout()
restartSubscriptions()
```

It restarts the subscriptions of all tabs of the origin, in every worker and in every tab that runs its subscriptions itself, with the current cookies.
Restarted subscriptions start without their last `tracked()` event id, as new ones, so no cursor of the previous user is reused.
Subscriptions the server then rejects, e.g. with `UNAUTHORIZED`, fail with that error.
Only the server can guarantee that a session ends: `restartSubscriptions()` does not reach a tab that is closed or hung, or a client of another browser.

## Behavior

- The client opens the stream with its current subscriptions and keeps it while at least one subscription is active.
- Adding and removing subscriptions are separate short POST requests; the stream is not reopened.
- Each subscription starts with the context of the request that added it, so it sees the current session.
- Subscription events, `tracked()` ids, errors, and completion follow the semantics of `httpSubscriptionLink`.
- Retryable errors restart only the failed subscription.
- An error thrown by `createContext` fails the subscriptions of that request, as an error of their procedures would, and the stream stays open.
- Keepalive pings and the client inactivity timeout come from the router `sse` config (`sse.ping`, `sse.client.reconnectAfterInactivityMs`).
- After a lost stream, the client reconnects with exponential backoff (`retryDelayMs`) and resumes `tracked()` subscriptions from their last event id.
- Requests wait for their subscriptions to start, so cookies set by middleware reach the response, but no longer than `startTimeoutMs` (5 seconds by default).
- The client drops a stream that does not connect, or a subscription change that is not answered, within `requestTimeoutMs` and reopens the stream with its current subscriptions.
  Keep it above the server `startTimeoutMs` plus the time to create the context.

## Security

The connection id lets its holder attach subscriptions to the stream, and every attached subscription runs with the cookies of the request that added it.
The server therefore accepts only `application/json` requests, which a cross-origin page cannot send without a CORS preflight; do not enable CORS for the multiplex endpoint.
A connection is visible only to update requests sent to the same host as the request that opened it.

## Multiple server instances

Open streams live in the memory of the server instance that accepted them.
When an update request reaches an instance that does not hold the stream, the server answers `404`, and the client reopens the stream with its whole subscription set.
With sticky sessions every update reaches the right instance; without them the client still works but reopens the stream more often.

## Roadmap

- Fallback for buffering proxies: when the first byte of the stream does not arrive in time, switch to long polling.
- Load balancer routing by connection: the client generates the connection id and sends it in a header with every request, so a header-hash policy (Caddy `lb_policy header`, nginx `hash`, HAProxy `balance hdr()`) keeps a connection on one instance without sticky sessions.
