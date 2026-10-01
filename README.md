# trpc-multiplex

Multiplex multiple tRPC subscriptions over a single HTTP stream.

`httpSubscriptionLink` opens a separate `EventSource` per subscription.
Behind an HTTP/1.1 proxy, a browser keeps at most 6 connections per domain across all tabs, so a few tabs with a few subscriptions each exhaust the limit and the site stops loading.
`trpc-multiplex` runs all subscriptions of a tRPC client over one server-sent event stream.

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

## Behavior

- The client opens the stream with its current subscriptions and keeps it while at least one subscription is active.
- Adding and removing subscriptions are separate short POST requests; the stream is not reopened.
- Each subscription starts with the context of the request that added it, so it sees the current session.
- Subscription events, `tracked()` ids, errors, and completion follow the semantics of `httpSubscriptionLink`.
- Retryable errors restart only the failed subscription.
- Keepalive pings and the client inactivity timeout come from the router `sse` config (`sse.ping`, `sse.client.reconnectAfterInactivityMs`).
- After a lost stream, the client reconnects with exponential backoff (`retryDelayMs`) and resumes `tracked()` subscriptions from their last event id.
- Requests wait for their subscriptions to start, so cookies set by middleware reach the response, but no longer than `startTimeoutMs` (5 seconds by default).

## Security

The connection id lets its holder attach subscriptions to the stream, and every attached subscription runs with the cookies of the request that added it.
The server therefore accepts only `application/json` requests, which a cross-origin page cannot send without a CORS preflight; do not enable CORS for the multiplex endpoint.
A connection is visible only to update requests sent to the same host as the request that opened it.

## Multiple server instances

Open streams live in the memory of the server instance that accepted them.
When an update request reaches an instance that does not hold the stream, the server answers `404`, and the client reopens the stream with its whole subscription set.
With sticky sessions every update reaches the right instance; without them the client still works but reopens the stream more often.

## Roadmap

- Shared stream across tabs: a `SharedWorker` holds one stream per browser and deduplicates identical subscriptions of different tabs, so any number of tabs uses a single connection.
- Fallback for buffering proxies: when the first byte of the stream does not arrive in time, switch to long polling.
- Load balancer routing by connection: the client generates the connection id and sends it in a header with every request, so a header-hash policy (Caddy `lb_policy header`, nginx `hash`, HAProxy `balance hdr()`) keeps a connection on one instance without sticky sessions.
