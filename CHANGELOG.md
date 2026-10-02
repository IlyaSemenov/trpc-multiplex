# trpc-multiplex

## 0.3.0

### Minor Changes

- 17c188c: An error thrown by `createContext` now fails the subscriptions of its request and reaches `onError` with `ctx` of `undefined`, instead of failing the request and reopening the stream of all subscriptions.

### Patch Changes

- 1f3b03c: The client no longer loads internal modules of tRPC, which can change in any tRPC release.
- 0a598d5: A tab runs its subscriptions itself after the shared worker died 3 times, instead of starting a new worker without end.

## 0.2.0

### Minor Changes

- e04f11a: Add the `requestTimeoutMs` option: the client reopens a stream that does not connect, or whose subscription change is not answered, in time.
- e04f11a: Add the `worker` option of `multiplexLink`, `startMultiplexWorker` from `trpc-multiplex/worker`, and `restartSubscriptions()` to run the subscriptions of all tabs over one stream held by a shared worker.

### Patch Changes

- 5b46109: The server no longer starts the subscriptions of a client that disconnected while its context was being created.
- e04f11a: Subscriptions report a connection error when the endpoint responds with something other than a multiplex stream, e.g. an SPA fallback page.

## 0.1.0

### Minor Changes

- 2e2d565: Initial release.
