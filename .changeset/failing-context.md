---
"trpc-multiplex": minor
---

An error thrown by `createContext` now fails the subscriptions of its request and reaches `onError` with `ctx` of `undefined`, instead of failing the request and reopening the stream of all subscriptions.
