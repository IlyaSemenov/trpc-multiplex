---
"trpc-multiplex": patch
---

A tab runs its subscriptions itself after the shared worker died 3 times, instead of starting a new worker without end.
