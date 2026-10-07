---
"trpc-multiplex": patch
---

Fix `DataCloneError` in the shared worker on an input that cannot be cloned, such as a Vue reactive proxy.
