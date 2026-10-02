import { createTRPCClient } from "@trpc/client"
import type { Unsubscribable } from "@trpc/server/observable"
import { multiplexLink, restartSubscriptions } from "trpc-multiplex/client"

import type { AppRouter } from "../server"

import { transport } from "./config"

const client = createTRPCClient<AppRouter>({
  links: [
    multiplexLink({
      ...transport,
      // The tests run the worker in the dev server too; an app would pass it in production only.
      worker: name =>
        new SharedWorker(new URL("./trpc-worker.ts", import.meta.url), { type: "module", name }),
    }),
  ],
})

interface Received {
  data: unknown[]
  started: number
  errors: string[]
  subscription: Unsubscribable
}

const subscriptions = new Map<string, Received>()

/** API the browser tests drive the page with. */
const testApp = {
  subscribe(key: string, path: "events" | "tracked", input: unknown) {
    const received: Received = { data: [], started: 0, errors: [], subscription: undefined! }
    const handlers = {
      onStarted: () => received.started++,
      onData: (data: unknown) => received.data.push(data),
      onError: (error: Error) => received.errors.push(error.message),
    }
    received.subscription =
      path === "events"
        ? client.events.subscribe(input as { topic: string }, handlers)
        : client.tracked.subscribe(input as object, {
            ...handlers,
            onData: ({ data }) => received.data.push(data),
          })
    subscriptions.set(key, received)
  },
  unsubscribe(key: string) {
    subscriptions.get(key)?.subscription.unsubscribe()
  },
  received(key: string) {
    const { data, started, errors } = subscriptions.get(key)!
    return { data, started, errors }
  },
  restart: restartSubscriptions,
  restoredFromCache: false,
}

addEventListener("pageshow", (event: PageTransitionEvent) => {
  if (event.persisted) {
    testApp.restoredFromCache = true
  }
})

Object.assign(globalThis, { testApp })
export type TestApp = typeof testApp
