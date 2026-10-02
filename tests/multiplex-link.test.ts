import { afterEach, describe, expect, test } from "bun:test"

import { TRPCError } from "@trpc/server"
import { createMultiplexServer } from "trpc-multiplex/server"

import { cleanups, connect, createRouter, startServer, until } from "./harness"

afterEach(() => {
  cleanups.splice(0).forEach(cleanup => cleanup())
})

test("runs all subscriptions of a client over one stream", async () => {
  const server = startServer(createRouter())
  const client = connect(server)
  const received: unknown[] = []
  let started = 0

  for (const topic of ["a", "b", "c"]) {
    const subscription = client.events.subscribe(
      { topic },
      {
        onStarted: () => started++,
        onData: data => received.push(data),
      },
    )
    cleanups.push(() => subscription.unsubscribe())
  }
  await until(() => started === 3)

  server.emitter.emit("a", 1)
  server.emitter.emit("c", 3)
  await until(() => received.length === 2)

  expect(received).toEqual([
    { user: "alice", payload: 1 },
    { user: "alice", payload: 3 },
  ])
  expect(server.openedStreams()).toBe(1)
})

test("adds and removes subscriptions without reopening the stream", async () => {
  const server = startServer(createRouter())
  const client = connect(server)
  const received: unknown[] = []
  let started = 0

  const first = client.events.subscribe(
    { topic: "a" },
    {
      onStarted: () => started++,
      onData: data => received.push(data),
    },
  )
  cleanups.push(() => first.unsubscribe())
  await until(() => started === 1)

  const cleanup = client.cleanup.subscribe(undefined, { onStarted: () => started++ })
  await until(() => started === 2)

  const cleanedUp = new Promise(resolve => server.emitter.once("cleaned-up", resolve))
  cleanup.unsubscribe()
  await cleanedUp

  server.emitter.emit("a", "still alive")
  await until(() => received.length === 1)

  expect(received).toEqual([{ user: "alice", payload: "still alive" }])
  expect(server.openedStreams()).toBe(1)
})

test("starts each added subscription with the context of its own request", async () => {
  const server = startServer(createRouter())
  const client = connect(server)
  const received: unknown[] = []
  let started = 0

  const first = client.events.subscribe(
    { topic: "a" },
    {
      onStarted: () => started++,
      onData: data => received.push(data),
    },
  )
  cleanups.push(() => first.unsubscribe())
  await until(() => started === 1)

  server.setUser("bob")
  const second = client.events.subscribe(
    { topic: "b" },
    {
      onStarted: () => started++,
      onData: data => received.push(data),
    },
  )
  cleanups.push(() => second.unsubscribe())
  await until(() => started === 2)

  server.emitter.emit("a", 1)
  server.emitter.emit("b", 2)
  await until(() => received.length === 2)

  expect(received).toEqual([
    { user: "alice", payload: 1 },
    { user: "bob", payload: 2 },
  ])
})

test("closes the stream when the last subscription is removed", async () => {
  const server = startServer(createRouter())
  const client = connect(server)
  let started = false

  const subscription = client.events.subscribe(
    { topic: "a" },
    { onStarted: () => (started = true) },
  )
  await until(() => started)
  expect(server.activeStreams()).toBe(1)

  subscription.unsubscribe()

  await until(() => server.activeStreams() === 0)
})

test("closes the stream when the last subscription completes or fails", async () => {
  const server = startServer(createRouter())
  const client = connect(server)
  let completed = false
  let failed = false

  client.finite.subscribe(undefined, { onComplete: () => (completed = true) })
  await until(() => completed)
  await until(() => server.activeStreams() === 0)

  client.forbidden.subscribe(undefined, { onError: () => (failed = true) })
  await until(() => failed)
  await until(() => server.activeStreams() === 0)
  expect(server.openedStreams()).toBe(2)
})

test("completes a subscription whose procedure finishes", async () => {
  const server = startServer(createRouter())
  const client = connect(server)
  const received: unknown[] = []
  let completed = false

  client.finite.subscribe(undefined, {
    onData: data => received.push(data),
    onComplete: () => (completed = true),
  })

  await until(() => completed)
  expect(received).toEqual([1, 2])
})

test("fails only the subscription whose procedure throws a non-retryable error", async () => {
  const server = startServer(createRouter())
  const client = connect(server)
  const received: unknown[] = []
  let error: unknown
  let started = false

  const healthy = client.events.subscribe(
    { topic: "a" },
    {
      onStarted: () => (started = true),
      onData: data => received.push(data),
    },
  )
  cleanups.push(() => healthy.unsubscribe())
  client.forbidden.subscribe(undefined, {
    onError: cause => {
      error = cause
    },
  })

  await until(() => started && error !== undefined)
  server.emitter.emit("a", 1)
  await until(() => received.length === 1)

  expect(error).toMatchObject({ data: { code: "UNAUTHORIZED" } })
  expect(received).toEqual([{ user: "alice", payload: 1 }])
})

describe("failing createContext", () => {
  test("restarts the added subscription without reopening the stream", async () => {
    const errors: unknown[] = []
    const server = startServer(createRouter(), { onError: opts => errors.push(opts) })
    const client = connect(server)
    const received: unknown[] = []
    let healthyStarted = false
    let addedStarted = false

    const healthy = client.events.subscribe(
      { topic: "a" },
      { onStarted: () => (healthyStarted = true) },
    )
    cleanups.push(() => healthy.unsubscribe())
    await until(() => healthyStarted)

    server.failNextContext(new Error("Database is unavailable"))
    const added = client.events.subscribe(
      { topic: "b" },
      {
        onStarted: () => (addedStarted = true),
        onData: data => received.push(data),
      },
    )
    cleanups.push(() => added.unsubscribe())
    await until(() => addedStarted)

    server.emitter.emit("b", 1)
    await until(() => received.length === 1)

    expect(received).toEqual([{ user: "alice", payload: 1 }])
    expect(errors).toMatchObject([
      { error: { code: "INTERNAL_SERVER_ERROR" }, path: "events", ctx: undefined },
    ])
    expect(server.openedStreams()).toBe(1)
  })

  test("fails the subscriptions of the request with its error", async () => {
    const server = startServer(createRouter())
    const client = connect(server)
    let error: unknown

    server.failNextContext(new TRPCError({ code: "UNAUTHORIZED" }))
    client.events.subscribe({ topic: "a" }, { onError: cause => (error = cause) })

    await until(() => error !== undefined)
    expect(error).toMatchObject({ data: { code: "UNAUTHORIZED" } })
    expect(server.openedStreams()).toBe(1)
  })
})

test("reopens the stream when an update reaches a server that does not hold it", async () => {
  const router = createRouter()
  const server = startServer(router, { routeUpdatesTo: createMultiplexServer({ router }) })
  const client = connect(server)
  const received: unknown[] = []
  let started = 0

  const first = client.events.subscribe(
    { topic: "a" },
    {
      onStarted: () => started++,
      onData: data => received.push(data),
    },
  )
  cleanups.push(() => first.unsubscribe())
  await until(() => started === 1)

  const second = client.events.subscribe(
    { topic: "b" },
    {
      onStarted: () => started++,
      onData: data => received.push(data),
    },
  )
  cleanups.push(() => second.unsubscribe())
  // The reopened stream restarts the first subscription too.
  await until(() => started === 3)

  server.emitter.emit("a", 1)
  server.emitter.emit("b", 2)
  await until(() => received.length === 2)

  expect(received).toEqual([
    { user: "alice", payload: 1 },
    { user: "alice", payload: 2 },
  ])
  expect(server.openedStreams()).toBe(2)
  expect(server.activeStreams()).toBe(1)
})

test("reconnects after the server ends the stream and resumes tracked subscriptions", async () => {
  const server = startServer(createRouter())
  const client = connect(server)
  const received: unknown[] = []

  const subscription = client.tracked.subscribe({}, { onData: ({ data }) => received.push(data) })
  cleanups.push(() => subscription.unsubscribe())
  await until(() => received.length === 1)

  server.emitter.emit("tracked", "event-1")
  await until(() => received.length === 2)

  server.dropStreams()

  await until(() => received.length === 3)
  expect(received).toEqual([null, "event-1", "event-1"])
  expect(server.openedStreams()).toBe(2)
})

test("reconnects when the stream stays silent longer than the inactivity timeout", async () => {
  const server = startServer(createRouter({ reconnectAfterInactivityMs: 50 }))
  const client = connect(server)

  const subscription = client.events.subscribe({ topic: "a" }, {})
  cleanups.push(() => subscription.unsubscribe())

  await until(() => server.openedStreams() === 2)
})

test("does not let a hanging subscription block the stream and further changes", async () => {
  const server = startServer(createRouter(), { startTimeoutMs: 50 })
  const client = connect(server)
  const received: unknown[] = []
  let started = 0

  for (const subscription of [
    client.hanging.subscribe(undefined, {}),
    client.events.subscribe(
      { topic: "a" },
      {
        onStarted: () => started++,
        onData: data => received.push(data),
      },
    ),
  ]) {
    cleanups.push(() => subscription.unsubscribe())
  }
  await until(() => started === 1)

  const hanging = client.hanging.subscribe(undefined, {})
  cleanups.push(() => hanging.unsubscribe())
  await Bun.sleep(10)
  const added = client.events.subscribe(
    { topic: "b" },
    {
      onStarted: () => started++,
      onData: data => received.push(data),
    },
  )
  cleanups.push(() => added.unsubscribe())
  await until(() => started === 2)

  server.emitter.emit("a", 1)
  server.emitter.emit("b", 2)
  await until(() => received.length === 2)

  expect(received).toEqual([
    { user: "alice", payload: 1 },
    { user: "alice", payload: 2 },
  ])
  expect(server.openedStreams()).toBe(1)
})

test("releases a subscription whose event cannot be serialized", async () => {
  const server = startServer(createRouter())
  const client = connect(server)
  const cleanedUp = new Promise(resolve => server.emitter.once("cleaned-up", resolve))

  const subscription = client.unserializable.subscribe(undefined, {})
  cleanups.push(() => subscription.unsubscribe())

  await cleanedUp
})

test("stops a subscription when its abort signal fires", async () => {
  const server = startServer(createRouter())
  const client = connect(server)
  const abort = new AbortController()
  let started = false
  let completed = false

  client.cleanup.subscribe(undefined, {
    signal: abort.signal,
    onStarted: () => (started = true),
    onComplete: () => (completed = true),
  })
  await until(() => started)

  const cleanedUp = new Promise(resolve => server.emitter.once("cleaned-up", resolve))
  abort.abort()

  expect(completed).toBe(true)
  await cleanedUp
})

test("restarts only the subscription that failed with a retryable error", async () => {
  const server = startServer(createRouter())
  const attempts: number[] = []
  const client = connect(server, attemptIndex => {
    attempts.push(attemptIndex)
    return 0
  })
  const received: unknown[] = []
  let healthyStarted = 0

  for (const subscription of [
    client.events.subscribe({ topic: "a" }, { onStarted: () => healthyStarted++ }),
    client.flaky.subscribe(undefined, { onData: data => received.push(data) }),
  ]) {
    cleanups.push(() => subscription.unsubscribe())
  }

  await until(() => received.length === 1)
  expect(received).toEqual(["started"])
  expect(attempts).toEqual([0, 1])
  expect(healthyStarted).toBe(1)
  expect(server.openedStreams()).toBe(1)
})

test("grows the reconnect delay on consecutive failures and resets it after connecting", async () => {
  const server = startServer(createRouter(), { rejectedOpens: 2 })
  const attempts: number[] = []
  const client = connect(server, attemptIndex => {
    attempts.push(attemptIndex)
    return 0
  })
  let started = 0

  const subscription = client.events.subscribe({ topic: "a" }, { onStarted: () => started++ })
  cleanups.push(() => subscription.unsubscribe())
  await until(() => started === 1)
  expect(attempts).toEqual([0, 1])

  server.dropStreams()

  await until(() => started === 2)
  expect(attempts).toEqual([0, 1, 0])
})

test("drops a stream that does not connect in time and reconnects", async () => {
  const server = startServer(createRouter(), { contextDelayMs: 100 })
  const attempts: number[] = []
  const client = connect(
    server,
    attemptIndex => {
      attempts.push(attemptIndex)
      return 0
    },
    50,
  )
  const errors: unknown[] = []
  let started = false

  const subscription = client.events.subscribe(
    { topic: "a" },
    {
      onStarted: () => (started = true),
      onConnectionStateChange: state => {
        if (state.error) {
          errors.push(state.error.message)
        }
      },
    },
  )
  cleanups.push(() => subscription.unsubscribe())

  await until(() => attempts.length === 2)
  expect(errors.slice(0, 2)).toEqual([
    "Timeout of 50ms reached while waiting for a response",
    "Timeout of 50ms reached while waiting for a response",
  ])
  expect(attempts).toEqual([0, 1])
  expect(started).toBe(false)
})

test("reopens the stream with the current set when an update is not answered in time", async () => {
  const server = startServer(createRouter(), { firstUpdateDelayMs: 200 })
  const client = connect(server, () => 0, 50)
  const received: unknown[] = []
  let started = 0

  const first = client.events.subscribe({ topic: "a" }, { onStarted: () => started++ })
  cleanups.push(() => first.unsubscribe())
  await until(() => started === 1)

  const second = client.events.subscribe(
    { topic: "b" },
    {
      onStarted: () => started++,
      onData: data => received.push(data),
    },
  )
  cleanups.push(() => second.unsubscribe())
  // The update timed out after the server had started the subscription, so the reopened stream is the only one left.
  await until(() => server.openedStreams() === 2 && server.activeStreams() === 1)
  await until(() => started >= 3)

  server.emitter.emit("b", 1)
  await until(() => received.length === 1)
  await Bun.sleep(20)
  expect(received).toEqual([{ user: "alice", payload: 1 }])
})

test.each([
  {
    response: "a 404 page",
    fetch: () => new Response("Not Found", { status: 404 }),
    error: "Multiplex request failed with status 404",
  },
  {
    response: "an SPA fallback page",
    fetch: () =>
      new Response("<!doctype html><title>App</title>", {
        headers: { "content-type": "text/html" },
      }),
    error: "The multiplex endpoint responded without opening a stream",
  },
])(
  "keeps reconnecting with an error when the endpoint serves $response",
  async ({ fetch, error }) => {
    const misconfigured = Bun.serve({ port: 0, fetch })
    cleanups.push(() => misconfigured.stop(true))
    const attempts: number[] = []
    const client = connect({ url: misconfigured.url.href }, attemptIndex => {
      attempts.push(attemptIndex)
      return 0
    })
    const errors: unknown[] = []
    let failed = false

    const subscription = client.events.subscribe(
      { topic: "a" },
      {
        onConnectionStateChange: state => state.error && errors.push(state.error.message),
        onError: () => (failed = true),
      },
    )
    cleanups.push(() => subscription.unsubscribe())

    await until(() => attempts.length >= 3)
    expect(attempts.slice(0, 3)).toEqual([0, 1, 2])
    expect(errors.slice(0, 3)).toEqual([error, error, error])
    expect(failed).toBe(false)
  },
)
