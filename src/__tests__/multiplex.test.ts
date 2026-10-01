import { afterEach, expect, test } from "bun:test"
import { EventEmitter, on } from "node:events"

import { createTRPCClient } from "@trpc/client"
import { initTRPC, tracked, TRPCError } from "@trpc/server"

import { multiplexLink } from "../client"
import type { MultiplexMessage } from "../protocol"
import { decodeMessages } from "../protocol"
import { createMultiplexServer } from "../server"

interface Context {
  user: string
}

interface Harness {
  url: string
  emitter: EventEmitter
  /** Number of `open` requests, i.e. streams the client has opened. */
  opened: () => number
  /** Number of streams currently held open by the client. */
  active: () => number
  setUser: (user: string) => void
  /** End all open streams as a server restart would. */
  dropStreams: () => void
}

const cleanups: (() => void)[] = []

afterEach(() => {
  cleanups.splice(0).forEach(cleanup => cleanup())
})

function createRouter(opts: { reconnectAfterInactivityMs?: number } = {}) {
  const t = initTRPC.context<Context>().create({
    sse: { client: { reconnectAfterInactivityMs: opts.reconnectAfterInactivityMs } },
  })

  let flakyStarts = 0

  return t.router({
    /** Fails to start twice with a retryable error, then works. */
    flaky: t.procedure.subscription(async function* () {
      if (++flakyStarts <= 2) {
        throw new TRPCError({ code: "SERVICE_UNAVAILABLE" })
      }
      yield "started"
    }),

    events: t.procedure
      .input((value: unknown) => value as { topic: string; lastEventId?: string })
      .subscription(async function* ({ input, ctx, signal }) {
        for await (const [payload] of on(emitter(ctx), input.topic, { signal })) {
          yield { user: ctx.user, payload: payload as unknown }
        }
      }),

    tracked: t.procedure
      .input((value: unknown) => value as { lastEventId?: string })
      .subscription(async function* ({ input, ctx, signal }) {
        yield tracked("resumed", input.lastEventId ?? null)
        for await (const [id] of on(emitter(ctx), "tracked", { signal })) {
          yield tracked(id as string, id as string)
        }
      }),

    finite: t.procedure.subscription(async function* () {
      yield 1
      yield 2
    }),

    unserializable: t.procedure.subscription(async function* ({ ctx }) {
      try {
        yield 1n
      } finally {
        emitter(ctx).emit("cleaned-up")
      }
    }),

    hanging: t.procedure.subscription(() => new Promise<never>(() => {})),

    forbidden: t.procedure.subscription(() => {
      throw new TRPCError({ code: "UNAUTHORIZED" })
    }),

    cleanup: t.procedure.subscription(async function* ({ ctx, signal }) {
      try {
        yield "ready"
        await new Promise(resolve => signal?.addEventListener("abort", resolve))
      } finally {
        emitter(ctx).emit("cleaned-up")
      }
    }),
  })
}

type Router = ReturnType<typeof createRouter>

// The emitter is passed through the context so that every test has its own event source.
const emitters = new WeakMap<Context, EventEmitter>()
function emitter(ctx: Context) {
  return emitters.get(ctx)!
}

function serve(
  router: Router,
  opts: {
    routeUpdatesTo?: ReturnType<typeof createMultiplexServer<Router>>
    startTimeoutMs?: number
    /** Number of first `open` requests to reject as an unavailable server would. */
    rejectedOpens?: number
  } = {},
): Harness {
  const events = new EventEmitter()
  const multiplex = createMultiplexServer({ router, startTimeoutMs: opts.startTimeoutMs })
  const streams = new Set<() => void>()
  let user = "alice"
  let opened = 0
  let rejectedOpens = opts.rejectedOpens ?? 0

  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = (await req
        .clone()
        .json()
        .catch(() => ({}))) as { type?: string }
      if (body.type === "open" && rejectedOpens > 0) {
        rejectedOpens--
        return new Response(null, { status: 503 })
      }

      const target = body.type === "update" && opts.routeUpdatesTo ? opts.routeUpdatesTo : multiplex

      const response = await target.handle({
        req,
        createContext: async () => {
          const ctx = { user }
          emitters.set(ctx, events)
          return ctx
        },
      })

      if (body.type !== "open") {
        return response
      }

      opened++
      return new Response(breakable(response.body!, streams), response)
    },
  })

  cleanups.push(() => server.stop(true))

  return {
    url: server.url.href,
    emitter: events,
    opened: () => opened,
    active: () => streams.size,
    setUser: value => {
      user = value
    },
    dropStreams: () => {
      streams.forEach(drop => drop())
    },
  }
}

/** Proxy the stream so that a test can end it and observe when the client stops reading it. */
function breakable(source: ReadableStream<Uint8Array>, streams: Set<() => void>) {
  const reader = source.getReader()
  let drop: () => void

  return new ReadableStream<Uint8Array>({
    start(controller) {
      drop = () => {
        streams.delete(drop)
        controller.close()
        void reader.cancel()
      }
      streams.add(drop)
    },
    async pull(controller) {
      const { done, value } = await reader.read()
      if (done) {
        streams.delete(drop)
        controller.close()
      } else {
        controller.enqueue(value)
      }
    },
    async cancel(reason) {
      streams.delete(drop)
      await reader.cancel(reason)
    },
  })
}

/** Send a raw protocol request, as a page outside of `multiplexLink` would. */
async function post(url: string, body: unknown, contentType = "application/json") {
  return await fetch(url, {
    method: "POST",
    headers: { "content-type": contentType },
    body: JSON.stringify(body),
  })
}

/** Open a stream with one subscription and wait until it is connected. */
async function openRaw(harness: Harness) {
  const response = await post(harness.url, {
    type: "open",
    subscriptions: [{ id: "own", path: "events", input: { topic: "a" } }],
  })
  const messages = decodeMessages(response.body!)
  const { value: connected } = (await messages.next()) as {
    value: Extract<MultiplexMessage, { type: "connected" }>
  }
  cleanups.push(() => void messages.return(undefined))
  return { connectionId: connected.connectionId, messages }
}

function connect(harness: Harness, retryDelayMs: (attemptIndex: number) => number = () => 0) {
  return createTRPCClient<Router>({
    links: [multiplexLink({ url: harness.url, retryDelayMs })],
  })
}

async function until(condition: () => boolean, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error("Condition was not met in time")
    }
    await Bun.sleep(5)
  }
}

test("runs all subscriptions of a client over one stream", async () => {
  const harness = serve(createRouter())
  const client = connect(harness)
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

  harness.emitter.emit("a", 1)
  harness.emitter.emit("c", 3)
  await until(() => received.length === 2)

  expect(received).toEqual([
    { user: "alice", payload: 1 },
    { user: "alice", payload: 3 },
  ])
  expect(harness.opened()).toBe(1)
})

test("adds and removes subscriptions without reopening the stream", async () => {
  const harness = serve(createRouter())
  const client = connect(harness)
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

  const cleanedUp = new Promise(resolve => harness.emitter.once("cleaned-up", resolve))
  cleanup.unsubscribe()
  await cleanedUp

  harness.emitter.emit("a", "still alive")
  await until(() => received.length === 1)

  expect(received).toEqual([{ user: "alice", payload: "still alive" }])
  expect(harness.opened()).toBe(1)
})

test("starts each added subscription with the context of its own request", async () => {
  const harness = serve(createRouter())
  const client = connect(harness)
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

  harness.setUser("bob")
  const second = client.events.subscribe(
    { topic: "b" },
    {
      onStarted: () => started++,
      onData: data => received.push(data),
    },
  )
  cleanups.push(() => second.unsubscribe())
  await until(() => started === 2)

  harness.emitter.emit("a", 1)
  harness.emitter.emit("b", 2)
  await until(() => received.length === 2)

  expect(received).toEqual([
    { user: "alice", payload: 1 },
    { user: "bob", payload: 2 },
  ])
})

test("closes the stream when the last subscription is removed", async () => {
  const harness = serve(createRouter())
  const client = connect(harness)
  let started = false

  const subscription = client.events.subscribe(
    { topic: "a" },
    { onStarted: () => (started = true) },
  )
  await until(() => started)
  expect(harness.active()).toBe(1)

  subscription.unsubscribe()

  await until(() => harness.active() === 0)
})

test("completes a subscription whose procedure finishes", async () => {
  const harness = serve(createRouter())
  const client = connect(harness)
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
  const harness = serve(createRouter())
  const client = connect(harness)
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
  harness.emitter.emit("a", 1)
  await until(() => received.length === 1)

  expect(error).toMatchObject({ data: { code: "UNAUTHORIZED" } })
  expect(received).toEqual([{ user: "alice", payload: 1 }])
})

test("reopens the stream when an update reaches a server that does not hold it", async () => {
  const router = createRouter()
  const harness = serve(router, { routeUpdatesTo: createMultiplexServer({ router }) })
  const client = connect(harness)
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

  harness.emitter.emit("a", 1)
  harness.emitter.emit("b", 2)
  await until(() => received.length === 2)

  expect(received).toEqual([
    { user: "alice", payload: 1 },
    { user: "alice", payload: 2 },
  ])
  expect(harness.opened()).toBe(2)
  expect(harness.active()).toBe(1)
})

test("reconnects after the server ends the stream and resumes tracked subscriptions", async () => {
  const harness = serve(createRouter())
  const client = connect(harness)
  const received: unknown[] = []

  const subscription = client.tracked.subscribe({}, { onData: ({ data }) => received.push(data) })
  cleanups.push(() => subscription.unsubscribe())
  await until(() => received.length === 1)

  harness.emitter.emit("tracked", "event-1")
  await until(() => received.length === 2)

  harness.dropStreams()

  await until(() => received.length === 3)
  expect(received).toEqual([null, "event-1", "event-1"])
  expect(harness.opened()).toBe(2)
})

test("reconnects when the stream stays silent longer than the inactivity timeout", async () => {
  const harness = serve(createRouter({ reconnectAfterInactivityMs: 50 }))
  const client = connect(harness)

  const subscription = client.events.subscribe({ topic: "a" }, {})
  cleanups.push(() => subscription.unsubscribe())

  await until(() => harness.opened() === 2)
})

test("rejects updates that a cross-origin page could send without a preflight", async () => {
  const harness = serve(createRouter())
  const { connectionId } = await openRaw(harness)

  const response = await post(
    harness.url,
    {
      type: "update",
      connectionId,
      add: [{ id: "foreign", path: "events", input: { topic: "a" } }],
      remove: [],
    },
    "text/plain",
  )

  expect(response.status).toBe(415)
})

test("does not expose a connection to updates sent to another host", async () => {
  const harness = serve(createRouter())
  const { connectionId } = await openRaw(harness)
  const otherHost = new URL(harness.url)
  otherHost.hostname = otherHost.hostname === "localhost" ? "127.0.0.1" : "localhost"

  const response = await post(otherHost.href, {
    type: "update",
    connectionId,
    add: [{ id: "foreign", path: "events", input: { topic: "a" } }],
    remove: [],
  })

  expect(response.status).toBe(404)
})

test("does not let a hanging subscription block the stream and further changes", async () => {
  const harness = serve(createRouter(), { startTimeoutMs: 50 })
  const client = connect(harness)
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

  harness.emitter.emit("a", 1)
  harness.emitter.emit("b", 2)
  await until(() => received.length === 2)

  expect(received).toEqual([
    { user: "alice", payload: 1 },
    { user: "alice", payload: 2 },
  ])
  expect(harness.opened()).toBe(1)
})

test("releases a subscription whose event cannot be serialized", async () => {
  const harness = serve(createRouter())
  const client = connect(harness)
  const cleanedUp = new Promise(resolve => harness.emitter.once("cleaned-up", resolve))

  const subscription = client.unserializable.subscribe(undefined, {})
  cleanups.push(() => subscription.unsubscribe())

  await cleanedUp
})

test("stops a subscription when its abort signal fires", async () => {
  const harness = serve(createRouter())
  const client = connect(harness)
  const abort = new AbortController()
  let started = false
  let completed = false

  client.cleanup.subscribe(undefined, {
    signal: abort.signal,
    onStarted: () => (started = true),
    onComplete: () => (completed = true),
  })
  await until(() => started)

  const cleanedUp = new Promise(resolve => harness.emitter.once("cleaned-up", resolve))
  abort.abort()

  expect(completed).toBe(true)
  await cleanedUp
})

test("restarts only the subscription that failed with a retryable error", async () => {
  const harness = serve(createRouter())
  const attempts: number[] = []
  const client = connect(harness, attemptIndex => {
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
  expect(harness.opened()).toBe(1)
})

test("grows the reconnect delay on consecutive failures and resets it after connecting", async () => {
  const harness = serve(createRouter(), { rejectedOpens: 2 })
  const attempts: number[] = []
  const client = connect(harness, attemptIndex => {
    attempts.push(attemptIndex)
    return 0
  })
  let started = 0

  const subscription = client.events.subscribe({ topic: "a" }, { onStarted: () => started++ })
  cleanups.push(() => subscription.unsubscribe())
  await until(() => started === 1)
  expect(attempts).toEqual([0, 1])

  harness.dropStreams()

  await until(() => started === 2)
  expect(attempts).toEqual([0, 1, 0])
})
