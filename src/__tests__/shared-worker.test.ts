import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { EventEmitter, on } from "node:events"

import { createTRPCClient } from "@trpc/client"
import { initTRPC, tracked, TRPCError } from "@trpc/server"
import superjson from "superjson"

import type { Router, TestServer } from "../../tests/harness"
import { cleanups, createRouter, startServer, until } from "../../tests/harness"
import { multiplexLink } from "../client"
import { createWorkerHost } from "../worker/host"
import type { TabMessage, WorkerMessage } from "../worker-protocol"
import { RESTART_STORAGE_KEY } from "../worker-protocol"

/**
 * Web Locks of one origin, shared by the tabs and the worker of a test.
 *
 * `kill` releases a lock the way the browser does when the holder's context is destroyed.
 */
class FakeLocks {
  private readonly held = new Map<string, () => void>()
  private readonly waiting = new Map<string, (() => void)[]>()

  request(
    name: string,
    optionsOrCallback: LockOptions | LockGrantedCallback<unknown>,
    maybeCallback?: LockGrantedCallback<unknown>,
  ) {
    const [options, callback] =
      typeof optionsOrCallback === "function"
        ? [{} as LockOptions, optionsOrCallback]
        : [optionsOrCallback, maybeCallback!]

    return new Promise((resolve, reject) => {
      const grant = () => {
        let released = false
        const release = () => {
          if (released) {
            return
          }
          released = true
          this.held.delete(name)
          this.waiting.get(name)?.shift()?.()
        }
        this.held.set(name, release)
        void Promise.resolve()
          .then(() => callback({ name, mode: "exclusive" }))
          .then(
            value => {
              release()
              resolve(value)
            },
            (error: unknown) => {
              release()
              reject(error)
            },
          )
      }

      if (!this.held.has(name)) {
        grant()
        return
      }

      const queue = this.waiting.get(name) ?? []
      this.waiting.set(name, queue)
      queue.push(grant)
      options.signal?.addEventListener("abort", () => {
        queue.splice(queue.indexOf(grant), 1)
        reject(new DOMException("Aborted", "AbortError"))
      })
    })
  }

  kill(name: string) {
    this.held.get(name)?.()
  }

  heldNames(prefix: string) {
    return [...this.held.keys()].filter(name => name.startsWith(prefix))
  }
}

let locks: FakeLocks
let storage: Map<string, string>

beforeEach(() => {
  locks = new FakeLocks()
  storage = new Map()
  Object.defineProperty(navigator, "locks", { value: locks, configurable: true })
  Object.defineProperty(globalThis, "localStorage", {
    value: {
      getItem: (key: string) => storage.get(key) ?? null,
      setItem: (key: string, value: string) => storage.set(key, value),
    },
    configurable: true,
  })
})

afterEach(() => {
  cleanups.splice(0).forEach(cleanup => cleanup())
  delete (navigator as { locks?: unknown }).locks
  delete (globalThis as { localStorage?: unknown }).localStorage
  delete (globalThis as { location?: unknown }).location
})

/** A shared worker of the test origin; `die` replaces it with a new one, as the browser does after a crash. */
function createWorker(server: Pick<TestServer, "url">, opts: { url?: string } = {}) {
  let instance = 0
  let host = start()

  function start() {
    const lock = `trpc-multiplex:worker:${++instance}`
    void locks.request(lock, () => new Promise(() => {}))
    return {
      lock,
      host: createWorkerHost({
        url: opts.url ?? server.url,
        baseUrl: server.url,
        retryDelayMs: () => 0,
        lock,
        locks: locks as unknown as LockManager,
      }),
    }
  }

  return {
    factory: () => {
      const channel = new MessageChannel()
      host.host.connect(channel.port2)
      cleanups.push(() => channel.port1.close())
      return Object.assign(new EventTarget(), { port: channel.port1 }) as unknown as SharedWorker
    },
    restart: (restartId: string) => {
      storage.set(RESTART_STORAGE_KEY, restartId)
      host.host.restart(restartId)
    },
    die: () => {
      const { lock } = host
      host = start()
      locks.kill(lock)
    },
  }
}

const TAB_LOCK = "trpc-multiplex:tab:"

/** A tab with its own tRPC client; it takes its lock on its first subscription. */
function openTab(
  server: Pick<TestServer, "url">,
  worker: (name: string) => SharedWorker,
  opts: { workerTimeoutMs?: number } = {},
) {
  Object.defineProperty(globalThis, "location", {
    value: { href: server.url },
    configurable: true,
  })
  const client = createTRPCClient<Router>({
    links: [
      multiplexLink({
        url: server.url,
        retryDelayMs: () => 0,
        worker,
        workerTimeoutMs: opts.workerTimeoutMs,
      }),
    ],
  })

  return { client }
}

describe("multiplexLink with a worker", () => {
  test("runs the subscriptions of several tabs over one stream", async () => {
    const server = startServer(createRouter())
    const worker = createWorker(server)
    const received: unknown[] = []
    let started = 0

    for (const topic of ["a", "b", "c"]) {
      const { client } = openTab(server, worker.factory)
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

  test("passes an input that cannot be cloned, such as a reactive proxy, to the worker", async () => {
    const server = startServer(createRouter())
    const worker = createWorker(server)
    const { client } = openTab(server, worker.factory)
    const received: unknown[] = []
    let started = false

    const subscription = client.events.subscribe(new Proxy({ topic: "a" }, {}), {
      onStarted: () => (started = true),
      onData: data => received.push(data),
    })
    cleanups.push(() => subscription.unsubscribe())
    await until(() => started)
    server.emitter.emit("a", 1)
    await until(() => received.length === 1)

    expect(received).toEqual([{ user: "alice", payload: 1 }])
  })

  test("drops the subscriptions of a closed tab and closes the stream after the last one", async () => {
    const server = startServer(createRouter())
    const worker = createWorker(server)
    let started = 0

    openTab(server, worker.factory).client.cleanup.subscribe(undefined, {
      onStarted: () => started++,
    })
    await until(() => started === 1)
    const closingLocks = locks.heldNames(TAB_LOCK)
    openTab(server, worker.factory).client.events.subscribe(
      { topic: "a" },
      { onStarted: () => started++ },
    )
    await until(() => started === 2)

    // Closing a page releases its locks without any message to the worker.
    const cleanedUp = new Promise(resolve => server.emitter.once("cleaned-up", resolve))
    closingLocks.forEach(name => locks.kill(name))
    await cleanedUp
    expect(server.activeStreams()).toBe(1)

    locks.heldNames(TAB_LOCK).forEach(name => locks.kill(name))
    await until(() => server.activeStreams() === 0)
  })

  test("leaves the worker after the last subscription of the tab and comes back for the next one", async () => {
    const server = startServer(createRouter())
    const worker = createWorker(server)
    const { client } = openTab(server, worker.factory)
    let started = 0

    const removed = client.events.subscribe({ topic: "a" }, { onStarted: () => started++ })
    await until(() => started === 1)
    removed.unsubscribe()
    await until(() => locks.heldNames(TAB_LOCK).length === 0 && server.activeStreams() === 0)

    const added = client.events.subscribe({ topic: "a" }, { onStarted: () => started++ })
    cleanups.push(() => added.unsubscribe())
    await until(() => started === 2)
    expect(locks.heldNames(TAB_LOCK)).toHaveLength(1)
  })

  test("moves the subscriptions to a new worker after the worker dies and resumes tracked ones", async () => {
    const server = startServer(createRouter())
    const worker = createWorker(server)
    const { client } = openTab(server, worker.factory)
    const received: unknown[] = []

    const subscription = client.tracked.subscribe({}, { onData: ({ data }) => received.push(data) })
    cleanups.push(() => subscription.unsubscribe())
    await until(() => received.length === 1)
    server.emitter.emit("tracked", "event-1")
    await until(() => received.length === 2)

    worker.die()

    await until(() => received.length === 3)
    expect(received).toEqual([null, "event-1", "event-1"])
  })

  test("runs the subscriptions in the tab after the worker died 3 times", async () => {
    const server = startServer(createRouter())
    const worker = createWorker(server)
    const { client } = openTab(server, worker.factory)
    const received: unknown[] = []

    const subscription = client.tracked.subscribe({}, { onData: ({ data }) => received.push(data) })
    cleanups.push(() => subscription.unsubscribe())
    await until(() => received.length === 1)

    for (let deaths = 1; deaths <= 3; deaths++) {
      worker.die()
      await until(() => received.length === deaths + 1)
      expect(locks.heldNames(TAB_LOCK)).toHaveLength(1)
    }

    worker.die()
    await until(() => received.length === 5)
    expect(locks.heldNames(TAB_LOCK)).toEqual([])
  })

  test.each([
    {
      lifecycle: "frozen",
      suspend: () => document.dispatchEvent(new Event("freeze")),
      resume: () => document.dispatchEvent(new Event("resume")),
    },
    {
      lifecycle: "in the back/forward cache",
      suspend: () => dispatchEvent(Object.assign(new Event("pagehide"), { persisted: true })),
      resume: () => dispatchEvent(Object.assign(new Event("pageshow"), { persisted: true })),
    },
  ])(
    "leaves the worker while the page is $lifecycle and resumes tracked subscriptions after",
    async ({ suspend, resume }) => {
      Object.defineProperty(globalThis, "document", {
        value: new EventTarget(),
        configurable: true,
      })
      cleanups.push(() => delete (globalThis as { document?: unknown }).document)
      const server = startServer(createRouter())
      const worker = createWorker(server)
      const { client } = openTab(server, worker.factory)
      const received: unknown[] = []

      const subscription = client.tracked.subscribe(
        {},
        { onData: ({ data }) => received.push(data) },
      )
      cleanups.push(() => subscription.unsubscribe())
      await until(() => received.length === 1)
      server.emitter.emit("tracked", "event-1")
      await until(() => received.length === 2)

      suspend()
      await until(() => server.activeStreams() === 0)
      expect(locks.heldNames(TAB_LOCK)).toEqual([])

      resume()
      await until(() => received.length === 3)
      expect(received).toEqual([null, "event-1", "event-1"])
      expect(server.activeStreams()).toBe(1)
    },
  )

  test("restarts the subscriptions of all tabs with the current context and without their last event id", async () => {
    const server = startServer(createRouter())
    const worker = createWorker(server)
    const tracking = openTab(server, worker.factory)
    const listening = openTab(server, worker.factory)
    const trackedData: unknown[] = []
    const events: unknown[] = []
    let started = 0

    tracking.client.tracked.subscribe({}, { onData: ({ data }) => trackedData.push(data) })
    listening.client.events.subscribe(
      { topic: "a" },
      {
        onStarted: () => started++,
        onData: data => events.push(data),
      },
    )
    await until(() => trackedData.length === 1 && started === 1)
    server.emitter.emit("tracked", "event-1")
    await until(() => trackedData.length === 2)

    server.setUser("bob")
    worker.restart("restart-1")
    await until(() => trackedData.length === 3 && started === 2)
    server.emitter.emit("a", 1)
    await until(() => events.length === 1)

    expect(trackedData).toEqual([null, "event-1", null])
    expect(events).toEqual([{ user: "bob", payload: 1 }])
    expect(server.openedStreams()).toBe(1)
  })

  test("applies a restart that the worker missed", async () => {
    const server = startServer(createRouter())
    const worker = createWorker(server)
    const tracking = openTab(server, worker.factory)
    const trackedData: unknown[] = []

    tracking.client.tracked.subscribe({}, { onData: ({ data }) => trackedData.push(data) })
    await until(() => trackedData.length === 1)
    server.emitter.emit("tracked", "event-1")
    await until(() => trackedData.length === 2)

    // Another tab restarted while the worker was not listening.
    storage.set(RESTART_STORAGE_KEY, "restart-1")
    const late = openTab(server, worker.factory)
    late.client.events.subscribe({ topic: "a" }, {})

    await until(() => trackedData.length === 3)
    expect(trackedData).toEqual([null, "event-1", null])
  })

  test("drops the last event ids of all subscriptions when the worker fails amid a restart", async () => {
    const server = startServer(createRouter())
    const lock = "trpc-multiplex:worker:scripted"
    void locks.request(lock, () => new Promise(() => {}))
    const channel = new MessageChannel()
    cleanups.push(() => channel.port1.close())
    const scripted = Object.assign(new EventTarget(), {
      port: channel.port1,
    }) as unknown as SharedWorker
    const subscribed: string[] = []
    const send = (message: WorkerMessage) => channel.port2.postMessage(message)
    channel.port2.onmessage = ({ data }: MessageEvent<TabMessage>) => {
      if (data.type === "hello") {
        send({ type: "ready", lock, restartId: null })
      } else if (data.type === "subscribe") {
        subscribed.push(data.id)
      }
    }
    const { client } = openTab(server, () => scripted)
    const received = new Map<string, unknown[]>()
    let restarted = false

    for (const id of ["1", "2"]) {
      received.set(id, [])
      const subscription = client.tracked.subscribe(
        {},
        {
          onStarted: () => (restarted = id === "1"),
          onData: ({ data }) => received.get(id)!.push(data),
        },
      )
      cleanups.push(() => subscription.unsubscribe())
    }
    await until(() => subscribed.length === 2)
    for (const id of subscribed) {
      send({ type: "event", id, event: { type: "data", data: "before", eventId: `event-${id}` } })
    }
    await until(() => [...received.values()].every(data => data.length === 1))

    // The worker fails after the restart marker of only one subscription has reached the tab.
    storage.set(RESTART_STORAGE_KEY, "restart-1")
    send({ type: "event", id: "1", event: { type: "restarted", restartId: "restart-1" } })
    send({ type: "event", id: "1", event: { type: "started" } })
    await until(() => restarted)
    scripted.dispatchEvent(new Event("error"))

    await until(() => [...received.values()].every(data => data.length === 2))
    expect(Object.fromEntries(received)).toEqual({ "1": ["before", null], "2": ["before", null] })
  })

  test("passes data, errors, and resumed inputs through the worker serialized by the tab transformer", async () => {
    const events = new EventEmitter()
    const t = initTRPC.create({ transformer: superjson })
    const router = t.router({
      dates: t.procedure
        .input((value: unknown) => value as { since: Date; lastEventId?: string })
        .subscription(async function* ({ input, signal }) {
          yield tracked("resumed", { since: input.since, lastEventId: input.lastEventId ?? null })
          for await (const [id] of on(events, "date", { signal })) {
            yield tracked(id as string, { at: new Date(0), lastEventId: null })
          }
        }),
      forbidden: t.procedure.subscription(() => {
        throw new TRPCError({ code: "FORBIDDEN", message: "No access" })
      }),
    })
    const server = startServer(router as unknown as Router)
    const worker = createWorker(server)
    Object.defineProperty(globalThis, "location", {
      value: { href: server.url },
      configurable: true,
    })
    const client = createTRPCClient<typeof router>({
      links: [
        multiplexLink({
          url: server.url,
          retryDelayMs: () => 0,
          transformer: superjson,
          worker: worker.factory,
        }),
      ],
    })
    const received: unknown[] = []
    let error: unknown

    const subscription = client.dates.subscribe(
      { since: new Date(1) },
      { onData: ({ data }) => received.push(data) },
    )
    cleanups.push(() => subscription.unsubscribe())
    client.forbidden.subscribe(undefined, { onError: cause => (error = cause) })
    await until(() => received.length === 1 && error !== undefined)
    events.emit("date", "event-1")
    await until(() => received.length === 2)
    server.dropStreams()
    await until(() => received.length === 3)

    expect(received).toEqual([
      { since: new Date(1), lastEventId: null },
      { at: new Date(0), lastEventId: null },
      { since: new Date(1), lastEventId: "event-1" },
    ])
    expect(error).toMatchObject({ message: "No access", data: { code: "FORBIDDEN" } })
  })
})

describe("multiplexLink without a usable worker", () => {
  async function expectRunsInTab(server: TestServer, tab: ReturnType<typeof openTab>) {
    const received: unknown[] = []
    let started = false
    const subscription = tab.client.events.subscribe(
      { topic: "a" },
      {
        onStarted: () => (started = true),
        onData: data => received.push(data),
      },
    )
    cleanups.push(() => subscription.unsubscribe())
    await until(() => started)

    server.emitter.emit("a", 1)
    await until(() => received.length === 1)
    expect(received).toEqual([{ user: "alice", payload: 1 }])
    expect(server.openedStreams()).toBe(1)
  }

  test("runs the subscriptions in the tab when the worker cannot be created", async () => {
    const server = startServer(createRouter())

    await expectRunsInTab(
      server,
      openTab(server, () => {
        throw new Error("SharedWorker is not supported")
      }),
    )
  })

  test("runs the subscriptions in the tab when the worker serves another endpoint", async () => {
    const server = startServer(createRouter())
    const worker = createWorker(server, { url: "/other" })

    await expectRunsInTab(server, openTab(server, worker.factory))
  })

  test("runs the subscriptions in the tab when the worker does not answer in time", async () => {
    const server = startServer(createRouter())
    const silent = () => {
      const channel = new MessageChannel()
      cleanups.push(() => channel.port1.close())
      return Object.assign(new EventTarget(), { port: channel.port1 }) as unknown as SharedWorker
    }

    await expectRunsInTab(server, openTab(server, silent, { workerTimeoutMs: 20 }))
  })

  test("runs the subscriptions in the tab without Web Locks", async () => {
    const server = startServer(createRouter())
    const worker = createWorker(server)
    delete (navigator as { locks?: unknown }).locks

    await expectRunsInTab(server, openTab(server, worker.factory))
  })
})
