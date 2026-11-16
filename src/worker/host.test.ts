import { afterEach, expect, test } from "bun:test"

import type { TestServer } from "../../tests/harness"
import { cleanups, createRouter, startServer, until } from "../../tests/harness"
import type { SubscriptionEvent } from "../client/multiplexer"
import type { TabMessage, WorkerMessage } from "../worker-protocol"

import type { WorkerHostOptions } from "./host"
import { createWorkerHost } from "./host"

afterEach(() => {
  cleanups.splice(0).forEach(cleanup => cleanup())
})

/** Web Locks of open tabs: the host waits for their locks, which are never released. */
const locks = { request: () => new Promise(() => {}) } as unknown as LockManager

/** Talk to the host over a raw port, as a tab of any version could. */
function connectRaw(server: TestServer, opts: Pick<WorkerHostOptions, "fetch"> = {}) {
  const host = createWorkerHost({
    ...opts,
    url: server.url,
    baseUrl: server.url,
    retryDelayMs: () => 0,
    lock: "trpc-multiplex:worker:raw",
    locks,
  })
  const channel = new MessageChannel()
  host.connect(channel.port2)
  cleanups.push(() => channel.port1.close())
  const messages: WorkerMessage[] = []
  channel.port1.onmessage = ({ data }: MessageEvent<WorkerMessage>) => messages.push(data)
  return {
    messages,
    events: () => messages.flatMap(message => (message.type === "event" ? [message.event] : [])),
    post: (message: TabMessage) => channel.port1.postMessage(message),
  }
}

test("opens the stream through the fetch of its options", async () => {
  const server = startServer(createRouter())
  const urls: string[] = []
  const tab = connectRaw(server, {
    fetch: (url, init) => {
      urls.push(url)
      return fetch(url, init)
    },
  })

  tab.post({
    type: "hello",
    version: 1,
    url: server.url,
    lock: "trpc-multiplex:tab:raw",
    restartId: null,
  })
  tab.post({ type: "subscribe", id: "1", path: "events", input: { topic: "a" }, restartId: null })

  await until(() => tab.events().some(event => event.type === "started"))
  expect(urls).toEqual([server.url])
})

test("starts a subscription registered with a stale restart over, without its last event id", async () => {
  const server = startServer(createRouter())
  const tab = connectRaw(server)

  tab.post({
    type: "hello",
    version: 1,
    url: server.url,
    lock: "trpc-multiplex:tab:raw",
    restartId: "restart-2",
  })
  tab.post({
    type: "subscribe",
    id: "1",
    path: "tracked",
    input: {},
    lastEventId: "event-1",
    restartId: "restart-1",
  })

  await until(() => tab.events().some(event => event.type === "data"))
  expect(tab.events()).toEqual<SubscriptionEvent[]>([
    { type: "restarted", restartId: "restart-2" },
    { type: "started" },
    { type: "state", state: "pending", error: null },
    { type: "data", data: null, eventId: "resumed" },
  ])
})

test("rejects a tab of another protocol version", async () => {
  const server = startServer(createRouter())
  const tab = connectRaw(server)

  tab.post({
    type: "hello",
    version: 0,
    url: server.url,
    lock: "trpc-multiplex:tab:raw",
    restartId: null,
  })

  await until(() => tab.messages.length === 1)
  expect(tab.messages[0]!.type).toBe("reject")
})
