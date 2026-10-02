import { afterEach, expect, test } from "bun:test"

import type { TestServer } from "../../tests/harness"
import { cleanups, createRouter, startServer } from "../../tests/harness"
import type { MultiplexMessage } from "../protocol"
import { decodeMessages } from "../protocol"

afterEach(() => {
  cleanups.splice(0).forEach(cleanup => cleanup())
})

/** Send a raw protocol request, as a page outside of `multiplexLink` would. */
async function post(url: string, body: unknown, contentType = "application/json") {
  return await fetch(url, {
    method: "POST",
    headers: { "content-type": contentType },
    body: JSON.stringify(body),
  })
}

/** Open a stream with one subscription and wait until it is connected. */
async function openRaw(server: TestServer) {
  const response = await post(server.url, {
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

test("rejects updates that a cross-origin page could send without a preflight", async () => {
  const server = startServer(createRouter())
  const { connectionId } = await openRaw(server)

  const response = await post(
    server.url,
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
  const server = startServer(createRouter())
  const { connectionId } = await openRaw(server)
  const otherHost = new URL(server.url)
  otherHost.hostname = otherHost.hostname === "localhost" ? "127.0.0.1" : "localhost"

  const response = await post(otherHost.href, {
    type: "update",
    connectionId,
    add: [{ id: "foreign", path: "events", input: { topic: "a" } }],
    remove: [],
  })

  expect(response.status).toBe(404)
})

test("does not start subscriptions of a client that gave up while the context was created", async () => {
  const server = startServer(createRouter(), { contextDelayMs: 100 })
  let announced = false
  server.emitter.once("announced", () => (announced = true))

  await fetch(server.url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      type: "open",
      subscriptions: [{ id: "gone", path: "announced", input: undefined }],
    }),
    signal: AbortSignal.timeout(20),
  }).catch(() => {})
  await Bun.sleep(150)

  expect(announced).toBe(false)
  expect(server.activeStreams()).toBe(0)
})

test.each([
  { request: "a body that is not JSON", body: "open" },
  { request: "an unknown request type", body: JSON.stringify({ type: "close" }) },
  {
    request: "a subscription without a string id",
    body: JSON.stringify({ type: "open", subscriptions: [{ id: 1, path: "announced" }] }),
  },
  {
    request: "an update with a malformed remove list",
    body: JSON.stringify({
      type: "update",
      connectionId: "connection-1",
      add: [{ id: "added", path: "announced" }],
      remove: [1],
    }),
  },
])("rejects $request without running any procedure", async ({ body }) => {
  const server = startServer(createRouter())
  let announced = false
  server.emitter.once("announced", () => (announced = true))

  const response = await fetch(server.url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  })

  expect(response.status).toBe(400)
  expect(announced).toBe(false)
})
