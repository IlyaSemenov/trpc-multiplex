import { afterEach, expect, test } from "bun:test"

import { cleanups, createRouter, startServer, until } from "../../tests/harness"

import { createMultiplexer } from "./multiplexer"

afterEach(() => {
  cleanups.splice(0).forEach(cleanup => cleanup())
})

test("drops a subscription that the listener of another one removes while they restart", async () => {
  const server = startServer(createRouter())
  const multiplexer = createMultiplexer({ url: server.url, retryDelayMs: () => 0, restartId: null })
  let started = 0

  const removing = multiplexer.subscribe(
    { path: "events", input: { topic: "a" } },
    event => {
      if (event.type === "started") {
        started++
      } else if (event.type === "restarted") {
        removing()
        removed()
      }
    },
    null,
  )
  const removed = multiplexer.subscribe(
    { path: "events", input: { topic: "b" } },
    event => event.type === "started" && started++,
    null,
  )
  cleanups.push(removing, removed)
  await until(() => started === 2)

  multiplexer.restart("restart-1")

  await until(() => server.activeStreams() === 0)
  expect(server.openedStreams()).toBe(1)
})
