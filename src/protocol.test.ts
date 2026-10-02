import { expect, test } from "bun:test"

import type { MultiplexMessage } from "./protocol"
import { decodeMessages, encodeMessage } from "./protocol"

const messages: MultiplexMessage[] = [
  { type: "connected", connectionId: "connection-1" },
  // Line breaks and multi-byte characters inside the data must not split or corrupt a message.
  { type: "data", id: "1", data: { text: "Привет 👋\n\ndata: not a message" }, eventId: "event-1" },
  { type: "stopped", id: "1" },
]
const bytes = new TextEncoder().encode(messages.map(encodeMessage).join(""))

async function decode(chunks: Uint8Array[]) {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      chunks.forEach(chunk => controller.enqueue(chunk))
      controller.close()
    },
  })
  return await Array.fromAsync(decodeMessages(stream))
}

test("decodes the messages wherever the stream is split into two chunks", async () => {
  const splits = Array.from({ length: bytes.length - 1 }, (_, index) => index + 1)

  const decoded = await Promise.all(
    splits.map(async split => ({
      split,
      messages: await decode([bytes.subarray(0, split), bytes.subarray(split)]),
    })),
  )

  expect(decoded).toEqual(splits.map(split => ({ split, messages })))
})

test("decodes the messages from a stream of single bytes", async () => {
  const chunks = Array.from(bytes, byte => Uint8Array.of(byte))

  expect(await decode(chunks)).toEqual(messages)
})
