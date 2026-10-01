/** A subscription the client asks the server to start on a multiplexed connection. */
export interface SubscriptionRequest {
  /** Client-assigned id, unique within the connection. */
  id: string
  path: string
  /** Input serialized by the client transformer. */
  input: unknown
}

/**
 * Body of a POST request to the multiplex endpoint.
 *
 * `open` starts a new connection whose response is the event stream.
 * `update` changes the subscription set of an open connection; it runs with the context of its own request,
 * so a subscription always sees the current session, not the one the stream was opened with.
 */
export type MultiplexRequest =
  | { type: "open"; subscriptions: SubscriptionRequest[] }
  | { type: "update"; connectionId: string; add: SubscriptionRequest[]; remove: string[] }

/** Event of the multiplexed stream, sent from the server to the client. */
export type MultiplexMessage =
  | { type: "connected"; connectionId: string; reconnectAfterInactivityMs?: number }
  | { type: "ping" }
  | { type: "started"; id: string }
  | { type: "data"; id: string; data: unknown; eventId?: string }
  | { type: "error"; id: string; error: unknown }
  | { type: "stopped"; id: string }

/** Status of an `update` request for a connection the server does not hold, e.g. one opened on another instance. */
export const UNKNOWN_CONNECTION_STATUS = 404

export function encodeMessage(message: MultiplexMessage) {
  // JSON.stringify escapes line breaks, so every message fits a single `data:` line.
  return `data: ${JSON.stringify(message)}\n\n`
}

/** Parse a server-sent event stream into multiplex messages. */
export async function* decodeMessages(
  body: ReadableStream<Uint8Array>,
): AsyncGenerator<MultiplexMessage> {
  // Read manually: Safari does not support async iteration of `ReadableStream`.
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let buffer = ""

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) {
        return
      }

      buffer += decoder.decode(value, { stream: true })
      const events = buffer.split("\n\n")
      buffer = events.pop()!

      for (const event of events) {
        const data = event
          .split("\n")
          .filter(line => line.startsWith("data:"))
          .map(line => line.slice(5).trimStart())
          .join("\n")

        if (data) {
          yield JSON.parse(data) as MultiplexMessage
        }
      }
    }
  } finally {
    reader.releaseLock()
  }
}

export function parseRequest(body: unknown): MultiplexRequest | undefined {
  if (!isRecord(body)) {
    return undefined
  }

  if (body.type === "open" && isSubscriptionList(body.subscriptions)) {
    return { type: "open", subscriptions: body.subscriptions }
  }

  if (
    body.type === "update" &&
    typeof body.connectionId === "string" &&
    isSubscriptionList(body.add) &&
    Array.isArray(body.remove) &&
    body.remove.every(id => typeof id === "string")
  ) {
    return { type: "update", connectionId: body.connectionId, add: body.add, remove: body.remove }
  }

  return undefined
}

function isSubscriptionList(value: unknown): value is SubscriptionRequest[] {
  return (
    Array.isArray(value) &&
    value.every(
      item => isRecord(item) && typeof item.id === "string" && typeof item.path === "string",
    )
  )
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}
