import * as v from "valibot"

import type { MultiplexRequest } from "../protocol"

const SubscriptionRequestSchema = v.object({
  id: v.string(),
  path: v.string(),
  input: v.optional(v.unknown()),
  lastEventId: v.optional(v.string()),
})

const MultiplexRequestSchema = v.variant("type", [
  v.object({ type: v.literal("open"), subscriptions: v.array(SubscriptionRequestSchema) }),
  v.object({
    type: v.literal("update"),
    connectionId: v.string(),
    add: v.array(SubscriptionRequestSchema),
    remove: v.array(v.string()),
  }),
])

/** Validate the body of a request to the multiplex endpoint; `undefined` if it is not a multiplex request. */
export function parseRequest(body: unknown): MultiplexRequest | undefined {
  const result = v.safeParse(MultiplexRequestSchema, body)
  return result.success ? result.output : undefined
}
