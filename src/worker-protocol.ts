import type { SubscriptionEvent, TabSubscriptionRequest } from "./client/multiplexer"

/**
 * Version of the messages between a tab and the shared worker.
 *
 * It is part of the worker name, so tabs of different versions never share a worker.
 * Bump it on any incompatible change of the messages below.
 */
export const WORKER_PROTOCOL_VERSION = 1

export const WORKER_NAME = `trpc-multiplex-v${WORKER_PROTOCOL_VERSION}`

/**
 * Channel of `restartSubscriptions()`.
 *
 * Its name and message stay the same across versions, so a restart reaches workers and tabs of every deploy.
 */
export const RESTART_CHANNEL = "trpc-multiplex:restart"

/** `localStorage` key with the id of the last restart, for tabs that were suspended when it was broadcast. */
export const RESTART_STORAGE_KEY = "trpc-multiplex:restart"

export interface RestartMessage {
  type: "restart"
  restartId: string
}

/** Message from a tab to the worker over the port of one tab session. */
export type TabMessage =
  | {
      type: "hello"
      version: number
      /** Absolute endpoint URL, compared with the worker's own. */
      url: string
      /** Web Lock the tab holds while the session lives; the worker drops the session once it is released. */
      lock: string
      restartId: string | null
    }
  | ({ type: "subscribe"; id: string; restartId: string | null } & TabSubscriptionRequest)
  | { type: "unsubscribe"; id: string }
  /** Apply a restart the worker may have missed, e.g. because it started after the broadcast. */
  | { type: "restart"; restartId: string }
  | { type: "bye" }

/** Message from the worker to a tab. */
export type WorkerMessage =
  | {
      type: "ready"
      /** Web Lock the worker holds while it lives; tabs wait for it to notice the worker's death. */
      lock: string
      restartId: string | null
    }
  | { type: "reject"; reason: string }
  | { type: "event"; id: string; event: SubscriptionEvent }

export function isRestartMessage(value: unknown): value is RestartMessage {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as RestartMessage).type === "restart" &&
    typeof (value as RestartMessage).restartId === "string"
  )
}
