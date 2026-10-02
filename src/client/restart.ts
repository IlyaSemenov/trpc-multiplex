import type { RestartMessage } from "../worker-protocol"
import { RESTART_CHANNEL, RESTART_STORAGE_KEY } from "../worker-protocol"

/**
 * Start every subscription of this origin over with the current cookies, in all tabs and shared workers.
 *
 * Call it after a request that changes the session completes, e.g. login, logout, or impersonation.
 * Subscriptions start without their last `tracked()` event id, as new ones, so no cursor of the previous user is reused.
 * Subscriptions that already failed, e.g. with `UNAUTHORIZED`, are not revived.
 */
export function restartSubscriptions() {
  const restartId = crypto.randomUUID()

  // Stored before broadcasting, so a tab that misses the broadcast while suspended notices it on resume.
  try {
    localStorage.setItem(RESTART_STORAGE_KEY, restartId)
  } catch {}

  // Every transport, including the ones of this tab, listens on its own channel object.
  const channel = new BroadcastChannel(RESTART_CHANNEL)
  channel.postMessage({ type: "restart", restartId } satisfies RestartMessage)
  channel.close()
}

/** Id of the last restart of this origin, or `null` where `localStorage` is not available. */
export function readRestartId() {
  try {
    return globalThis.localStorage?.getItem(RESTART_STORAGE_KEY) ?? null
  } catch {
    return null
  }
}
