import { EventEmitter, on } from "node:events"
import { join } from "node:path"

import { initTRPC, tracked } from "@trpc/server"
import { createMultiplexServer } from "trpc-multiplex/server"

interface Context {
  user: string
}

const events = new EventEmitter()
const t = initTRPC.context<Context>().create()
let subscriptions = 0

/** Count the procedure as a running server subscription until it finishes. */
async function* counted<T>(source: AsyncIterable<T>) {
  subscriptions++
  try {
    yield* source
  } finally {
    subscriptions--
  }
}

export const router = t.router({
  events: t.procedure
    .input((value: unknown) => value as { topic: string })
    .subscription(({ input, ctx, signal }) =>
      counted(
        (async function* () {
          for await (const [payload] of on(events, input.topic, { signal })) {
            yield { user: ctx.user, payload: payload as unknown }
          }
        })(),
      ),
    ),

  tracked: t.procedure
    .input((value: unknown) => value as { lastEventId?: string })
    .subscription(({ input, signal }) =>
      counted(
        (async function* () {
          yield tracked("resumed", input.lastEventId ?? null)
          for await (const [id] of on(events, "tracked", { signal })) {
            yield tracked(id as string, id as string)
          }
        })(),
      ),
    ),
})

export type AppRouter = typeof router

const multiplex = createMultiplexServer({ router })
const streams = new Set<() => void>()
let opened = 0
let seen = 0

function readCookie(req: Request, name: string) {
  return req.headers
    .get("cookie")
    ?.split(";")
    .map(part => part.trim().split("="))
    .find(([key]) => key === name)?.[1]
}

/** Track the stream so a test can count and end it. */
function track(source: ReadableStream<Uint8Array>) {
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

const dist = join(import.meta.dir, "app/dist")

// The multiplex endpoint and test controls; it also serves the built test app, so the `build` projects need no other server.
Bun.serve({
  port: 4100,
  idleTimeout: 0,
  async fetch(req) {
    const url = new URL(req.url)

    if (url.pathname === "/api/trpc-multiplex") {
      const response = await multiplex.handle({
        req,
        createContext: async () => ({ user: readCookie(req, "user") ?? "anonymous" }),
      })
      // Like a session middleware that rewrites the cookie on every response.
      const headers = new Headers(response.headers)
      headers.append("set-cookie", `seen=${++seen}; Path=/; SameSite=Lax`)
      if (!response.body || !headers.get("content-type")?.startsWith("text/event-stream")) {
        return new Response(response.body, { status: response.status, headers })
      }

      opened++
      return new Response(track(response.body), { status: response.status, headers })
    }

    if (url.pathname === "/test/stats") {
      return Response.json({ opened, active: streams.size, subscriptions })
    }

    if (url.pathname === "/test/emit") {
      events.emit(url.searchParams.get("topic")!, url.searchParams.get("payload"))
      return new Response(null, { status: 204 })
    }

    if (url.pathname === "/test/drop") {
      streams.forEach(drop => drop())
      return new Response(null, { status: 204 })
    }

    if (url.pathname === "/test/reset") {
      streams.forEach(drop => drop())
      opened = 0
      return new Response(null, { status: 204 })
    }

    const file = Bun.file(join(dist, url.pathname === "/" ? "index.html" : url.pathname))
    return (await file.exists()) ? new Response(file) : new Response(null, { status: 404 })
  },
})
