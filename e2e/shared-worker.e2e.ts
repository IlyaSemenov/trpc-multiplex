import type { APIRequestContext, BrowserContext, Page } from "@playwright/test"
import { expect, test } from "@playwright/test"

import type { TestApp } from "./app/main"

const API = "http://localhost:4100"

declare const testApp: TestApp

interface Stats {
  /** Streams opened since the test started. */
  opened: number
  /** Streams open now. */
  active: number
  /** Procedures running on the server now. */
  subscriptions: number
}

async function stats(request: APIRequestContext): Promise<Stats> {
  return (await (await request.get(`${API}/test/stats`)).json()) as Stats
}

async function emit(request: APIRequestContext, topic: string, payload: string) {
  await request.get(`${API}/test/emit?topic=${topic}&payload=${payload}`)
}

async function openPage(context: BrowserContext) {
  const page = await context.newPage()
  await page.goto("/")
  await page.waitForFunction(() => "testApp" in globalThis)
  return page
}

async function subscribe(page: Page, key: string, path: "events" | "tracked", input: unknown) {
  await page.evaluate(([key, path, input]) => testApp.subscribe(key, path, input), [
    key,
    path,
    input,
  ] as const)
}

async function received(page: Page, key: string) {
  return await page.evaluate(key => testApp.received(key), key)
}

test.beforeEach(async ({ request }) => {
  await request.get(`${API}/test/reset`)
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(0)
})

test("runs the subscriptions of all tabs over one stream", async ({ context, request }) => {
  const pages = [await openPage(context), await openPage(context), await openPage(context)]
  for (const [index, page] of pages.entries()) {
    await subscribe(page, "events", "events", { topic: `topic-${index}` })
  }
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(3)

  await emit(request, "topic-0", "zero")
  await emit(request, "topic-2", "two")

  await expect
    .poll(() => received(pages[0]!, "events"))
    .toMatchObject({
      data: [{ user: "anonymous", payload: "zero" }],
    })
  await expect
    .poll(() => received(pages[2]!, "events"))
    .toMatchObject({
      data: [{ user: "anonymous", payload: "two" }],
    })
  expect(await received(pages[1]!, "events")).toMatchObject({ data: [] })
  expect(await stats(request)).toMatchObject({ opened: 1, active: 1 })
})

test("drops the subscriptions of a closed tab and closes the stream after the last tab", async ({
  context,
  request,
}) => {
  const closing = await openPage(context)
  const staying = await openPage(context)
  await subscribe(closing, "a", "events", { topic: "a" })
  await subscribe(closing, "b", "events", { topic: "b" })
  await subscribe(staying, "c", "events", { topic: "c" })
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(3)

  await closing.close()

  await expect.poll(async () => (await stats(request)).subscriptions).toBe(1)
  expect(await stats(request)).toMatchObject({ active: 1 })

  await staying.close()

  await expect.poll(async () => await stats(request)).toMatchObject({ subscriptions: 0, active: 0 })
})

test("reconnects all tabs after the stream drops and resumes tracked subscriptions", async ({
  context,
  request,
}) => {
  const pages = [await openPage(context), await openPage(context)]
  for (const page of pages) {
    await subscribe(page, "tracked", "tracked", {})
  }
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(2)
  await emit(request, "tracked", "event-1")
  for (const page of pages) {
    await expect.poll(async () => (await received(page, "tracked")).data).toEqual([null, "event-1"])
  }

  await request.get(`${API}/test/drop`)

  for (const page of pages) {
    await expect
      .poll(async () => (await received(page, "tracked")).data)
      .toEqual([null, "event-1", "event-1"])
  }
  expect(await stats(request)).toMatchObject({ opened: 2, active: 1 })
})

test("runs the subscriptions in each tab without SharedWorker", async ({ context, request }) => {
  await context.addInitScript(() => {
    delete (globalThis as { SharedWorker?: unknown }).SharedWorker
  })
  const pages = [await openPage(context), await openPage(context)]
  for (const page of pages) {
    await subscribe(page, "events", "events", { topic: "a" })
  }
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(2)

  await emit(request, "a", "hello")

  for (const page of pages) {
    await expect
      .poll(async () => (await received(page, "events")).data)
      .toEqual([{ user: "anonymous", payload: "hello" }])
  }
  expect(await stats(request)).toMatchObject({ opened: 2, active: 2 })
})

test("starts a new subscription with the cookies another tab has set", async ({
  context,
  request,
}) => {
  const signingIn = await openPage(context)
  const other = await openPage(context)
  await subscribe(other, "before", "events", { topic: "a" })
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(1)

  await signingIn.evaluate(() => (document.cookie = "user=bob; path=/"))
  await subscribe(other, "after", "events", { topic: "a" })
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(2)
  await emit(request, "a", "hello")

  await expect
    .poll(async () => (await received(other, "before")).data)
    .toEqual([{ user: "anonymous", payload: "hello" }])
  await expect
    .poll(async () => (await received(other, "after")).data)
    .toEqual([{ user: "bob", payload: "hello" }])
})

test("stores cookies that responses to the worker's requests set", async ({ context, request }) => {
  const page = await openPage(context)
  await subscribe(page, "events", "events", { topic: "a" })
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(1)

  await expect.poll(() => page.evaluate(() => document.cookie)).toMatch(/seen=\d+/)
})

test("restarts the subscriptions of all tabs with the current cookies", async ({
  context,
  request,
}) => {
  const signingIn = await openPage(context)
  const other = await openPage(context)
  await subscribe(other, "events", "events", { topic: "a" })
  await subscribe(other, "tracked", "tracked", {})
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(2)
  await emit(request, "tracked", "event-1")
  await expect.poll(async () => (await received(other, "tracked")).data).toEqual([null, "event-1"])

  await signingIn.evaluate(() => {
    document.cookie = "user=bob; path=/"
    testApp.restart()
  })

  await expect
    .poll(async () => (await received(other, "tracked")).data)
    .toEqual([null, "event-1", null])
  await expect.poll(async () => (await received(other, "events")).started).toBe(2)
  await emit(request, "a", "hello")
  await expect
    .poll(async () => (await received(other, "events")).data)
    .toEqual([{ user: "bob", payload: "hello" }])
  expect(await stats(request)).toMatchObject({ opened: 1, subscriptions: 2 })
})

test("leaves the worker while the page is away and resumes after coming back", async ({
  context,
  request,
}) => {
  const page = await openPage(context)
  const sibling = await openPage(context)
  await subscribe(page, "tracked", "tracked", {})
  await subscribe(sibling, "events", "events", { topic: "a" })
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(2)
  await emit(request, "tracked", "event-1")
  await expect.poll(async () => (await received(page, "tracked")).data).toEqual([null, "event-1"])

  await page.goto("/other.html")
  await expect.poll(async () => (await stats(request)).subscriptions).toBe(1)

  await page.goBack()
  await page.waitForFunction(() => "testApp" in globalThis)
  // Only a page restored from the back/forward cache still has its subscription.
  if (await page.evaluate(() => testApp.restoredFromCache)) {
    await expect
      .poll(async () => (await received(page, "tracked")).data)
      .toEqual([null, "event-1", "event-1"])
    await expect.poll(async () => (await stats(request)).subscriptions).toBe(2)
  }
  expect(await stats(request)).toMatchObject({ active: 1 })
})
