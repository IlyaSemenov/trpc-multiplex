import { defineConfig, devices } from "@playwright/test"

const browsers = {
  chromium: devices["Desktop Chrome"],
  firefox: devices["Desktop Firefox"],
  webkit: devices["Desktop Safari"],
}

// `build` serves the production build from the API server, `dev` runs the Vite dev server in front of it.
const servers = {
  build: "http://localhost:4100",
  dev: "http://localhost:4101",
}

export default defineConfig({
  testDir: "e2e",
  testMatch: "**/*.e2e.ts",
  // All projects share one API server and its counters.
  workers: 1,
  timeout: 15_000,
  use: { trace: "retain-on-failure" },
  projects: Object.entries(browsers).flatMap(([browser, device]) =>
    Object.entries(servers).map(([mode, baseURL]) => ({
      name: `${browser}-${mode}`,
      use: { ...device, baseURL },
    })),
  ),
  webServer: [
    {
      command: "bun e2e/server.ts",
      url: "http://localhost:4100/test/stats",
      reuseExistingServer: false,
    },
    {
      command: "vite --config e2e/app/vite.config.ts --port 4101 --strictPort",
      url: "http://localhost:4101",
      reuseExistingServer: false,
    },
  ],
})
