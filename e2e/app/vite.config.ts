import { resolve } from "node:path"

import { defineConfig } from "vite"

const dist = resolve(import.meta.dirname, "../../dist")

export default defineConfig({
  root: import.meta.dirname,
  resolve: {
    // The app uses the built package, as it would from node_modules.
    alias: [{ find: /^trpc-multiplex\/(client|worker)$/, replacement: `${dist}/$1.mjs` }],
  },
  server: {
    proxy: {
      "/api": "http://localhost:4100",
    },
  },
  build: {
    rollupOptions: {
      input: [
        resolve(import.meta.dirname, "index.html"),
        resolve(import.meta.dirname, "other.html"),
      ],
    },
  },
})
