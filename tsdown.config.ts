import { defineConfig } from "tsdown"

export default defineConfig({
  entry: {
    client: "src/client/index.ts",
    server: "src/server/index.ts",
  },
  format: "esm",
  dts: true,
  exports: true,
  publint: true,
  attw: {
    profile: "esm-only",
  },
})
