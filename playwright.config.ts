import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  workers: 1,
  reporter: "list",
  use: {
    baseURL: "http://127.0.0.1:4331",
    browserName: "chromium",
    screenshot: "only-on-failure",
  },
  webServer: {
    command: "mise exec -- bun run astro:preview --host 127.0.0.1 --port 4331",
    url: "http://127.0.0.1:4331",
    reuseExistingServer: false,
  },
});
