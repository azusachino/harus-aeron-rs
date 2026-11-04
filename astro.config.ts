import { unified } from "@astrojs/markdown-remark";
import { defineConfig } from "astro/config";
import { fileURLToPath } from "node:url";
import { resolveDocLinks } from "./scripts/astro/markdown/resolve-doc-links";

const docsDirectory = fileURLToPath(new URL("./docs", import.meta.url));

export default defineConfig({
  srcDir: "./scripts/astro",
  site: "https://harus-aeron-rs.local",
  trailingSlash: "never",
  redirects: {
    "/tutorial/": "/tutorial/README",
  },
  markdown: {
    processor: unified({
      remarkPlugins: [() => resolveDocLinks(docsDirectory)],
    }),
    shikiConfig: { theme: "github-light" },
  },
});
