import { expect, test } from "bun:test";
import { resolveDocLinks } from "./resolve-doc-links";

test("rewrites relative Markdown links while keeping query and fragment suffixes", () => {
  const tree = {
    type: "root",
    children: [
      { type: "link", url: "04-client-conductor.md?mode=full#driver-events" },
      { type: "link", url: "https://example.com/guide.md#external" },
    ],
  };
  const transform = resolveDocLinks("/project/docs");

  transform(tree, { history: ["/project/docs/tutorial/03-client-pub-sub.md"] });

  expect(tree.children[0]?.url).toBe(
    "/tutorial/04-client-conductor?mode=full#driver-events",
  );
  expect(tree.children[1]?.url).toBe("https://example.com/guide.md#external");
});
