import { mkdir } from "node:fs/promises";
import { expect, test } from "@playwright/test";

const screenshots = "../../.tmp/harus-aeron-rs-astro";

test("documentation links, search, diagrams, and responsive layout work", async ({
  page,
}) => {
  const pageErrors: string[] = [];
  const failedResponses: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("response", (response) => {
    if (response.status() >= 400)
      failedResponses.push(`${response.status()} ${response.url()}`);
  });

  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Aeron in Rust" }),
  ).toBeVisible();
  await mkdir(screenshots, { recursive: true });
  await page.screenshot({ path: `${screenshots}/home.png`, fullPage: true });

  await page.getByRole("link", { name: "Tutorial overview" }).click();
  await expect(page).toHaveURL(/\/tutorial\/README$/);
  await expect(
    page.getByRole("heading", { name: /Aeron Rust Course.*Tutorial Index/ }),
  ).toBeVisible();

  await page.getByRole("link", { name: "Agrona Core" }).click();
  await expect(page).toHaveURL(/\/tutorial\/01-agrona-core$/);
  await expect(
    page.getByRole("heading", { name: /Agrona Concurrent Primitives/ }),
  ).toBeVisible();

  await page.goto("/tutorial/03-client-pub-sub");
  await page.getByRole("link", { name: "Chapter 4", exact: true }).click();
  await expect(page).toHaveURL(/\/tutorial\/04-client-conductor$/);
  await expect(
    page.getByRole("heading", { name: /ClientConductor and Driver Events/ }),
  ).toBeVisible();

  await page
    .getByRole("link", { name: "Aeron 1.54.0 Baseline Assessment" })
    .click();
  await expect(page).toHaveURL(/\/aeron-1\.54\.0-baseline-assessment$/);
  await expect(
    page.getByRole("heading", { name: /Aeron 1.54.0 baseline assessment/i }),
  ).toBeVisible();
  await page.screenshot({
    path: `${screenshots}/assessment.png`,
    fullPage: true,
  });

  await page.getByRole("link", { name: "Catch up to Aeron 1.54.0" }).click();
  await expect(page).toHaveURL(/\/plans\/2026-11-04-aeron-1\.54-catch-up$/);
  await expect(
    page.getByRole("heading", { name: "Catch up to Aeron 1.54.0" }),
  ).toBeVisible();

  await page.goto("/");
  await page
    .getByRole("searchbox", { name: "Search documents" })
    .fill("1.54.0 baseline");
  await expect(
    page.getByRole("option", { name: /Aeron 1.54.0 baseline assessment/i }),
  ).toBeVisible();
  await page.locator("#filter").clear();
  await expect(page.locator("svg").first()).toBeVisible();

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.locator("body")).toHaveJSProperty("scrollWidth", 390);
  await expect(page.locator("img")).toHaveCount(0);
  expect(pageErrors).toEqual([]);
  expect(failedResponses).toEqual([]);
});

test("theme label, saved modes, cycling, and live system preference stay in sync", async ({
  page,
}) => {
  await page.emulateMedia({ colorScheme: "light" });
  await page.goto("/");

  const themeButton = page.getByRole("button", {
    name: "Theme: System (Light)",
  });
  await expect(themeButton).toBeVisible();
  await themeButton.click();
  await expect(page.getByRole("button", { name: "Theme: Dark" })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");

  await page.getByRole("button", { name: "Theme: Dark" }).click();
  await expect(
    page.getByRole("button", { name: "Theme: Light" }),
  ).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");

  await page.getByRole("button", { name: "Theme: Light" }).click();
  await expect(
    page.getByRole("button", { name: "Theme: System (Light)" }),
  ).toBeVisible();
  await page.emulateMedia({ colorScheme: "dark" });
  await expect(
    page.getByRole("button", { name: "Theme: System (Dark)" }),
  ).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");

  await page.reload();
  await expect(
    page.getByRole("button", { name: "Theme: System (Dark)" }),
  ).toBeVisible();
  await page.evaluate(() => localStorage.setItem("aeron-theme", "dark"));
  await page.reload();
  await expect(page.getByRole("button", { name: "Theme: Dark" })).toBeVisible();
});

test("code copy uses the code block text only", async ({ page, context }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.goto("/tutorial/01-agrona-core");

  const code = page.locator("pre code").first();
  const expected = await code.textContent();
  await page.getByRole("button", { name: "Copy code" }).first().click();
  await expect
    .poll(() => page.evaluate(() => navigator.clipboard.readText()))
    .toBe(expected);
});
