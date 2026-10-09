import { expect, test } from "@playwright/test";

test("runs the featured analytical query and renders its data", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "What is Hacker News talking about?" })).toBeVisible();
  await expect(page.locator("#query-status")).toContainText("Query completed");
  await expect(page.locator("#chart-container svg")).toBeVisible();
  await expect(page.locator("#story-count")).toHaveText("12,345");
  await expect(page.locator("#sql-editor")).toHaveValue(/postgresql/);

  await page.getByRole("button", { name: "Table" }).click();
  await expect(page.getByRole("columnheader", { name: "month" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "2026-09" })).toBeVisible();
});

test("clears stale results when a query fails", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("#chart-container svg")).toBeVisible();

  await page.locator("#sql-editor").fill("SELECT 'FORCE_TIMEOUT'");
  await page.getByRole("button", { name: /Run query/ }).click();

  await expect(page.locator("#query-status")).toContainText("query timed out");
  await expect(page.locator("#chart-container svg")).toHaveCount(0);
  await expect(page.locator("#row-count")).toHaveText("—");
});

test("selects and runs a retained front-page snapshot", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator("#snapshot-count")).toHaveText("2");

  await page.getByRole("button", { name: /Rewind the front page/ }).click();
  await expect(page.locator("#snapshot-control")).toBeVisible();
  await expect(page.locator("#sql-editor")).toHaveValue(/AT \(TIMESTAMP/);
  await expect(page.locator("#query-status")).toContainText("Query completed");

  await page.getByRole("button", { name: "Table" }).click();
  await expect(page.getByRole("cell", { name: "An earlier front page story" })).toBeVisible();
});
