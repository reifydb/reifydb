// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
// @vitest-environment node

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, it, expect } from "vitest";

const base = readFileSync(fileURLToPath(new URL("./base.css", import.meta.url)), "utf8");
const theme = readFileSync(fileURLToPath(new URL("./theme.css", import.meta.url)), "utf8");

function rule(selector: string): string {
  const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = base.match(new RegExp(`^${escaped} \\{([^}]*)\\}`, "m"));
  expect(match, `${selector} is missing from base.css`).not.toBeNull();
  return match![1];
}

describe("editorial classes in base.css", () => {
  it("defines every shared class", () => {
    // Projects style with these names; a rename here silently unstyles every project.
    for (const selector of [
      ".sky",
      ".eyebrow",
      ".label",
      ".dialog",
      ".faq-item",
      ".faq-item__question",
      ".faq-item__answer",
      ".faq-item__portrait",
      ".faq-item__next",
      ".marker",
      ".link-accent",
      ".link-accent:hover",
      ".facts-band",
      ".facts-band__fact",
      ".facts-band__label",
      ".facts-band__value",
    ]) {
      rule(selector);
    }
  });

  it("rules use the rule token", () => {
    expect(rule(".faq-item")).toContain("border-top: 1px solid var(--color-rule)");
    expect(rule(".faq-item__portrait")).toContain("border: 1px solid var(--color-rule)");
  });

  it("ink accents use the accent ink token", () => {
    expect(rule(".eyebrow")).toContain("color: var(--color-accent-ink)");
    expect(rule(".dialog")).toContain("border: 2px solid var(--color-accent-ink)");
    expect(rule(".faq-item__next")).toContain("color: var(--color-accent-ink)");
  });

  it("loud accents keep their roles", () => {
    // Yellow marks, pink links, core indigo is the one dark band.
    expect(rule(".marker")).toContain("var(--color-accent-yellow)");
    expect(rule(".link-accent")).toContain("text-decoration-color: var(--color-accent-pink)");
    expect(rule(".link-accent:hover")).toContain("color: var(--color-accent-pink)");
    expect(rule(".facts-band")).toContain("background: var(--color-core)");
  });

  it("type roles use the font tokens", () => {
    expect(rule(".eyebrow")).toContain("font-family: var(--font-mono)");
    expect(rule(".label")).toContain("font-family: var(--font-mono)");
    expect(rule(".facts-band__label")).toContain("font-family: var(--font-mono)");
    expect(rule(".facts-band__value")).toContain("font-family: var(--font-display)");
  });

  it("the faq next marker has its blink keyframes", () => {
    // Without the keyframes the animation name points at nothing.
    expect(rule(".faq-item__next")).toContain("animation: blink");
    expect(base).toMatch(/^@keyframes blink \{/m);
  });

  it("never repeats the token colours as raw hex", () => {
    // A raw hex copy drifts the moment the token changes.
    expect(base.toLowerCase()).not.toContain("#e3e5f4");
    expect(base.toLowerCase()).not.toContain("#4b52d6");
  });

  it("theme defines the tokens with their values", () => {
    expect(theme).toContain("--color-rule: #e3e5f4;");
    expect(theme).toContain("--color-accent-ink: #4b52d6;");
  });
});
