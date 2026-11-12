// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, it, expect, vi } from "vitest";
import { FaqItem } from "./faq-item.js";

const ANSWER = "Yes, every view updates in the same transaction.";

function typed(container: HTMLElement): string {
  const visible = container.querySelector(".faq-item__answer [aria-hidden='true']");
  const all = visible?.textContent ?? "";
  const rest = visible?.querySelector(".invisible")?.textContent ?? "";
  const next = visible?.querySelector(".faq-item__next")?.textContent ?? "";
  return all.slice(0, all.length - rest.length - next.length);
}

function tick(times: number) {
  for (let i = 0; i < times; i++) {
    act(() => {
      vi.advanceTimersByTime(25);
    });
  }
}

describe("FaqItem", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("starts closed", () => {
    render(<FaqItem question="Is it transactional?" answer={ANSWER} />);
    expect(screen.getByRole("button", { name: "Is it transactional?" })).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByText(ANSWER)).toBeNull();
  });

  it("opens on click and closes on a second click", () => {
    render(<FaqItem question="Is it transactional?" answer={ANSWER} />);
    const button = screen.getByRole("button");
    fireEvent.click(button);
    expect(button).toHaveAttribute("aria-expanded", "true");
    fireEvent.click(button);
    expect(button).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByText(ANSWER)).toBeNull();
  });

  it("gives screen readers the full answer at once", () => {
    // The typing effect is visual only; a screen reader must never read a half answer.
    const { container } = render(<FaqItem question="Q" answer={ANSWER} />);
    fireEvent.click(screen.getByRole("button"));
    expect(container.querySelector(".sr-only")).toHaveTextContent(ANSWER, { normalizeWhitespace: false });
    expect(typed(container)).toBe("");
  });

  it("types the answer two characters every 25ms", () => {
    const { container } = render(<FaqItem question="Q" answer={ANSWER} />);
    fireEvent.click(screen.getByRole("button"));
    expect(typed(container)).toBe("");
    tick(1);
    expect(typed(container)).toBe(ANSWER.slice(0, 2));
    tick(4);
    expect(typed(container)).toBe(ANSWER.slice(0, 10));
  });

  it("shows the next marker only when typing is done", () => {
    const { container } = render(<FaqItem question="Q" answer={ANSWER} />);
    fireEvent.click(screen.getByRole("button"));
    expect(container.querySelector(".faq-item__next")).toBeNull();
    tick(Math.ceil(ANSWER.length / 2) - 1);
    expect(container.querySelector(".faq-item__next")).toBeNull();
    tick(1);
    expect(typed(container)).toBe(ANSWER);
    expect(container.querySelector(".faq-item__next")).not.toBeNull();
  });

  it("a click on the answer skips to the end", () => {
    const { container } = render(<FaqItem question="Q" answer={ANSWER} />);
    fireEvent.click(screen.getByRole("button"));
    fireEvent.click(container.querySelector(".faq-item__answer")!);
    expect(typed(container)).toBe(ANSWER);
    expect(container.querySelector(".faq-item__next")).not.toBeNull();
  });

  it("starts typing from the beginning when reopened", () => {
    // Reopening must not show a stale finished answer.
    const { container } = render(<FaqItem question="Q" answer={ANSWER} />);
    const button = screen.getByRole("button");
    fireEvent.click(button);
    fireEvent.click(container.querySelector(".faq-item__answer")!);
    fireEvent.click(button);
    fireEvent.click(button);
    expect(typed(container)).toBe("");
    expect(container.querySelector(".faq-item__next")).toBeNull();
  });

  it("renders the portrait only when given", () => {
    // Projects without a mascot must not get an empty portrait box.
    const { container, rerender } = render(<FaqItem question="Q" answer={ANSWER} />);
    fireEvent.click(screen.getByRole("button"));
    expect(container.querySelector(".faq-item__portrait")).toBeNull();
    rerender(<FaqItem question="Q" answer={ANSWER} portrait={<svg data-testid="robot" />} />);
    expect(container.querySelector(".faq-item__portrait")).toContainElement(screen.getByTestId("robot"));
  });

  it("rotates the arrow only while open", () => {
    const { container } = render(<FaqItem question="Q" answer={ANSWER} />);
    const arrow = container.querySelector("svg")!;
    expect(arrow).not.toHaveClass("rotate-90");
    fireEvent.click(screen.getByRole("button"));
    expect(arrow).toHaveClass("rotate-90", "text-accent-ink");
  });
});
