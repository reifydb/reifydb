// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { Section } from "./section.js";

describe("Section", () => {
  it("renders a section element", () => {
    // Pages are built from section landmarks; a div would drop them from the outline.
    const { container } = render(<Section>body</Section>);
    expect(container.firstElementChild?.tagName).toBe("SECTION");
  });

  it("plain is the default tone", () => {
    const { container } = render(<Section>body</Section>);
    expect(container.firstElementChild).toHaveClass("bg-bg-primary");
    expect(container.firstElementChild).not.toHaveClass("bg-bg-tint");
  });

  it("tint uses the tint background", () => {
    // Alternating white and tint sections must stay distinguishable.
    const { container } = render(<Section tone="tint">body</Section>);
    expect(container.firstElementChild).toHaveClass("bg-bg-tint");
    expect(container.firstElementChild).not.toHaveClass("bg-bg-primary");
  });

  it("draws the rule on top with the rule token", () => {
    for (const tone of ["plain", "tint"] as const) {
      const { container } = render(<Section tone={tone}>body</Section>);
      expect(container.firstElementChild).toHaveClass("border-t", "border-rule", "py-16", "sm:py-24");
    }
  });

  it("passes the id through", () => {
    // Anchor links like /#faq break without the id.
    const { container } = render(<Section id="faq">body</Section>);
    expect(container.firstElementChild).toHaveAttribute("id", "faq");
  });

  it("keeps its own classes when a className is added", () => {
    const { container } = render(<Section className="pb-0">body</Section>);
    expect(container.firstElementChild).toHaveClass("pb-0", "border-t", "bg-bg-primary");
  });

  it("renders its children", () => {
    render(<Section><p>content</p></Section>);
    expect(screen.getByText("content")).toBeInTheDocument();
  });
});
