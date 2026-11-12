// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { PageHero } from "./page-hero.js";

describe("PageHero", () => {
  it("renders the title as the only h1", () => {
    // A page must have exactly one h1, and the hero owns it.
    render(<PageHero eyebrow="Company" title="Mission" subtitle="Why we build" />);
    const headings = screen.getAllByRole("heading", { level: 1 });
    expect(headings).toHaveLength(1);
    expect(headings[0]).toHaveTextContent("Mission");
  });

  it("renders the eyebrow with the eyebrow style", () => {
    render(<PageHero eyebrow="Company" title="Mission" />);
    expect(screen.getByText("Company")).toHaveClass("eyebrow", "uppercase");
  });

  it("renders nothing extra when only a title is given", () => {
    // Empty eyebrow, subtitle or action rows would add stray gaps.
    const { container } = render(<PageHero title="Mission" />);
    const inner = container.querySelector("section > div");
    expect(inner?.children).toHaveLength(1);
    expect(container.querySelector(".eyebrow")).toBeNull();
    expect(container.querySelector("p")).toBeNull();
  });

  it("renders the subtitle", () => {
    render(<PageHero title="Mission" subtitle="Why we build" />);
    expect(screen.getByText("Why we build")).toBeInTheDocument();
  });

  it("renders actions in one row container", () => {
    render(
      <PageHero title="Mission">
        <button>Start</button>
        <button>Read docs</button>
      </PageHero>,
    );
    const start = screen.getByRole("button", { name: "Start" });
    expect(start.parentElement).toBe(screen.getByRole("button", { name: "Read docs" }).parentElement);
    expect(start.parentElement).toHaveClass("mt-9");
  });

  it("uses the sky background", () => {
    const { container } = render(<PageHero title="Mission" />);
    expect(container.firstElementChild).toHaveClass("sky");
  });

  it("keeps bottom padding without a scene", () => {
    const { container } = render(<PageHero title="Mission" />);
    expect(container.querySelector("section > div")).toHaveClass("pb-[clamp(36px,6vw,72px)]");
  });

  it("puts the scene outside the container and drops the bottom padding", () => {
    // The scene runs full width under the text; padding above it would leave a gap.
    const { container } = render(<PageHero title="Mission" scene={<canvas data-testid="scene" />} />);
    const inner = container.querySelector("section > div");
    const scene = screen.getByTestId("scene");
    expect(inner).not.toHaveClass("pb-[clamp(36px,6vw,72px)]");
    expect(inner?.contains(scene)).toBe(false);
    expect(container.firstElementChild?.contains(scene)).toBe(true);
  });
});
