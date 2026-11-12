// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { render } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { Container } from "./container.js";

describe("Container", () => {
  it("is wide by default", () => {
    // Sections must line up with the nav on the 1280 grid unless a page asks for prose width.
    const { container } = render(<Container>body</Container>);
    expect(container.firstElementChild).toHaveClass("max-w-7xl");
    expect(container.firstElementChild).not.toHaveClass("max-w-3xl");
  });

  it("narrow uses the prose width", () => {
    const { container } = render(<Container width="narrow">body</Container>);
    expect(container.firstElementChild).toHaveClass("max-w-3xl");
    expect(container.firstElementChild).not.toHaveClass("max-w-7xl");
  });

  it("always keeps the side gutter and centres itself", () => {
    // Without the gutter, text touches the screen edge on phones.
    for (const width of ["wide", "narrow"] as const) {
      const { container } = render(<Container width={width}>body</Container>);
      expect(container.firstElementChild).toHaveClass("mx-auto", "w-full", "px-4", "sm:px-6", "lg:px-8");
    }
  });

  it("keeps its own classes when a className is added", () => {
    const { container } = render(<Container className="pt-8">body</Container>);
    expect(container.firstElementChild).toHaveClass("pt-8", "max-w-7xl", "px-4");
  });

  it("renders its children", () => {
    const { getByText } = render(<Container><span>inside</span></Container>);
    expect(getByText("inside")).toBeInTheDocument();
  });
});
