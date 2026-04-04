// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { Separator } from "./separator.js";

describe("Separator", () => {
  it("renders a div", () => {
    const { container } = render(<Separator />);
    expect(container.firstElementChild).toBeInTheDocument();
  });

  it("applies default styling", () => {
    const { container } = render(<Separator />);
    expect(container.firstElementChild?.className).toContain("bg-white/[0.12]");
  });

  it("applies custom className", () => {
    const { container } = render(<Separator className="h-8" />);
    expect(container.firstElementChild?.className).toContain("h-8");
  });
});
