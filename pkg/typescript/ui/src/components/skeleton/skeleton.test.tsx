// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { Skeleton } from "./skeleton.js";

describe("Skeleton", () => {
  it("renders a div with pulse animation", () => {
    const { container } = render(<Skeleton />);
    expect(container.firstElementChild?.className).toContain("animate-pulse");
  });

  it("applies custom className", () => {
    const { container } = render(<Skeleton className="h-4 w-48" />);
    expect(container.firstElementChild?.className).toContain("h-4");
    expect(container.firstElementChild?.className).toContain("w-48");
  });
});
