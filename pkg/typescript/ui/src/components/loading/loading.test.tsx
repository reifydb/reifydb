// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { Loading } from "./loading.js";

describe("Loading", () => {
  it("renders default text", () => {
    render(<Loading />);
    expect(screen.getByText("Loading")).toBeInTheDocument();
  });

  it("renders custom text", () => {
    render(<Loading text="Fetching" />);
    expect(screen.getByText("Fetching")).toBeInTheDocument();
  });

  it("renders animated dots", () => {
    const { container } = render(<Loading />);
    const dots = container.querySelectorAll(".animate-pulse");
    expect(dots).toHaveLength(3);
  });

  it("applies custom className", () => {
    const { container } = render(<Loading className="text-lg" />);
    expect(container.firstElementChild?.className).toContain("text-lg");
  });
});
