// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { Spinner } from "./spinner.js";

describe("Spinner", () => {
  it("renders with status role", () => {
    render(<Spinner />);
    expect(screen.getByRole("status")).toBeInTheDocument();
  });

  it("applies size class", () => {
    render(<Spinner size="lg" />);
    expect(screen.getByRole("status").classList.toString()).toContain("h-6");
  });

  it("applies custom className", () => {
    render(<Spinner className="text-primary" />);
    expect(screen.getByRole("status").classList.toString()).toContain("text-primary");
  });
});
