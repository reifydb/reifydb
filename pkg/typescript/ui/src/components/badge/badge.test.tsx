// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { Badge } from "./badge.js";

describe("Badge", () => {
  it("renders children", () => {
    render(<Badge>Status</Badge>);
    expect(screen.getByText("Status")).toBeInTheDocument();
  });

  it("applies variant styling", () => {
    render(<Badge variant="danger">Error</Badge>);
    expect(screen.getByText("Error").className).toContain("text-status-error");
  });

  it("applies custom className", () => {
    render(<Badge className="ml-2">Test</Badge>);
    expect(screen.getByText("Test").className).toContain("ml-2");
  });

  it("uses default variant when none specified", () => {
    render(<Badge>Default</Badge>);
    expect(screen.getByText("Default").className).toContain("text-text-primary");
  });
});
