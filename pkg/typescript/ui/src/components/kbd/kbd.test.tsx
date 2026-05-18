// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { Kbd } from "./kbd.js";

describe("Kbd", () => {
  it("renders children", () => {
    render(<Kbd>K</Kbd>);
    expect(screen.getByText("K")).toBeInTheDocument();
  });

  it("renders as kbd element", () => {
    render(<Kbd>Enter</Kbd>);
    expect(screen.getByText("Enter").tagName).toBe("KBD");
  });

  it("applies custom className", () => {
    render(<Kbd className="ml-2">X</Kbd>);
    expect(screen.getByText("X").className).toContain("ml-2");
  });
});
