// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, it, expect } from "vitest";
import { Input } from "./input.js";

describe("Input", () => {
  it("renders an input element", () => {
    render(<Input placeholder="Type here" />);
    expect(screen.getByPlaceholderText("Type here")).toBeInTheDocument();
  });

  it("renders label when provided", () => {
    render(<Input label="Email" />);
    expect(screen.getByLabelText("Email")).toBeInTheDocument();
  });

  it("renders error message", () => {
    render(<Input error="Required field" />);
    expect(screen.getByText("Required field")).toBeInTheDocument();
  });

  it("applies error styling", () => {
    render(<Input label="Name" error="Required" />);
    expect(screen.getByLabelText("Name").className).toContain("border-status-error");
  });

  it("accepts user input", async () => {
    render(<Input placeholder="Type" />);
    const input = screen.getByPlaceholderText("Type");
    await userEvent.type(input, "hello");
    expect(input).toHaveValue("hello");
  });

  it("applies custom className", () => {
    render(<Input className="w-full" />);
    expect(screen.getByRole("textbox").className).toContain("w-full");
  });
});
