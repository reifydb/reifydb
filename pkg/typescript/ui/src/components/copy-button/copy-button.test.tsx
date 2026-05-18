// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { CopyButton } from "./copy-button.js";

describe("CopyButton", () => {
  beforeEach(() => {
    Object.assign(navigator, {
      clipboard: { writeText: vi.fn().mockResolvedValue(undefined) },
    });
  });

  it("renders a button", () => {
    render(<CopyButton text="test" />);
    expect(screen.getByRole("button")).toBeInTheDocument();
  });

  it("renders label when provided", () => {
    render(<CopyButton text="test" label="Copy" />);
    expect(screen.getByText("Copy")).toBeInTheDocument();
  });

  it("copies text to clipboard on click", async () => {
    render(<CopyButton text="hello" />);
    await userEvent.click(screen.getByRole("button"));
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith("hello");
  });

  it("shows copied state after click", async () => {
    render(<CopyButton text="test" label="Copy" />);
    await userEvent.click(screen.getByRole("button"));
    expect(screen.getByText("Copied!")).toBeInTheDocument();
  });

  it("calls onCopy callback", async () => {
    const onCopy = vi.fn();
    render(<CopyButton text="test" onCopy={onCopy} />);
    await userEvent.click(screen.getByRole("button"));
    expect(onCopy).toHaveBeenCalledOnce();
  });
});
