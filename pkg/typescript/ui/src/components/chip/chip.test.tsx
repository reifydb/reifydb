// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, it, expect, vi } from "vitest";
import { Chip } from "./chip.js";

describe("Chip", () => {
  it("renders children", () => {
    render(<Chip active={false} onClick={() => {}}>Filter</Chip>);
    expect(screen.getByText("Filter")).toBeInTheDocument();
  });

  it("calls onClick when clicked", async () => {
    const onClick = vi.fn();
    render(<Chip active={false} onClick={onClick}>Click</Chip>);
    await userEvent.click(screen.getByRole("button"));
    expect(onClick).toHaveBeenCalledOnce();
  });

  it("applies active styling", () => {
    render(<Chip active={true} onClick={() => {}}>Active</Chip>);
    expect(screen.getByRole("button").className).toContain("text-primary ");
  });

  it("applies inactive styling", () => {
    render(<Chip active={false} onClick={() => {}}>Inactive</Chip>);
    expect(screen.getByRole("button").className).toContain("text-text-secondary");
  });

  it("applies custom className", () => {
    render(<Chip active={false} onClick={() => {}} className="ml-2">Test</Chip>);
    expect(screen.getByRole("button").className).toContain("ml-2");
  });
});
