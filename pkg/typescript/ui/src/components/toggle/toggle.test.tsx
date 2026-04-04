// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, it, expect, vi } from "vitest";
import { Toggle } from "./toggle.js";

describe("Toggle", () => {
  it("renders both options", () => {
    render(<Toggle options={["Off", "On"]} value="Off" onChange={() => {}} />);
    expect(screen.getByText("Off")).toBeInTheDocument();
    expect(screen.getByText("On")).toBeInTheDocument();
  });

  it("calls onChange when switch is clicked", async () => {
    const onChange = vi.fn();
    render(<Toggle options={["Off", "On"]} value="Off" onChange={onChange} />);
    await userEvent.click(screen.getByRole("button"));
    expect(onChange).toHaveBeenCalledWith("On");
  });

  it("calls onChange when label is clicked", async () => {
    const onChange = vi.fn();
    render(<Toggle options={["Off", "On"]} value="Off" onChange={onChange} />);
    await userEvent.click(screen.getByText("On"));
    expect(onChange).toHaveBeenCalledWith("On");
  });

  it("highlights active option", () => {
    render(<Toggle options={["Off", "On"]} value="On" onChange={() => {}} />);
    expect(screen.getByText("On").className).toContain("text-text-primary");
    expect(screen.getByText("Off").className).toContain("text-text-muted");
  });
});
