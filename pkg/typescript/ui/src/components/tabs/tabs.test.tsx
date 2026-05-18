// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, it, expect, vi } from "vitest";
import { Tabs } from "./tabs.js";

const tabs = [
  { value: "a", label: "Tab A" },
  { value: "b", label: "Tab B" },
  { value: "c", label: "Tab C" },
];

describe("Tabs", () => {
  it("renders all tabs", () => {
    render(<Tabs tabs={tabs} value="a" onChange={() => {}} />);
    expect(screen.getAllByRole("button")).toHaveLength(3);
  });

  it("renders tab labels", () => {
    render(<Tabs tabs={tabs} value="a" onChange={() => {}} />);
    expect(screen.getByText("Tab A")).toBeInTheDocument();
    expect(screen.getByText("Tab B")).toBeInTheDocument();
  });

  it("calls onChange when tab is clicked", async () => {
    const onChange = vi.fn();
    render(<Tabs tabs={tabs} value="a" onChange={onChange} />);
    await userEvent.click(screen.getByText("Tab B"));
    expect(onChange).toHaveBeenCalledWith("b");
  });

  it("highlights active tab", () => {
    render(<Tabs tabs={tabs} value="b" onChange={() => {}} />);
    expect(screen.getByText("Tab B").className).toContain("bg-bg-tertiary");
    expect(screen.getByText("Tab A").className).not.toContain("bg-bg-tertiary");
  });
});
