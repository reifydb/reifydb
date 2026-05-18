// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, it, expect } from "vitest";
import { Select } from "./select.js";

const options = [
  { value: "a", label: "Alpha" },
  { value: "b", label: "Beta" },
  { value: "c", label: "Gamma" },
];

describe("Select", () => {
  it("renders all options", () => {
    render(<Select options={options} />);
    expect(screen.getAllByRole("option")).toHaveLength(3);
  });

  it("renders label when provided", () => {
    render(<Select label="Choose" options={options} />);
    expect(screen.getByLabelText("Choose")).toBeInTheDocument();
  });

  it("selects an option", async () => {
    render(<Select label="Pick" options={options} />);
    const select = screen.getByLabelText("Pick");
    await userEvent.selectOptions(select, "b");
    expect(select).toHaveValue("b");
  });

  it("applies custom className", () => {
    render(<Select options={options} className="w-full" />);
    expect(screen.getByRole("combobox").className).toContain("w-full");
  });
});
