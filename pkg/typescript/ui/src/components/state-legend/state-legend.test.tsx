// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { StateLegend } from "./state-legend.js";

describe("StateLegend", () => {
  it("renders only the requested states in the given order", () => {
    render(<StateLegend states={["delete", "insert"]} />);
    const items = screen.getAllByRole("listitem");
    expect(items.map((item) => item.textContent)).toEqual(["deleted", "inserted"]);
  });

  it("maps each state to its own state token", () => {
    // Insert is yellow, update sky, delete pink everywhere; a swapped swatch lies about what changed.
    const { container } = render(<StateLegend states={["insert", "update", "delete"]} />);
    const swatches = Array.from(container.querySelectorAll("li > span"));
    const tokens = ["bg-state-insert", "bg-state-update", "bg-state-delete"];
    swatches.forEach((swatch, i) => {
      for (const token of tokens) {
        if (token === tokens[i]) expect(swatch).toHaveClass(token);
        else expect(swatch).not.toHaveClass(token);
      }
    });
  });

  it("hides the swatches from screen readers", () => {
    // The label carries the meaning; colour alone must not be announced.
    const { container } = render(<StateLegend states={["insert", "update", "delete"]} />);
    for (const swatch of container.querySelectorAll("li > span")) {
      expect(swatch).toHaveAttribute("aria-hidden", "true");
    }
  });

  it("renders an empty list for no states", () => {
    render(<StateLegend states={[]} />);
    expect(screen.getByRole("list").children).toHaveLength(0);
  });

  it("keeps its own classes when a className is added", () => {
    render(<StateLegend states={["insert"]} className="px-4" />);
    expect(screen.getByRole("list")).toHaveClass("px-4", "font-mono", "uppercase");
  });
});
