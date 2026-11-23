// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { ExpectsError } from "./expects-error.js";

describe("ExpectsError", () => {
  it("says the snippet fails and names the code", () => {
    // The editor and the docs stand-ins must print exactly this text, otherwise the label changes when the editor loads.
    render(<ExpectsError code="AST_006" />);
    expect(screen.getByText("fails with AST_006")).toBeInTheDocument();
  });

  it("draws the warning badge", () => {
    // An error-coloured label must never appear, since the failure is intended and the page is not broken.
    render(<ExpectsError code="AST_006" />);
    const className = screen.getByText("fails with AST_006").className;
    expect(className).toContain("text-status-warning");
    expect(className).not.toContain("text-status-error");
  });

  it("applies custom className", () => {
    // Without a pass-through className a header cannot space the label from its title.
    render(<ExpectsError code="CA_004" className="ml-2" />);
    expect(screen.getByText("fails with CA_004").className).toContain("ml-2");
  });
});
