// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, it, expect, vi } from "vitest";
import { Table, TableHead, TableHeader, TableBody, TableRow, TableCell } from "./table.js";

describe("Table", () => {
  it("renders a table", () => {
    render(
      <Table>
        <TableHead>
          <TableHeader>Col</TableHeader>
        </TableHead>
        <TableBody>
          <TableRow>
            <TableCell>Data</TableCell>
          </TableRow>
        </TableBody>
      </Table>,
    );
    expect(screen.getByRole("table")).toBeInTheDocument();
    expect(screen.getByText("Col")).toBeInTheDocument();
    expect(screen.getByText("Data")).toBeInTheDocument();
  });

  it("supports clickable headers", async () => {
    const onClick = vi.fn();
    render(
      <Table>
        <TableHead>
          <TableHeader onClick={onClick}>Sort</TableHeader>
        </TableHead>
        <TableBody>
          <TableRow><TableCell>Row</TableCell></TableRow>
        </TableBody>
      </Table>,
    );
    await userEvent.click(screen.getByText("Sort"));
    expect(onClick).toHaveBeenCalledOnce();
  });

  it("supports clickable rows", async () => {
    const onClick = vi.fn();
    render(
      <Table>
        <TableHead>
          <TableHeader>Col</TableHeader>
        </TableHead>
        <TableBody>
          <TableRow onClick={onClick}><TableCell>Click me</TableCell></TableRow>
        </TableBody>
      </Table>,
    );
    await userEvent.click(screen.getByText("Click me"));
    expect(onClick).toHaveBeenCalledOnce();
  });
});
