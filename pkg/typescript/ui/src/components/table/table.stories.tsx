// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { Table, TableHead, TableHeader, TableBody, TableRow, TableCell } from "./table.js";

const meta = {
  title: "Components/Table",
  component: Table,
  tags: ["autodocs"],
} satisfies Meta<typeof Table>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => (
    <Table>
      <TableHead>
        <TableHeader>Name</TableHeader>
        <TableHeader>Status</TableHeader>
        <TableHeader>Rows</TableHeader>
      </TableHead>
      <TableBody>
        <TableRow>
          <TableCell>users</TableCell>
          <TableCell>Active</TableCell>
          <TableCell>1,250,000</TableCell>
        </TableRow>
        <TableRow>
          <TableCell>orders</TableCell>
          <TableCell>Active</TableCell>
          <TableCell>3,200,000</TableCell>
        </TableRow>
        <TableRow>
          <TableCell>products</TableCell>
          <TableCell>Paused</TableCell>
          <TableCell>67,000</TableCell>
        </TableRow>
      </TableBody>
    </Table>
  ),
};

export const ClickableRows: Story = {
  render: () => (
    <Table>
      <TableHead>
        <TableHeader>Table</TableHeader>
        <TableHeader>Rows</TableHeader>
      </TableHead>
      <TableBody>
        <TableRow onClick={() => alert("Clicked users")}>
          <TableCell>users</TableCell>
          <TableCell>1,250,000</TableCell>
        </TableRow>
        <TableRow onClick={() => alert("Clicked orders")}>
          <TableCell>orders</TableCell>
          <TableCell>3,200,000</TableCell>
        </TableRow>
      </TableBody>
    </Table>
  ),
};

export const SortableHeaders: Story = {
  render: () => (
    <Table>
      <TableHead>
        <TableHeader onClick={() => alert("Sort by name")}>Name</TableHeader>
        <TableHeader onClick={() => alert("Sort by value")}>Value</TableHeader>
      </TableHead>
      <TableBody>
        <TableRow>
          <TableCell>Alpha</TableCell>
          <TableCell>100</TableCell>
        </TableRow>
      </TableBody>
    </Table>
  ),
};
