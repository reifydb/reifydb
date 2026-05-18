// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { ChipGroup } from "./chip-group.js";

const meta = {
  title: "Components/ChipGroup",
  component: ChipGroup,
  tags: ["autodocs"],
} satisfies Meta<typeof ChipGroup>;

export default meta;
type Story = StoryObj<typeof meta>;

export const TimeWindows: Story = {
  args: {
    options: [
      { value: "5m", label: "5m" },
      { value: "1h", label: "1h" },
      { value: "4h", label: "4h" },
      { value: "1d", label: "1d" },
    ],
    value: "1h",
  },
};

export const Filters: Story = {
  args: {
    options: [
      { value: "all", label: "All tables" },
      { value: "active", label: "Active" },
      { value: "pending", label: "Pending" },
      { value: "subscribed", label: "Subscribed" },
    ],
    value: "all",
  },
};

export const Interactive: Story = {
  render: () => {
    const [value, setValue] = useState("queries");
    return (
      <ChipGroup
        options={[
          { value: "queries", label: "Queries" },
          { value: "mutations", label: "Mutations" },
          { value: "subscriptions", label: "Subscriptions" },
        ]}
        value={value}
        onChange={setValue}
      />
    );
  },
};
