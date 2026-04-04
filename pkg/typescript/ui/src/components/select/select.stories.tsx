// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { Select } from "./select.js";

const sampleOptions = [
  { value: "users", label: "Users" },
  { value: "orders", label: "Orders" },
  { value: "products", label: "Products" },
];

const meta = {
  title: "Components/Select",
  component: Select,
  argTypes: {
    disabled: { control: "boolean" },
  },
  tags: ["autodocs"],
} satisfies Meta<typeof Select>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    options: sampleOptions,
  },
};

export const WithLabel: Story = {
  args: {
    label: "Table",
    options: sampleOptions,
  },
};

export const Disabled: Story = {
  args: {
    label: "Table",
    options: sampleOptions,
    disabled: true,
  },
};
