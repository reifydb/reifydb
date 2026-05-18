// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { Badge } from "./badge.js";

const meta = {
  title: "Components/Badge",
  component: Badge,
  argTypes: {
    variant: {
      control: "select",
      options: ["active", "inactive", "coming-soon", "default", "signal", "success", "danger", "warning"],
    },
  },
  tags: ["autodocs"],
} satisfies Meta<typeof Badge>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: { children: "Default" },
};

export const Active: Story = {
  args: { children: "Active", variant: "active" },
};

export const Success: Story = {
  args: { children: "Connected", variant: "success" },
};

export const Danger: Story = {
  args: { children: "Error", variant: "danger" },
};

export const Warning: Story = {
  args: { children: "Pending", variant: "warning" },
};

export const Signal: Story = {
  args: { children: "Signal", variant: "signal" },
};

export const Inactive: Story = {
  args: { children: "Offline", variant: "inactive" },
};

export const ComingSoon: Story = {
  args: { children: "Coming Soon", variant: "coming-soon" },
};
