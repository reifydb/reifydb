// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { SectionHeader } from "./section-header.js";
import { Badge } from "../badge/badge.js";

const meta = {
  title: "Components/SectionHeader",
  component: SectionHeader,
  tags: ["autodocs"],
} satisfies Meta<typeof SectionHeader>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    title: "Table schema",
  },
};

export const WithBadge: Story = {
  args: {
    title: "Recent queries",
    badge: <Badge variant="active">3 active</Badge>,
  },
};

export const WithLiveBadge: Story = {
  args: {
    title: "Active subscriptions",
    badge: (
      <Badge variant="active">
        <span className="status-dot status-dot-connected !h-1.5 !w-1.5" />
        live
      </Badge>
    ),
  },
};
