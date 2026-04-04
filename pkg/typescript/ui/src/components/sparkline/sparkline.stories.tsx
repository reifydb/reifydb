// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { Sparkline } from "./sparkline.js";

const meta = {
  title: "Components/Sparkline",
  component: Sparkline,
  tags: ["autodocs"],
} satisfies Meta<typeof Sparkline>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    trend: [3, 5, 8, 6, 4, 9, 12, 7, 5],
  },
};

export const HighValues: Story = {
  args: {
    trend: [10, 12, 15, 11, 14, 13, 12, 10, 11],
  },
};

export const LowValues: Story = {
  args: {
    trend: [1, 2, 3, 2, 1, 2, 3, 4, 2],
  },
};

export const MixedValues: Story = {
  args: {
    trend: [2, 5, 12, 3, 8, 15, 4, 7, 10],
  },
};
