// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { StateLegend } from "./state-legend.js";

const meta = {
  title: "Components/StateLegend",
  component: StateLegend,
  tags: ["autodocs"],
} satisfies Meta<typeof StateLegend>;

export default meta;
type Story = StoryObj<typeof meta>;

export const AllStates: Story = {
  args: {
    states: ["insert", "update", "delete"],
  },
};

export const InsertOnly: Story = {
  args: {
    states: ["insert"],
  },
};
