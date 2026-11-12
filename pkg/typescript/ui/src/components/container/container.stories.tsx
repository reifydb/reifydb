// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { Container } from "./container.js";

const meta = {
  title: "Components/Container",
  component: Container,
  tags: ["autodocs"],
} satisfies Meta<typeof Container>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Wide: Story = {
  args: {
    children: <div className="border border-rule p-4">Wide container, 1280px max</div>,
  },
};

export const Narrow: Story = {
  args: {
    width: "narrow",
    children: <div className="border border-rule p-4">Narrow container, 768px max for prose</div>,
  },
};
