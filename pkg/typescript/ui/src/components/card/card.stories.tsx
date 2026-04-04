// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { Card, CardHeader, CardTitle, CardDescription } from "./card.js";

const meta = {
  title: "Components/Card",
  component: Card,
  tags: ["autodocs"],
} satisfies Meta<typeof Card>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    children: "Card content goes here",
  },
};

export const WithHeader: Story = {
  render: () => (
    <Card>
      <CardHeader>
        <CardTitle>Card Title</CardTitle>
        <CardDescription>A short description of this card.</CardDescription>
      </CardHeader>
      <p className="text-sm text-text-secondary">Card body content goes here.</p>
    </Card>
  ),
};

export const CustomClassName: Story = {
  args: {
    children: "Custom styled card",
    className: "max-w-sm",
  },
};
