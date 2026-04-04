// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { Input } from "./input.js";

const meta = {
  title: "Components/Input",
  component: Input,
  argTypes: {
    disabled: { control: "boolean" },
  },
  tags: ["autodocs"],
} satisfies Meta<typeof Input>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    placeholder: "Enter text...",
  },
};

export const WithLabel: Story = {
  args: {
    label: "Email",
    placeholder: "you@example.com",
  },
};

export const WithError: Story = {
  args: {
    label: "Email",
    value: "invalid",
    error: "Please enter a valid email address",
  },
};

export const Disabled: Story = {
  args: {
    label: "Disabled",
    value: "Cannot edit",
    disabled: true,
  },
};
