// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { ExpectsError } from "./expects-error.js";

const meta = {
  title: "Components/ExpectsError",
  component: ExpectsError,
  tags: ["autodocs"],
} satisfies Meta<typeof ExpectsError>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: { code: "AST_006" },
};
