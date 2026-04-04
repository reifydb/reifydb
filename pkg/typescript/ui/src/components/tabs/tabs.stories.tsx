// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { Tabs } from "./tabs.js";

const sampleTabs = [
  { value: "overview", label: "Overview" },
  { value: "analytics", label: "Analytics" },
  { value: "settings", label: "Settings" },
];

const meta = {
  title: "Components/Tabs",
  component: Tabs,
  tags: ["autodocs"],
} satisfies Meta<typeof Tabs>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    tabs: sampleTabs,
    value: "overview",
  },
};

export const Interactive: Story = {
  render: () => {
    const [value, setValue] = useState("overview");
    return (
      <div>
        <Tabs tabs={sampleTabs} value={value} onChange={setValue} />
        <p className="mt-4 text-sm text-text-secondary">Active: {value}</p>
      </div>
    );
  },
};

export const TwoTabs: Story = {
  args: {
    tabs: [
      { value: "code", label: "Code" },
      { value: "preview", label: "Preview" },
    ],
    value: "code",
  },
};
