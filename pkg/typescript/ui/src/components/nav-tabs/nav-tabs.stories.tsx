// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { NavTabs } from "./nav-tabs.js";

const consoleTabs = [
  { label: "Overview", href: "/console/overview", isActive: true },
  { label: "Tables", href: "/console/tables" },
  { label: "Queries", href: "/console/queries" },
  { label: "Mutations", href: "/console/mutations" },
  { label: "Settings", href: "/console/settings" },
  { label: "Team", href: "/console/team" },
];

const dataTabs = [
  { label: "Rows", href: "/rows", isActive: true },
  { label: "Schema", href: "/schema" },
  { label: "Indexes", href: "/indexes" },
  { label: "Triggers", href: "/triggers" },
  { label: "History", href: "/history" },
];

const meta = {
  title: "Components/NavTabs",
  component: NavTabs,
  argTypes: {
    variant: {
      control: "select",
      options: ["underline", "pill"],
    },
  },
  tags: ["autodocs"],
} satisfies Meta<typeof NavTabs>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Underline: Story = {
  args: {
    items: consoleTabs,
    variant: "underline",
  },
};

export const Pill: Story = {
  args: {
    items: dataTabs,
    variant: "pill",
  },
};

export const Interactive: Story = {
  render: () => {
    const [active, setActive] = useState("/console/overview");
    const items = consoleTabs.map((t) => ({ ...t, isActive: t.href === active }));
    return (
      <NavTabs
        items={items}
        variant="underline"
        renderLink={({ href, className, children }) => (
          <button
            className={className}
            onClick={(e) => {
              e.preventDefault();
              setActive(href);
            }}
          >
            {children}
          </button>
        )}
      />
    );
  },
};

export const InteractivePill: Story = {
  render: () => {
    const [active, setActive] = useState("/rows");
    const items = dataTabs.map((t) => ({ ...t, isActive: t.href === active }));
    return (
      <NavTabs
        items={items}
        variant="pill"
        renderLink={({ href, className, children }) => (
          <button
            className={className}
            onClick={(e) => {
              e.preventDefault();
              setActive(href);
            }}
          >
            {children}
          </button>
        )}
      />
    );
  },
};
