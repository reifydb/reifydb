// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { PageHero } from "./page-hero.js";
import { Button } from "../button/button.js";

const meta = {
  title: "Components/PageHero",
  component: PageHero,
  tags: ["autodocs"],
} satisfies Meta<typeof PageHero>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    eyebrow: "Company",
    title: "Your whole backend, in one transactional database.",
    subtitle: "Tables, views, queues and procedures in one place.",
  },
};

export const WithActions: Story = {
  args: {
    eyebrow: "Uptime",
    title: "Monitors",
    subtitle: "Every probe result lands in one transaction.",
    children: (
      <>
        <Button variant="primary">New monitor</Button>
        <Button variant="secondary">Read docs</Button>
      </>
    ),
  },
};

export const TitleOnly: Story = {
  args: {
    title: "Status pages",
  },
};
