// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { Section } from "./section.js";
import { Container } from "../container/container.js";

const meta = {
  title: "Components/Section",
  component: Section,
  tags: ["autodocs"],
} satisfies Meta<typeof Section>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Plain: Story = {
  args: {
    children: <Container>Plain section</Container>,
  },
};

export const Tint: Story = {
  args: {
    tone: "tint",
    children: <Container>Tint section</Container>,
  },
};

export const Alternating: Story = {
  args: {
    children: null,
  },
  render: () => (
    <>
      <Section><Container>Plain</Container></Section>
      <Section tone="tint"><Container>Tint</Container></Section>
      <Section><Container>Plain</Container></Section>
    </>
  ),
};
