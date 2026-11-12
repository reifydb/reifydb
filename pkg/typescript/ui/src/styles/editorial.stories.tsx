// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";

const meta = {
  title: "Styles/Editorial classes",
} satisfies Meta;

export default meta;
type Story = StoryObj<typeof meta>;

export const Eyebrow: Story = {
  render: () => <p className="eyebrow uppercase">The backend platform for you and your agents</p>,
};

export const Label: Story = {
  render: () => <span className="label text-text-muted">market::trades</span>,
};

export const Marker: Story = {
  render: () => (
    <p className="text-lg">
      Every view updates <span className="marker">in the same transaction</span>.
    </p>
  ),
};

export const LinkAccent: Story = {
  render: () => (
    <a className="link-accent" href="#docs">
      Read the docs
    </a>
  ),
};

export const Sky: Story = {
  render: () => <div className="sky h-40 w-full" />,
};

export const Dialog: Story = {
  render: () => <div className="dialog max-w-sm p-4">A figure or table in a dialog frame.</div>,
};

export const FactsBand: Story = {
  render: () => (
    <div className="facts-band flex py-8">
      <div className="facts-band__fact">
        <p className="facts-band__label">License</p>
        <p className="facts-band__value">Apache 2.0</p>
      </div>
      <div className="facts-band__fact">
        <p className="facts-band__label">Runs in</p>
        <p className="facts-band__value">Browser and server</p>
      </div>
    </div>
  ),
};
