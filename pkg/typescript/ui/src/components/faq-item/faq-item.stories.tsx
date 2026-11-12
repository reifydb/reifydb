// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { Meta, StoryObj } from "@storybook/react-vite";
import { FaqItem } from "./faq-item.js";

const meta = {
  title: "Components/FaqItem",
  component: FaqItem,
  tags: ["autodocs"],
} satisfies Meta<typeof FaqItem>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    question: "Is every view updated in the same transaction?",
    answer: "Yes. A write and every view built on it commit together, or not at all.",
  },
};

export const WithPortrait: Story = {
  args: {
    question: "Can my agent build on it?",
    answer: "Yes. RQL reads top to bottom, so agents write it well.",
    portrait: <span className="size-6 bg-core" />,
  },
};

export const List: Story = {
  args: {
    question: "",
    answer: "",
  },
  render: () => (
    <div className="max-w-3xl">
      <FaqItem question="Is it open source?" answer="Yes, under Apache 2.0." />
      <FaqItem question="Does it run in the browser?" answer="Yes, the engine compiles to WASM." />
      <FaqItem question="Is it RQL or SQL?" answer="RQL, a pipeline language." />
    </div>
  ),
};
