import type { Meta, StoryObj } from "@storybook/react-vite";
import { CopyButton } from "./copy-button.js";

const meta = {
  title: "Components/CopyButton",
  component: CopyButton,
  tags: ["autodocs"],
} satisfies Meta<typeof CopyButton>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    text: "Hello, world!",
  },
};

export const WithLabel: Story = {
  args: {
    text: "npx @reifydb/cli init",
    label: "Copy",
  },
};

export const CustomClass: Story = {
  args: {
    text: "custom text",
    label: "Copy command",
    className: "bg-bg-secondary",
  },
};
