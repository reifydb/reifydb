import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { Toggle } from "./toggle.js";

const meta = {
  title: "Components/Toggle",
  component: Toggle,
  tags: ["autodocs"],
} satisfies Meta<typeof Toggle>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    options: ["Off", "On"],
    value: "Off",
  },
};

export const Interactive: Story = {
  render: () => {
    const [value, setValue] = useState("Table");
    return <Toggle options={["Table", "JSON"]} value={value} onChange={setValue} />;
  },
};

export const RightActive: Story = {
  args: {
    options: ["Light", "Dark"],
    value: "Dark",
  },
};
