import type { Meta, StoryObj } from "@storybook/react-vite";
import { StatusBar } from "./status-bar.js";

const meta = {
  title: "Components/StatusBar",
  component: StatusBar,
  tags: ["autodocs"],
} satisfies Meta<typeof StatusBar>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    items: [
      { dot: "success", label: "WS connected" },
      { dot: "success", label: "12 tables active" },
      { dot: "warning", label: "3 pending mutations" },
    ],
    trailing: "v0.4.7",
  },
};

export const AllGreen: Story = {
  args: {
    items: [
      { dot: "success", label: "Database connected" },
      { dot: "success", label: "All tables healthy" },
      { dot: "success", label: "Subscriptions active" },
    ],
    trailing: "reifydb v0.4.7",
  },
};

export const WithErrors: Story = {
  args: {
    items: [
      { dot: "success", label: "Database connected" },
      { dot: "danger", label: "Query timeout" },
      { dot: "warning", label: "Degraded performance" },
    ],
  },
};

export const NoDots: Story = {
  args: {
    items: [
      { label: "Rows: 4,821,334" },
      { label: "Latency: 12ms" },
    ],
    trailing: "UTC 13:45:22",
  },
};
