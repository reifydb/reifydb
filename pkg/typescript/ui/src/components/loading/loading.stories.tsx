import type { Meta, StoryObj } from "@storybook/react-vite";
import { Loading } from "./loading.js";

const meta = {
  title: "Components/Loading",
  component: Loading,
  tags: ["autodocs"],
} satisfies Meta<typeof Loading>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const CustomText: Story = {
  args: { text: "Fetching data" },
};

export const Connecting: Story = {
  args: { text: "Connecting" },
};
