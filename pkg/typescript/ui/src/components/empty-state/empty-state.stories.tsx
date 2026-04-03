import type { Meta, StoryObj } from "@storybook/react-vite";
import { EmptyState } from "./empty-state.js";
import { Button } from "../button/button.js";

const meta = {
  title: "Components/EmptyState",
  component: EmptyState,
  tags: ["autodocs"],
} satisfies Meta<typeof EmptyState>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    title: "No data found",
  },
};

export const WithDescription: Story = {
  args: {
    title: "No results",
    description: "Try adjusting your search or filters to find what you're looking for.",
  },
};

export const WithAction: Story = {
  args: {
    title: "No items yet",
    description: "Get started by creating your first item.",
    action: <Button size="sm">Create Item</Button>,
  },
};

export const WithIcon: Story = {
  args: {
    icon: (
      <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
        <path d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    ),
    title: "No results found",
    description: "We couldn't find anything matching your query.",
  },
};
