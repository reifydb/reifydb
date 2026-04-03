import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { Chip } from "./chip.js";

const meta = {
  title: "Components/Chip",
  component: Chip,
  argTypes: {
    active: { control: "boolean" },
  },
  tags: ["autodocs"],
} satisfies Meta<typeof Chip>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Active: Story = {
  args: {
    active: true,
    children: "Selected",
    onClick: () => {},
  },
};

export const Inactive: Story = {
  args: {
    active: false,
    children: "Not selected",
    onClick: () => {},
  },
};

export const FilterGroup: Story = {
  render: () => {
    const [active, setActive] = useState("all");
    const filters = ["All tables", "Active queries", "Pending mutations", "Subscriptions"];
    return (
      <div className="flex items-center gap-2">
        {filters.map((f) => (
          <Chip key={f} active={active === f} onClick={() => setActive(f)}>
            {f}
          </Chip>
        ))}
      </div>
    );
  },
};

export const TimeWindows: Story = {
  render: () => {
    const [active, setActive] = useState("1h");
    const windows = ["5m", "1h", "4h", "1d"];
    return (
      <div className="flex items-center gap-2">
        {windows.map((w) => (
          <Chip key={w} active={active === w} onClick={() => setActive(w)}>
            {w}
          </Chip>
        ))}
      </div>
    );
  },
};
