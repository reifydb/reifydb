import type { Meta, StoryObj } from "@storybook/react-vite";
import { Separator } from "./separator.js";
import { Chip } from "../chip/chip.js";

const meta = {
  title: "Components/Separator",
  component: Separator,
  tags: ["autodocs"],
} satisfies Meta<typeof Separator>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => (
    <div className="flex items-center gap-2">
      <Chip active={true} onClick={() => {}}>Sort</Chip>
      <Separator />
      <Chip active={false} onClick={() => {}}>Filter</Chip>
      <Separator />
      <Chip active={false} onClick={() => {}}>Window</Chip>
    </div>
  ),
};
