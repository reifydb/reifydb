// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { Modal } from "./modal.js";
import { Button } from "../button/button.js";

const meta = {
  title: "Components/Modal",
  component: Modal,
  tags: ["autodocs"],
} satisfies Meta<typeof Modal>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    open: true,
    title: "Confirm Action",
    children: (
      <div>
        <p className="text-sm text-text-secondary">Are you sure you want to proceed?</p>
        <div className="mt-4 flex gap-2 justify-end">
          <Button variant="secondary" size="sm">Cancel</Button>
          <Button size="sm">Confirm</Button>
        </div>
      </div>
    ),
  },
};

export const Interactive: Story = {
  render: () => {
    const [open, setOpen] = useState(false);
    return (
      <div>
        <Button onClick={() => setOpen(true)}>Open Modal</Button>
        <Modal open={open} onClose={() => setOpen(false)} title="Example Modal">
          <p className="text-sm text-text-secondary">This modal can be closed with Escape or clicking the backdrop.</p>
          <div className="mt-4 flex justify-end">
            <Button size="sm" onClick={() => setOpen(false)}>Done</Button>
          </div>
        </Modal>
      </div>
    );
  },
};
