import type { Meta, StoryObj } from "@storybook/react-vite";
import { CodeBlock } from "./code-block.js";

const meta = {
  title: "Components/CodeBlock",
  component: CodeBlock,
  argTypes: {
    language: {
      control: "select",
      options: ["bash", "json", "javascript", "typescript", "python", "rust"],
    },
    showCopy: { control: "boolean" },
  },
  tags: ["autodocs"],
} satisfies Meta<typeof CodeBlock>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Bash: Story = {
  args: {
    code: "curl -X GET https://api.reifydb.com/v1/query",
    language: "bash",
  },
};

export const JSON: Story = {
  args: {
    code: `{
  "table": "users",
  "rows": 1250000,
  "active": true,
  "version": "0.4.7"
}`,
    language: "json",
  },
};

export const TypeScript: Story = {
  args: {
    code: `import { ReifyClient } from "@reifydb/client";

const client = new ReifyClient({ url: "ws://localhost:9080" });
const result = await client.query("From users");`,
    language: "typescript",
  },
};

export const NoCopy: Story = {
  args: {
    code: 'echo "no copy button"',
    language: "bash",
    showCopy: false,
  },
};
