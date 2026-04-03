import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { CodeBlock } from "./code-block.js";

describe("CodeBlock", () => {
  it("renders code content", () => {
    const { container } = render(<CodeBlock code="const x = 1;" language="javascript" />);
    const codeEl = container.querySelector("code");
    expect(codeEl?.textContent).toContain("const x = 1;");
  });

  it("renders copy button by default", () => {
    render(<CodeBlock code="test" />);
    expect(screen.getByRole("button")).toBeInTheDocument();
  });

  it("hides copy button when showCopy is false", () => {
    render(<CodeBlock code="test" showCopy={false} />);
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("applies language class to code element", () => {
    const { container } = render(<CodeBlock code="test" language="json" />);
    const codeEl = container.querySelector("code");
    expect(codeEl?.className).toContain("language-json");
  });

  it("applies custom className", () => {
    const { container } = render(<CodeBlock code="test" className="rounded-lg" />);
    expect(container.firstElementChild?.className).toContain("rounded-lg");
  });
});
