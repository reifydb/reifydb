import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { SectionHeader } from "./section-header.js";

describe("SectionHeader", () => {
  it("renders title", () => {
    render(<SectionHeader title="My Section" />);
    expect(screen.getByText("My Section")).toBeInTheDocument();
  });

  it("renders badge when provided", () => {
    render(<SectionHeader title="Section" badge={<span>3 active</span>} />);
    expect(screen.getByText("3 active")).toBeInTheDocument();
  });

  it("does not render badge when not provided", () => {
    const { container } = render(<SectionHeader title="Section" />);
    expect(container.firstElementChild?.children).toHaveLength(1);
  });

  it("applies custom className", () => {
    const { container } = render(<SectionHeader title="Section" className="rounded-none" />);
    expect(container.firstElementChild?.className).toContain("rounded-none");
  });
});
