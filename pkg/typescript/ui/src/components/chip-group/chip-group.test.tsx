import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, it, expect, vi } from "vitest";
import { ChipGroup } from "./chip-group.js";

const options = [
  { value: "a", label: "Alpha" },
  { value: "b", label: "Beta" },
  { value: "c", label: "Gamma" },
];

describe("ChipGroup", () => {
  it("renders all options", () => {
    render(<ChipGroup options={options} value="a" onChange={() => {}} />);
    expect(screen.getAllByRole("button")).toHaveLength(3);
    expect(screen.getByText("Alpha")).toBeInTheDocument();
    expect(screen.getByText("Beta")).toBeInTheDocument();
    expect(screen.getByText("Gamma")).toBeInTheDocument();
  });

  it("highlights the active option", () => {
    render(<ChipGroup options={options} value="b" onChange={() => {}} />);
    expect(screen.getByText("Beta").className).toContain("text-primary ");
    expect(screen.getByText("Alpha").className).toContain("text-text-secondary");
  });

  it("calls onChange with the clicked option value", async () => {
    const onChange = vi.fn();
    render(<ChipGroup options={options} value="a" onChange={onChange} />);
    await userEvent.click(screen.getByText("Gamma"));
    expect(onChange).toHaveBeenCalledWith("c");
  });

  it("applies custom className", () => {
    const { container } = render(
      <ChipGroup options={options} value="a" onChange={() => {}} className="gap-4" />,
    );
    expect(container.firstElementChild?.className).toContain("gap-4");
  });
});
