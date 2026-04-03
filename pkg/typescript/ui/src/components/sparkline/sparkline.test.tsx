import { render } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { Sparkline } from "./sparkline.js";

describe("Sparkline", () => {
  it("renders bars for each trend value", () => {
    const { container } = render(<Sparkline trend={[3, 5, 8]} />);
    const bars = container.querySelectorAll("[style]");
    expect(bars).toHaveLength(3);
  });

  it("applies danger color for high values", () => {
    const { container } = render(<Sparkline trend={[12]} />);
    const bar = container.querySelector("[style]");
    expect(bar?.className).toContain("bg-status-error");
  });

  it("applies warning color for medium values", () => {
    const { container } = render(<Sparkline trend={[8]} />);
    const bar = container.querySelector("[style]");
    expect(bar?.className).toContain("bg-status-warning");
  });

  it("applies default color for low values", () => {
    const { container } = render(<Sparkline trend={[3]} />);
    const bar = container.querySelector("[style]");
    expect(bar?.className).toContain("bg-text-muted");
  });

  it("applies custom className", () => {
    const { container } = render(<Sparkline trend={[1, 2]} className="ml-2" />);
    expect(container.firstElementChild?.className).toContain("ml-2");
  });
});
