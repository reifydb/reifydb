import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { StatusBar } from "./status-bar.js";

describe("StatusBar", () => {
  it("renders all items", () => {
    render(
      <StatusBar items={[
        { label: "Connected" },
        { label: "Streaming" },
      ]} />,
    );
    expect(screen.getByText("Connected")).toBeInTheDocument();
    expect(screen.getByText("Streaming")).toBeInTheDocument();
  });

  it("renders dot indicators", () => {
    const { container } = render(
      <StatusBar items={[{ dot: "success", label: "OK" }]} />,
    );
    const dot = container.querySelector(".bg-status-success");
    expect(dot).toBeInTheDocument();
  });

  it("renders trailing content", () => {
    render(
      <StatusBar items={[{ label: "OK" }]} trailing="v1.0.0" />,
    );
    expect(screen.getByText("v1.0.0")).toBeInTheDocument();
  });

  it("renders without dots when not provided", () => {
    const { container } = render(
      <StatusBar items={[{ label: "No dot" }]} />,
    );
    expect(container.querySelector(".rounded-full")).toBeNull();
  });

  it("applies custom className", () => {
    const { container } = render(
      <StatusBar items={[{ label: "Test" }]} className="h-8" />,
    );
    expect(container.firstElementChild?.className).toContain("h-8");
  });
});
