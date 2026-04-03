import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { NavTabs } from "./nav-tabs.js";

const items = [
  { label: "Overview", href: "/overview", isActive: true },
  { label: "Settings", href: "/settings" },
  { label: "Billing", href: "/billing" },
];

describe("NavTabs", () => {
  it("renders all items as links", () => {
    render(<NavTabs items={items} />);
    expect(screen.getAllByRole("link")).toHaveLength(3);
    expect(screen.getByText("Overview")).toBeInTheDocument();
    expect(screen.getByText("Settings")).toBeInTheDocument();
  });

  it("applies active styling to active item (underline)", () => {
    render(<NavTabs items={items} variant="underline" />);
    expect(screen.getByText("Overview").className).toContain("border-primary");
    expect(screen.getByText("Settings").className).toContain("border-transparent");
  });

  it("applies active styling to active item (pill)", () => {
    render(<NavTabs items={items} variant="pill" />);
    expect(screen.getByText("Overview").className).toContain("bg-white/[0.06]");
    expect(screen.getByText("Settings").className).not.toContain("bg-white/[0.06] ");
  });

  it("sets correct href on links", () => {
    render(<NavTabs items={items} />);
    expect(screen.getByText("Overview").closest("a")?.getAttribute("href")).toBe("/overview");
    expect(screen.getByText("Billing").closest("a")?.getAttribute("href")).toBe("/billing");
  });

  it("uses renderLink when provided", () => {
    render(
      <NavTabs
        items={items}
        renderLink={({ href, className, children }) => (
          <button data-href={href} className={className}>{children}</button>
        )}
      />,
    );
    expect(screen.getAllByRole("button")).toHaveLength(3);
    expect(screen.getByText("Overview").getAttribute("data-href")).toBe("/overview");
  });

  it("applies custom className to container", () => {
    const { container } = render(<NavTabs items={items} className="justify-center" />);
    expect(container.querySelector("nav")?.className).toContain("justify-center");
  });

  it("defaults to underline variant", () => {
    const { container } = render(<NavTabs items={items} />);
    expect(container.querySelector("nav")?.className).toContain("border-b");
  });
});
