// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ReactNode } from "react";

export interface NavTabItem {
  label: string;
  href: string;
  isActive?: boolean;
}

export interface NavTabsProps {
  items: NavTabItem[];
  variant?: "underline" | "pill";
  className?: string;
  renderLink?: (props: { href: string; className: string; children: ReactNode }) => ReactNode;
}

const variantStyles = {
  underline: {
    container: "flex border-b border-border-light bg-transparent",
    item: "px-5 py-2.5 font-mono text-xs uppercase tracking-[1.4px] transition-colors border-b-2",
    active: "border-primary font-bold text-text-primary",
    inactive: "border-transparent text-text-muted hover:text-text-primary",
  },
  pill: {
    container: "flex items-center gap-1",
    item: "px-3 py-2 text-sm font-medium transition-colors rounded-[var(--radius-sm)]",
    active: "bg-bg-tertiary text-text-primary",
    inactive: "text-text-muted hover:bg-bg-secondary hover:text-text-primary",
  },
};

export function NavTabs({ items, variant = "underline", className = "", renderLink }: NavTabsProps) {
  const styles = variantStyles[variant];

  return (
    <nav className={`${styles.container} ${className}`}>
      {items.map((item) => {
        const itemClassName = `${styles.item} ${item.isActive ? styles.active : styles.inactive}`;

        if (renderLink) {
          return (
            <span key={item.href}>
              {renderLink({ href: item.href, className: itemClassName, children: item.label })}
            </span>
          );
        }

        return (
          <a key={item.href} href={item.href} className={itemClassName}>
            {item.label}
          </a>
        );
      })}
    </nav>
  );
}
