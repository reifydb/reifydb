// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ReactNode } from "react";

type BadgeVariant = "active" | "inactive" | "coming-soon" | "default" | "signal" | "success" | "danger" | "warning" | "error" | "outline";

const variantStyles: Record<BadgeVariant, string> = {
  active: "font-bold text-primary",
  inactive: "text-text-secondary",
  "coming-soon": "text-text-secondary",
  default: "text-text-primary",
  signal: "font-bold text-primary",
  success: "text-[11px] font-medium rounded-full border px-2.5 py-0.5 bg-status-success/15 text-status-success border-status-success/30",
  danger: "text-[11px] font-medium rounded-full border px-2.5 py-0.5 bg-status-error/15 text-status-error border-status-error/30",
  warning: "text-[11px] font-medium rounded-full border px-2.5 py-0.5 bg-status-warning/15 text-status-warning border-status-warning/30",
  error: "text-[11px] font-medium rounded-full border px-2.5 py-0.5 bg-status-error/15 text-status-error border-status-error/30",
  outline: "text-text-muted",
};

export interface BadgeProps {
  variant?: BadgeVariant;
  children: ReactNode;
  className?: string;
  onClick?: () => void;
}

export function Badge({ variant = "default", children, className = "", onClick }: BadgeProps) {
  return (
    <span
      className={`inline-flex items-center ${variantStyles[variant]} ${className}`}
      onClick={onClick}
    >
      {children}
    </span>
  );
}
