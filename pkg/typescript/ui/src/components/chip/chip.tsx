// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ReactNode } from "react";

export interface ChipProps {
  active: boolean;
  onClick: () => void;
  children: ReactNode;
  className?: string;
}

export function Chip({ active, onClick, children, className = "" }: ChipProps) {
  return (
    <button
      onClick={onClick}
      className={`text-xs font-medium px-3 py-1 rounded-[var(--radius-sm)] border cursor-pointer whitespace-nowrap transition-all ${
        active
          ? "text-primary border-primary/30 bg-primary/10"
          : "text-text-secondary bg-bg-secondary border-border-default hover:text-text-primary hover:border-border-light"
      } ${className}`}
    >
      {children}
    </button>
  );
}
