// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
      className={`text-xs font-semibold px-3 py-1 rounded-full border cursor-pointer whitespace-nowrap transition-none ${
        active
          ? "text-primary border-primary/30 bg-primary/10"
          : "text-text-secondary bg-bg-tertiary border-border-light hover:text-text-primary hover:border-border-subtle"
      } ${className}`}
    >
      {children}
    </button>
  );
}
