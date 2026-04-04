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
      className={`text-xs font-medium px-3 py-1 rounded-lg border cursor-pointer whitespace-nowrap transition-all ${
        active
          ? "text-primary border-primary/30 bg-primary/10"
          : "text-text-secondary bg-white/[0.04] border-white/[0.08] hover:text-text-primary hover:border-white/[0.12]"
      } ${className}`}
    >
      {children}
    </button>
  );
}
