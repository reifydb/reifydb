// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ReactNode } from "react";

export interface KbdProps {
  children: ReactNode;
  className?: string;
}

export function Kbd({ children, className = "" }: KbdProps) {
  return (
    <kbd
      className={`inline-flex h-5 min-w-5 items-center justify-center rounded border border-border-default
        bg-bg-tertiary px-1.5 font-mono text-[10px] font-medium text-text-muted ${className}`}
    >
      {children}
    </kbd>
  );
}
