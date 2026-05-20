// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

import type { ReactNode } from "react";

export interface SectionHeaderProps {
  title: string;
  badge?: ReactNode;
  className?: string;
}

export function SectionHeader({ title, badge, className = "" }: SectionHeaderProps) {
  return (
    <div className={`flex items-center justify-between bg-bg-tertiary px-3 py-2.5 rounded-t-[var(--radius-md)] ${className}`}>
      <span className="text-xs font-semibold text-text-secondary uppercase tracking-[1.4px]">{title}</span>
      {badge}
    </div>
  );
}
