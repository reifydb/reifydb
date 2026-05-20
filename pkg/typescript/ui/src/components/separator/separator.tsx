// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

export interface SeparatorProps {
  className?: string;
}

export function Separator({ className = "" }: SeparatorProps) {
  return <div className={`mx-1 h-5 w-px shrink-0 bg-border-default ${className}`} />;
}
