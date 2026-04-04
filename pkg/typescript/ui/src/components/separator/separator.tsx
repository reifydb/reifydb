// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

export interface SeparatorProps {
  className?: string;
}

export function Separator({ className = "" }: SeparatorProps) {
  return <div className={`mx-1 h-5 w-px shrink-0 bg-white/[0.12] ${className}`} />;
}
