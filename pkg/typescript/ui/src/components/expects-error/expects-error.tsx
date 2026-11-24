// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export interface ExpectsErrorProps {
  code: string;
  className?: string;
}

export function ExpectsError({ code, className }: ExpectsErrorProps) {
  return (
    <span className={`font-mono text-[11px] text-text-muted ${className ?? ""}`}>expects {code}</span>
  );
}
