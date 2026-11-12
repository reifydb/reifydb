// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export type StateKind = "insert" | "update" | "delete";

export interface StateLegendProps {
  states: StateKind[];
  className?: string;
}

const STATES: Record<StateKind, { label: string; swatch: string }> = {
  insert: { label: "inserted", swatch: "bg-state-insert" },
  update: { label: "updated", swatch: "bg-state-update" },
  delete: { label: "deleted", swatch: "bg-state-delete" },
};

export function StateLegend({ states, className = "" }: StateLegendProps) {
  return (
    <ul className={`flex flex-wrap items-center gap-x-3 gap-y-1 font-mono text-[10px] font-medium uppercase tracking-wider text-code-text-muted ${className}`}>
      {states.map((state) => (
        <li key={state} className="flex items-center gap-1.5">
          <span aria-hidden="true" className={`size-2.5 rounded-sm border border-black/10 ${STATES[state].swatch}`} />
          {STATES[state].label}
        </li>
      ))}
    </ul>
  );
}
