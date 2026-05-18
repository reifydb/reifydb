// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

export interface Tab {
  value: string;
  label: string;
}

export interface TabsProps {
  tabs: Tab[];
  value: string;
  onChange: (value: string) => void;
  className?: string;
}

export function Tabs({ tabs, value, onChange, className = "" }: TabsProps) {
  return (
    <div className={`flex gap-1 rounded-[var(--radius-md)] border border-border-default bg-bg-secondary p-1 ${className}`}>
      {tabs.map((tab) => (
        <button
          key={tab.value}
          onClick={() => onChange(tab.value)}
          className={`px-3 py-1.5 text-sm font-medium transition-all rounded-md
            ${
              value === tab.value
                ? "bg-bg-tertiary text-text-primary"
                : "text-text-muted hover:bg-bg-secondary hover:text-text-primary"
            }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
