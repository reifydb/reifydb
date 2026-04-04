// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

export interface ToggleProps {
  options: [string, string];
  value: string;
  onChange: (value: string) => void;
  className?: string;
}

export function Toggle({ options, value, onChange, className = "" }: ToggleProps) {
  const [left, right] = options;

  return (
    <div className={`inline-flex items-center gap-2 text-sm ${className}`}>
      <span
        className={`cursor-pointer transition-colors ${value === left ? "text-text-primary" : "text-text-muted"}`}
        onClick={() => onChange(left)}
      >
        {left}
      </span>
      <button
        onClick={() => onChange(value === left ? right : left)}
        className={`relative w-10 h-5 rounded-none transition-colors border-2 border-white/[0.08] ${
          value === right ? "bg-primary" : "bg-white/[0.04]"
        }`}
      >
        <span
          className={`absolute top-0.5 w-4 h-4 rounded-none transition-transform bg-text-primary ${
            value === right ? "translate-x-5" : "translate-x-0.5"
          }`}
        />
      </button>
      <span
        className={`cursor-pointer transition-colors ${value === right ? "text-text-primary" : "text-text-muted"}`}
        onClick={() => onChange(right)}
      >
        {right}
      </span>
    </div>
  );
}
