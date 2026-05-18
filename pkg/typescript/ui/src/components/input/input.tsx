// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { type InputHTMLAttributes, forwardRef } from "react";

export interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  error?: string;
}

export const Input = forwardRef<HTMLInputElement, InputProps>(
  ({ label, error, className = "", id, ...props }, ref) => {
    const inputId = id || label?.toLowerCase().replace(/\s+/g, "-");
    return (
      <div className="flex flex-col gap-1.5">
        {label && (
          <label htmlFor={inputId} className="text-sm font-medium text-text-secondary">
            {label}
          </label>
        )}
        <input
          ref={ref}
          id={inputId}
          className={`h-9 rounded-[4px] border border-border-default bg-bg-secondary px-3 text-sm text-text-primary
            placeholder:text-text-muted
            focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary
            disabled:cursor-not-allowed disabled:opacity-50
            ${error ? "border-status-error" : ""} ${className}`}
          {...props}
        />
        {error && <p className="text-xs text-status-error">{error}</p>}
      </div>
    );
  },
);

Input.displayName = "Input";
