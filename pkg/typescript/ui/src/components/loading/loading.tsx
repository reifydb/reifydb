// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

export interface LoadingProps {
  text?: string;
  className?: string;
}

export function Loading({ text = "Loading", className = "" }: LoadingProps) {
  return (
    <div className={`flex items-center gap-1 text-text-muted text-sm font-mono ${className}`}>
      <span>{text}</span>
      <span className="flex gap-0.5">
        <span className="animate-pulse" style={{ animationDelay: "0ms" }}>.</span>
        <span className="animate-pulse" style={{ animationDelay: "200ms" }}>.</span>
        <span className="animate-pulse" style={{ animationDelay: "400ms" }}>.</span>
      </span>
    </div>
  );
}
