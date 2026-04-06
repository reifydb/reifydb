// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState, useCallback } from "react";

export interface CopyButtonProps {
  text: string;
  className?: string;
  label?: string;
  onCopy?: () => void;
}

export function CopyButton({ text, className = "", label, onCopy }: CopyButtonProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
    onCopy?.();
  }, [text, onCopy]);

  return (
    <button
      onClick={handleCopy}
      className={`inline-flex items-center gap-1.5 px-2 py-1 text-xs text-text-muted
        rounded-sm border border-border-default transition-colors duration-75 hover:bg-bg-tertiary hover:text-text-primary ${className}`}
    >
      {copied ? (
        <>
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
            <path d="M3 8.5l3 3 7-7" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          {label ? "Copied!" : null}
        </>
      ) : (
        <>
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
            <rect x="5" y="5" width="8" height="8" rx="1.5" stroke="currentColor" strokeWidth="1.5" />
            <path d="M3 11V3.5A.5.5 0 013.5 3H11" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
          {label ?? null}
        </>
      )}
    </button>
  );
}
