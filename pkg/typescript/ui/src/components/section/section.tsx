// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactNode } from "react";

export interface SectionProps {
  tone?: "plain" | "tint";
  id?: string;
  className?: string;
  children: ReactNode;
}

export function Section({ tone = "plain", id, className = "", children }: SectionProps) {
  return (
    <section
      id={id}
      className={`py-16 sm:py-24 border-t border-rule ${tone === "tint" ? "bg-bg-tint" : "bg-bg-primary"} ${className}`}
    >
      {children}
    </section>
  );
}
