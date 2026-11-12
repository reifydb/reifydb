// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactNode } from "react";

export interface ContainerProps {
  width?: "wide" | "narrow";
  className?: string;
  children: ReactNode;
}

export function Container({ width = "wide", className = "", children }: ContainerProps) {
  return (
    <div className={`mx-auto w-full px-4 sm:px-6 lg:px-8 ${width === "wide" ? "max-w-7xl" : "max-w-3xl"} ${className}`}>
      {children}
    </div>
  );
}
