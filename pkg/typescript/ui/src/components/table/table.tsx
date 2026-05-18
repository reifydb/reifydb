// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ReactNode } from "react";

export interface TableProps {
  children: ReactNode;
  className?: string;
}

export function Table({ children, className = "" }: TableProps) {
  return (
    <div className="w-full overflow-x-auto rounded-[var(--radius-md)]">
      <table className={`w-full text-sm ${className}`}>{children}</table>
    </div>
  );
}

export function TableHead({ children }: { children: ReactNode }) {
  return (
    <thead className="border-b border-border-light">
      <tr>{children}</tr>
    </thead>
  );
}

export interface TableHeaderProps {
  children?: ReactNode;
  className?: string;
  onClick?: () => void;
}

export function TableHeader({ children, className = "", onClick }: TableHeaderProps) {
  return (
    <th
      className={`px-4 py-3 text-left text-xs font-medium text-text-muted ${onClick ? "cursor-pointer select-none hover:text-text-primary" : ""} ${className}`}
      onClick={onClick}
    >
      {children}
    </th>
  );
}

export function TableBody({ children }: { children: ReactNode }) {
  return <tbody className="divide-y divide-border-light">{children}</tbody>;
}

export interface TableRowProps {
  children: ReactNode;
  className?: string;
  onClick?: () => void;
}

export function TableRow({ children, className = "", onClick }: TableRowProps) {
  return (
    <tr className={`hover:bg-bg-secondary transition-colors ${onClick ? "cursor-pointer" : ""} ${className}`} onClick={onClick}>
      {children}
    </tr>
  );
}

export function TableCell({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <td className={`px-4 py-3.5 text-text-secondary ${className}`}>{children}</td>;
}
