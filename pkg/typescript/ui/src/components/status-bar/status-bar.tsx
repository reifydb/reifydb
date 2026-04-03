import type { ReactNode } from "react";

type StatusDot = "success" | "warning" | "danger" | "idle";

export interface StatusBarItem {
  dot?: StatusDot;
  label: string;
}

export interface StatusBarProps {
  items: StatusBarItem[];
  trailing?: ReactNode;
  className?: string;
}

const dotColors: Record<StatusDot, string> = {
  success: "bg-status-success",
  warning: "bg-status-warning",
  danger: "bg-status-error",
  idle: "bg-text-muted",
};

export function StatusBar({ items, trailing, className = "" }: StatusBarProps) {
  return (
    <div className={`flex h-7 shrink-0 items-center gap-4 border-t border-white/[0.06] bg-white/[0.04] px-4 ${className}`}>
      {items.map((item, i) => (
        <div key={i} className="flex items-center gap-1.5 text-xs text-text-muted">
          {item.dot && (
            <span className={`inline-block h-1.5 w-1.5 rounded-full ${dotColors[item.dot]}`} />
          )}
          {item.label}
        </div>
      ))}
      {trailing && <div className="ml-auto text-xs text-text-muted">{trailing}</div>}
    </div>
  );
}
