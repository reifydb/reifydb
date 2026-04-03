import type { ReactNode } from "react";

export interface KbdProps {
  children: ReactNode;
  className?: string;
}

export function Kbd({ children, className = "" }: KbdProps) {
  return (
    <kbd
      className={`inline-flex h-5 min-w-5 items-center justify-center rounded border border-white/[0.08]
        bg-white/[0.06] px-1.5 font-mono text-[10px] font-medium text-text-muted ${className}`}
    >
      {children}
    </kbd>
  );
}
