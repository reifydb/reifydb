import type { ReactNode } from "react";

export interface SectionHeaderProps {
  title: string;
  badge?: ReactNode;
  className?: string;
}

export function SectionHeader({ title, badge, className = "" }: SectionHeaderProps) {
  return (
    <div className={`flex items-center justify-between bg-white/[0.06] px-3 py-2.5 rounded-t-xl ${className}`}>
      <span className="text-xs font-semibold text-text-secondary">{title}</span>
      {badge}
    </div>
  );
}
