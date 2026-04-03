export interface Tab {
  value: string;
  label: string;
}

export interface TabsProps {
  tabs: Tab[];
  value: string;
  onChange: (value: string) => void;
  className?: string;
}

export function Tabs({ tabs, value, onChange, className = "" }: TabsProps) {
  return (
    <div className={`flex gap-1 rounded-[var(--radius-lg)] border border-white/[0.08] bg-white/[0.04] p-1 ${className}`}>
      {tabs.map((tab) => (
        <button
          key={tab.value}
          onClick={() => onChange(tab.value)}
          className={`px-3 py-1.5 text-sm font-medium transition-all rounded-md
            ${
              value === tab.value
                ? "bg-white/[0.08] text-text-primary"
                : "text-text-muted hover:bg-white/[0.04] hover:text-text-primary"
            }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
