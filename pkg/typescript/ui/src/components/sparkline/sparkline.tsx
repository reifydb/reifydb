export interface SparklineProps {
  trend: number[];
  className?: string;
}

export function Sparkline({ trend, className = "" }: SparklineProps) {
  const max = Math.max(...trend);
  return (
    <div className={`flex items-end gap-px h-4 ${className}`}>
      {trend.map((v, i) => {
        const h = Math.max(2, Math.round((v / max) * 14));
        const col = v >= 10 ? "bg-status-error" : v >= 7 ? "bg-status-warning" : "bg-text-muted";
        return <div key={i} className={`w-[3px] rounded-full ${col}`} style={{ height: `${h}px` }} />;
      })}
    </div>
  );
}
