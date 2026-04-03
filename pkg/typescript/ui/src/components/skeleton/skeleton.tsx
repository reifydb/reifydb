export interface SkeletonProps {
  className?: string;
}

export function Skeleton({ className = "" }: SkeletonProps) {
  return (
    <div
      className={`rounded-lg bg-white/[0.06] animate-pulse ${className}`}
    />
  );
}
