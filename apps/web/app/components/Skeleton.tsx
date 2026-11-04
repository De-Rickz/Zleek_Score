// app/components/Skeleton.tsx
// Placeholder block for loading.tsx. Pulses unless the user prefers reduced motion.
export default function Skeleton({ className = '' }: { className?: string }) {
  return <div aria-hidden className={`animate-pulse rounded bg-skeleton motion-reduce:animate-none ${className}`} />
}
