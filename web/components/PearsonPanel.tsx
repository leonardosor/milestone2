'use client';

import clsx from 'clsx';

interface PearsonPanelProps {
  pearsonR: number | null | undefined;
  label?: string;
  schoolCount?: number;
  noSelection?: boolean;
  loading?: boolean;
}

export default function PearsonPanel({
  pearsonR,
  label,
  schoolCount,
  noSelection,
  loading,
}: PearsonPanelProps) {
  if (loading) {
    return <div className="h-44 rounded-lg bg-[var(--surface)] animate-pulse" />;
  }

  const rDisplay = pearsonR != null ? pearsonR.toFixed(3) : '—';
  const rColor =
    pearsonR != null && pearsonR > 0.3 ? 'text-green-400' : 'text-[var(--muted)]';

  return (
    <div className="h-44 rounded-lg bg-[var(--surface)] border border-[var(--border)] p-4 flex flex-col justify-between">
      <div className="flex flex-col gap-0.5">
        {noSelection ? (
          <span className="text-sm text-[var(--muted)]">
            Select a state to see Pearson&nbsp;r
          </span>
        ) : (
          <>
            <span className={clsx('text-2xl font-bold font-mono', rColor)}>
              {rDisplay}
            </span>
            {pearsonR === null && (
              <span className="text-xs text-[var(--muted)]">
                (fewer than 3 schools)
              </span>
            )}
            {label && (
              <span className="text-xs text-[var(--muted)] truncate">{label}</span>
            )}
            {schoolCount != null && (
              <span className="text-xs text-[var(--muted)]">
                n&nbsp;=&nbsp;{schoolCount.toLocaleString()} schools
              </span>
            )}
          </>
        )}
      </div>
      <p className="text-xs text-[var(--muted)] leading-snug border-t border-[var(--border)] pt-2">
        Statistical association, not causation. Many factors shape educational
        outcomes.
      </p>
    </div>
  );
}
