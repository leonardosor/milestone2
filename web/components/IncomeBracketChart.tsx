'use client';

import { useMemo } from 'react';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
} from 'recharts';

interface DataPoint {
  x: number; // pct_high_income
  y: number; // math_pct_prof
  name: string;
}

interface IncomeBracketChartProps {
  data: DataPoint[];
  loading?: boolean;
  /** What one data point represents at the current drill level. */
  unitLabel?: string; // e.g. "states" | "districts" | "schools"
}

/** Proficiency bands used for the stacks (grade-8 math % proficient). */
const BANDS = [
  { key: 'lt25', label: '< 25%', color: '#0d0887', test: (y: number) => y < 25 },
  { key: 'b2550', label: '25–50%', color: '#b12a90', test: (y: number) => y >= 25 && y < 50 },
  { key: 'b5075', label: '50–75%', color: '#fca636', test: (y: number) => y >= 50 && y < 75 },
  { key: 'gte75', label: '≥ 75%', color: '#f0f921', test: (y: number) => y >= 75 },
] as const;

interface BracketRow {
  bracket: string;
  n: number;
  lt25: number;
  b2550: number;
  b5075: number;
  gte75: number;
  counts: Record<string, number>;
}

/** p-th percentile (linear interpolation) of a pre-sorted array. */
function percentile(sorted: number[], p: number): number {
  const idx = (sorted.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

/**
 * Income brackets from the empirical distribution (PDF) of the income
 * variable: quintile cut points at the 20/40/60/80th percentiles, so each
 * bracket holds ~20% of the data. Duplicate cut points (heavily skewed
 * distributions) are merged.
 */
function buildRows(data: DataPoint[]): BracketRow[] {
  if (data.length < 5) return [];
  const xs = data.map(d => d.x).sort((a, b) => a - b);

  const cuts = [0.2, 0.4, 0.6, 0.8].map(p => percentile(xs, p));
  // Merge duplicate cut points so every bracket is non-empty
  const uniqueCuts = [...new Set(cuts.map(c => +c.toFixed(4)))];

  const edges = [xs[0], ...uniqueCuts, xs[xs.length - 1]];
  const rows: BracketRow[] = [];

  for (let i = 0; i < edges.length - 1; i++) {
    const lo = edges[i];
    const hi = edges[i + 1];
    const last = i === edges.length - 2;
    const members = data.filter(d =>
      last ? d.x >= lo && d.x <= hi : d.x >= lo && d.x < hi
    );
    if (members.length === 0) continue;

    const counts: Record<string, number> = {};
    for (const band of BANDS) {
      counts[band.key] = members.filter(d => band.test(d.y)).length;
    }
    const n = members.length;
    rows.push({
      bracket: `${lo.toFixed(0)}–${hi.toFixed(0)}%`,
      n,
      lt25: (counts.lt25 / n) * 100,
      b2550: (counts.b2550 / n) * 100,
      b5075: (counts.b5075 / n) * 100,
      gte75: (counts.gte75 / n) * 100,
      counts,
    });
  }
  return rows;
}

function CustomTooltip({ active, payload, label, unitLabel }: any) {
  if (!active || !payload?.length) return null;
  const row = payload[0]?.payload as BracketRow;
  if (!row) return null;
  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded px-3 py-2 text-sm space-y-0.5">
      <div className="font-semibold">Income {label}</div>
      <div className="text-[var(--muted)] text-xs pb-1">
        {row.n.toLocaleString()} {unitLabel ?? 'schools'} in bracket
      </div>
      {[...BANDS].reverse().map(band => (
        <div key={band.key} className="flex items-center gap-2 text-[var(--muted)]">
          <span
            className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
            style={{ backgroundColor: band.color }}
          />
          {band.label}: {(row[band.key as keyof BracketRow] as number).toFixed(1)}%
          {' '}({row.counts[band.key]})
        </div>
      ))}
    </div>
  );
}

export default function IncomeBracketChart({
  data,
  loading,
  unitLabel,
}: IncomeBracketChartProps) {
  const rows = useMemo(() => buildRows(data), [data]);

  if (loading) {
    return <div className="h-80 rounded-lg bg-[var(--surface)] animate-pulse" />;
  }

  if (rows.length === 0) {
    return (
      <div className="h-80 rounded-lg bg-[var(--surface)] border border-[var(--border)] flex items-center justify-center text-[var(--muted)] text-sm">
        Not enough data for income brackets
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4">
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={rows} margin={{ top: 10, right: 10, bottom: 28, left: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
          <XAxis
            dataKey="bracket"
            tick={{ fill: 'var(--muted)', fontSize: 13 }}
            label={{
              value: 'High-income share — quintile brackets',
              position: 'insideBottom',
              offset: -14,
              fill: 'var(--muted)',
              fontSize: 13,
            }}
          />
          <YAxis
            domain={[0, 100]}
            tickFormatter={(v: number) => `${v}%`}
            tick={{ fill: 'var(--muted)', fontSize: 13 }}
          />
          <Tooltip
            cursor={{ fill: 'rgba(0,0,0,0.04)' }}
            content={<CustomTooltip unitLabel={unitLabel} />}
          />
          <Legend
            verticalAlign="top"
            formatter={(value: string) => (
              <span style={{ color: 'var(--muted)', fontSize: 13 }}>{value}</span>
            )}
          />
          {BANDS.map(band => (
            <Bar
              key={band.key}
              dataKey={band.key}
              name={`Math ${band.label}`}
              stackId="prof"
              fill={band.color}
              fillOpacity={0.85}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
