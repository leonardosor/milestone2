'use client';

import { useMemo } from 'react';
import {
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ReferenceLine,
} from 'recharts';

interface DataPoint {
  x: number;
  y: number;
  name: string;
}

interface ScatterPlotProps {
  data: DataPoint[];
  xLabel?: string;
  yLabel?: string;
  loading?: boolean;
  /** Called with the point's name when a dot is clicked (drill-through). */
  onPointClick?: (name: string) => void;
}

function CustomTooltip({ active, payload, xLabel, yLabel }: any) {
  if (!active || !payload?.length) return null;
  const d = payload[0]?.payload as DataPoint;
  if (!d) return null;
  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded px-2.5 py-1.5 text-sm space-y-0.5 max-w-[220px]">
      <div className="font-semibold truncate">{d.name}</div>
      <div className="text-[var(--muted)]">{xLabel ?? 'Income %'}: {d.x.toFixed(1)}</div>
      <div className="text-[var(--muted)]">{yLabel ?? 'Math %'}: {d.y.toFixed(1)}</div>
    </div>
  );
}

/**
 * Least-squares regression segment, clamped to the data's bounding box so the
 * line never shoots outside the plotted area.
 */
function computeTrendSegment(
  data: DataPoint[]
): [{ x: number; y: number }, { x: number; y: number }] | null {
  if (data.length < 3) return null;
  const n = data.length;
  const meanX = data.reduce((s, d) => s + d.x, 0) / n;
  const meanY = data.reduce((s, d) => s + d.y, 0) / n;
  let sxx = 0;
  let sxy = 0;
  for (const d of data) {
    sxx += (d.x - meanX) ** 2;
    sxy += (d.x - meanX) * (d.y - meanY);
  }
  if (sxx === 0) return null;
  const slope = sxy / sxx;
  const intercept = meanY - slope * meanX;

  const xMin = Math.min(...data.map(d => d.x));
  const xMax = Math.max(...data.map(d => d.x));
  const yMin = Math.min(...data.map(d => d.y));
  const yMax = Math.max(...data.map(d => d.y));

  // Clamp the segment to the y-range of the data
  const yAt = (x: number) => slope * x + intercept;
  let x0 = xMin;
  let x1 = xMax;
  if (slope !== 0) {
    const xAtYMin = (yMin - intercept) / slope;
    const xAtYMax = (yMax - intercept) / slope;
    const lo = Math.min(xAtYMin, xAtYMax);
    const hi = Math.max(xAtYMin, xAtYMax);
    x0 = Math.max(x0, lo);
    x1 = Math.min(x1, hi);
    if (x0 >= x1) return null;
  }
  return [
    { x: x0, y: yAt(x0) },
    { x: x1, y: yAt(x1) },
  ];
}

export default function ScatterPlot({
  data,
  xLabel,
  yLabel,
  loading,
  onPointClick,
}: ScatterPlotProps) {
  const trend = useMemo(() => computeTrendSegment(data), [data]);

  if (loading) {
    return <div className="h-72 rounded-lg bg-[var(--surface)] animate-pulse" />;
  }

  if (data.length === 0) {
    return (
      <div className="h-72 rounded-lg bg-[var(--surface)] border border-[var(--border)] flex items-center justify-center text-[var(--muted)] text-sm">
        No data
      </div>
    );
  }

  return (
    <div style={{ cursor: onPointClick ? 'pointer' : 'crosshair' }}>
      <ResponsiveContainer width="100%" height={280}>
        <ScatterChart margin={{ top: 10, right: 10, bottom: 32, left: 32 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
          <XAxis
            dataKey="x"
            name={xLabel ?? 'Income %'}
            type="number"
            domain={['auto', 'auto']}
            tick={{ fill: 'var(--muted)', fontSize: 13 }}
            label={{
              value: xLabel ?? 'Income %',
              position: 'insideBottom',
              offset: -14,
              fill: 'var(--muted)',
              fontSize: 13,
            }}
          />
          <YAxis
            dataKey="y"
            name={yLabel ?? 'Math %'}
            type="number"
            domain={['auto', 'auto']}
            tick={{ fill: 'var(--muted)', fontSize: 13 }}
            label={{
              value: yLabel ?? 'Math %',
              angle: -90,
              position: 'insideLeft',
              offset: 4,
              fill: 'var(--muted)',
              fontSize: 13,
            }}
          />
          <Tooltip cursor={false} content={<CustomTooltip xLabel={xLabel} yLabel={yLabel} />} />
          {trend && (
            <ReferenceLine
              segment={trend}
              stroke="var(--accent)"
              strokeWidth={2}
              strokeDasharray="6 4"
              ifOverflow="hidden"
            />
          )}
          <Scatter
            data={data}
            fill="#6a00a8"
            opacity={0.6}
            r={5}
            onClick={(pt: any) => {
              const name = pt?.payload?.name ?? pt?.name;
              if (name && onPointClick) onPointClick(name);
            }}
          />
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}
