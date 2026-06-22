'use client';

import {
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
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
}

function CustomTooltip({ active, payload }: any) {
  if (!active || !payload?.length) return null;
  const d = payload[0]?.payload as DataPoint;
  if (!d) return null;
  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded px-2 py-1 text-xs space-y-0.5 max-w-[180px]">
      <div className="font-semibold truncate">{d.name}</div>
      <div className="text-[var(--muted)]">Income: {d.x.toFixed(1)}%</div>
      <div className="text-[var(--muted)]">Math: {d.y.toFixed(1)}%</div>
    </div>
  );
}

export default function ScatterPlot({ data, xLabel, yLabel, loading }: ScatterPlotProps) {
  if (loading) {
    return <div className="h-44 rounded-lg bg-[var(--surface)] animate-pulse" />;
  }

  if (data.length === 0) {
    return (
      <div className="h-44 rounded-lg bg-[var(--surface)] border border-[var(--border)] flex items-center justify-center text-[var(--muted)] text-sm">
        No data
      </div>
    );
  }

  return (
    <div style={{ cursor: 'crosshair' }}>
      <ResponsiveContainer width="100%" height={176}>
        <ScatterChart margin={{ top: 8, right: 8, bottom: 28, left: 28 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
          <XAxis
            dataKey="x"
            name={xLabel ?? 'Income %'}
            type="number"
            domain={['auto', 'auto']}
            tick={{ fill: 'var(--muted)', fontSize: 10 }}
            label={{
              value: xLabel ?? 'Income %',
              position: 'insideBottom',
              offset: -12,
              fill: 'var(--muted)',
              fontSize: 10,
            }}
          />
          <YAxis
            dataKey="y"
            name={yLabel ?? 'Math %'}
            type="number"
            domain={['auto', 'auto']}
            tick={{ fill: 'var(--muted)', fontSize: 10 }}
            label={{
              value: yLabel ?? 'Math %',
              angle: -90,
              position: 'insideLeft',
              offset: 8,
              fill: 'var(--muted)',
              fontSize: 10,
            }}
          />
          <Tooltip cursor={false} content={<CustomTooltip />} />
          <Scatter data={data} fill="#6a00a8" opacity={0.6} r={4} />
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}
