'use client';

import { useCallback, useEffect, useState } from 'react';
import Map, { Source, Layer } from 'react-map-gl/maplibre';
import type { LayerProps, MapMouseEvent } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import type * as GeoJSON from 'geojson';
import {
  fetchCountyFeatures,
  getStateBounds,
  type CountyFeatureProps,
  type CountyData,
} from '@/lib/usCounties';

const countiesFillLayer: LayerProps = {
  id: 'counties-fill',
  type: 'fill',
  paint: {
    'fill-color': [
      'step',
      ['coalesce', ['get', 'avg_math_pct_prof'], -1],
      '#2a2d3a',
      0,  '#0d0887',
      25, '#6a00a8',
      35, '#b12a90',
      45, '#e16462',
      55, '#fca636',
      65, '#f0f921',
    ] as any,
    'fill-opacity': 0.75,
  },
};

const countiesBorderLayer: LayerProps = {
  id: 'counties-border',
  type: 'line',
  paint: {
    'line-color': '#0f1117',
    'line-width': 0.5,
  },
};

const LEGEND = [
  { color: '#2a2d3a', label: 'No data' },
  { color: '#0d0887', label: '< 25%' },
  { color: '#6a00a8', label: '25–35%' },
  { color: '#b12a90', label: '35–45%' },
  { color: '#e16462', label: '45–55%' },
  { color: '#fca636', label: '55–65%' },
  { color: '#f0f921', label: '≥ 65%' },
];

interface HoverInfo extends CountyFeatureProps {
  x: number;
  y: number;
}

interface CountyMapProps {
  stateAbbr: string;
  countyStats: CountyData[];
  onBack: () => void;
  onCountySelect?: (fips: string) => void;
}

export default function CountyMap({ stateAbbr, countyStats, onBack, onCountySelect }: CountyMapProps) {
  const [geojson, setGeojson] = useState<GeoJSON.FeatureCollection<GeoJSON.Geometry, CountyFeatureProps> | null>(null);
  const [bounds, setBounds] = useState<[[number, number], [number, number]] | null>(null);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);

  useEffect(() => {
    setFetchError(null);
    setGeojson(null);
    fetchCountyFeatures(stateAbbr, countyStats)
      .then(fc => {
        setGeojson(fc);
        setBounds(getStateBounds(fc.features));
      })
      .catch((err: Error) => setFetchError(err.message));
  }, [stateAbbr, countyStats]);

  const onMouseMove = useCallback((e: MapMouseEvent) => {
    const feature = e.features?.[0];
    if (!feature) { setHoverInfo(null); return; }
    const p = feature.properties as CountyFeatureProps;
    setHoverInfo({ x: e.point.x, y: e.point.y, ...p });
  }, []);

  const onMouseLeave = useCallback(() => setHoverInfo(null), []);

  const onClick = useCallback((e: MapMouseEvent) => {
    const feature = e.features?.[0];
    if (!feature || !onCountySelect) return;
    const fips = (feature.properties as CountyFeatureProps).fips;
    if (fips) onCountySelect(fips);
  }, [onCountySelect]);

  if (fetchError) {
    return (
      <div className="w-full h-full rounded-lg bg-[var(--surface)] flex items-center justify-center text-red-400 text-sm">
        Failed to load map: {fetchError}
      </div>
    );
  }

  if (!geojson || !bounds) {
    return (
      <div className="w-full h-full rounded-lg bg-[var(--surface)] animate-pulse flex items-center justify-center text-[var(--muted)] text-sm">
        Loading counties…
      </div>
    );
  }

  return (
    <div className="relative w-full h-full">
      <Map
        initialViewState={{
          bounds,
          fitBoundsOptions: { padding: 40 },
        }}
        mapStyle="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
        interactiveLayerIds={['counties-fill']}
        onMouseMove={onMouseMove}
        onMouseLeave={onMouseLeave}
        onClick={onClick}
        style={{ width: '100%', height: '100%' }}
        attributionControl={false}
      >
        <Source id="counties" type="geojson" data={geojson}>
          <Layer {...countiesFillLayer} />
          <Layer {...countiesBorderLayer} />
        </Source>
      </Map>

      {/* Back button */}
      <button
        onClick={onBack}
        className="absolute top-3 left-3 z-10 bg-[var(--surface)] border border-[var(--border)] text-sm px-3 py-1.5 rounded hover:bg-[var(--accent)] transition-colors"
      >
        ← Back
      </button>

      {/* Tooltip */}
      {hoverInfo && (
        <div
          className="pointer-events-none absolute z-10 bg-[var(--surface)] border border-[var(--border)] rounded px-3 py-2 text-sm space-y-0.5"
          style={{ left: hoverInfo.x + 10, top: hoverInfo.y + 10 }}
        >
          <div className="font-semibold">{hoverInfo.name}</div>
          <div className="text-[var(--muted)] font-mono text-xs">{hoverInfo.fips}</div>
          <div className="text-[var(--muted)]">
            Math Prof.:{' '}
            {hoverInfo.avg_math_pct_prof != null
              ? `${hoverInfo.avg_math_pct_prof.toFixed(1)}%`
              : '—'}
          </div>
          <div className="text-[var(--muted)]">
            r ={' '}
            {hoverInfo.pearson_r != null ? hoverInfo.pearson_r.toFixed(3) : '—'}
          </div>
          <div className="text-[var(--muted)]">
            Schools:{' '}
            {hoverInfo.school_count != null
              ? hoverInfo.school_count.toLocaleString()
              : '—'}
          </div>
        </div>
      )}

      {/* Legend */}
      <div className="absolute bottom-4 left-4 z-10 bg-[var(--surface)] border border-[var(--border)] rounded px-3 py-2 space-y-1">
        <div className="text-xs font-semibold text-[var(--foreground)] mb-1">Gr. 8 Math Proficiency</div>
        {LEGEND.map(({ color, label }) => (
          <div key={label} className="flex items-center gap-2 text-xs text-[var(--muted)]">
            <span className="w-3 h-3 rounded-sm flex-shrink-0" style={{ backgroundColor: color }} />
            {label}
          </div>
        ))}
      </div>
    </div>
  );
}
