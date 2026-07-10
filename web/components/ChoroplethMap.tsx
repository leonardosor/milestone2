'use client';

import { useCallback, useEffect, useState } from 'react';
import Map, { Source, Layer } from 'react-map-gl/maplibre';
import type { LayerProps, MapMouseEvent } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import type * as GeoJSON from 'geojson';
import {
  fetchUSStateFeatures,
  type StateFeatureProps,
  type StateData,
} from '@/lib/usStates';

const statesFillLayer: LayerProps = {
  id: 'states-fill',
  type: 'fill',
  paint: {
    'fill-color': [
      'step',
      ['coalesce', ['get', 'avg_math_pct_prof'], -1],
      '#dde2ea',
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

const statesBorderLayer: LayerProps = {
  id: 'states-border',
  type: 'line',
  paint: {
    'line-color': '#ffffff',
    'line-width': 0.8,
  },
};

const LEGEND = [
  { color: '#dde2ea', label: 'No data' },
  { color: '#0d0887', label: '< 25%' },
  { color: '#6a00a8', label: '25–35%' },
  { color: '#b12a90', label: '35–45%' },
  { color: '#e16462', label: '45–55%' },
  { color: '#fca636', label: '55–65%' },
  { color: '#f0f921', label: '≥ 65%' },
];

interface HoverInfo extends StateFeatureProps {
  x: number;
  y: number;
}

interface ChoroplethMapProps {
  stateStats: StateData[];
  onStateSelect: (abbr: string) => void;
}

export default function ChoroplethMap({ stateStats, onStateSelect }: ChoroplethMapProps) {
  const [geojson, setGeojson] = useState<GeoJSON.FeatureCollection<GeoJSON.Geometry, StateFeatureProps> | null>(null);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);

  useEffect(() => {
    // Cancellation guard: if stateStats changes (or we unmount) before the
    // fetch resolves, ignore the stale result so it can't clobber newer state.
    let cancelled = false;
    setFetchError(null);
    fetchUSStateFeatures(stateStats)
      .then(fc => { if (!cancelled) setGeojson(fc); })
      .catch((err: Error) => { if (!cancelled) setFetchError(err.message); });
    return () => { cancelled = true; };
  }, [stateStats]);

  const onMouseMove = useCallback((e: MapMouseEvent) => {
    const feature = e.features?.[0];
    if (!feature) { setHoverInfo(null); return; }
    const p = feature.properties as StateFeatureProps;
    setHoverInfo({ x: e.point.x, y: e.point.y, ...p });
  }, []);

  const onMouseLeave = useCallback(() => setHoverInfo(null), []);

  const onClick = useCallback(
    (e: MapMouseEvent) => {
      const feature = e.features?.[0];
      if (!feature) return;
      const abbr = (feature.properties as StateFeatureProps).abbr;
      if (abbr) onStateSelect(abbr);
    },
    [onStateSelect]
  );

  if (fetchError) {
    return (
      <div className="w-full h-full rounded-lg bg-[var(--surface)] flex items-center justify-center text-red-400 text-sm">
        Failed to load map: {fetchError}
      </div>
    );
  }

  if (!geojson) {
    return (
      <div className="lg:col-span-2 h-96 rounded-lg bg-[var(--surface)] animate-pulse flex items-center justify-center text-[var(--muted)] text-sm">
        Loading map…
      </div>
    );
  }

  return (
    <div className="relative w-full h-full">
      <Map
        initialViewState={{ latitude: 38, longitude: -96, zoom: 3 }}
        mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
        interactiveLayerIds={['states-fill']}
        onMouseMove={onMouseMove}
        onMouseLeave={onMouseLeave}
        onClick={onClick}
        style={{ width: '100%', height: '100%' }}
        attributionControl={false}
      >
        <Source id="states" type="geojson" data={geojson}>
          <Layer {...statesFillLayer} />
          <Layer {...statesBorderLayer} />
        </Source>
      </Map>

      {/* Tooltip */}
      {hoverInfo && (
        <div
          className="pointer-events-none absolute z-10 bg-[var(--surface)] border border-[var(--border)] rounded px-3 py-2 text-sm space-y-0.5"
          style={{ left: hoverInfo.x + 10, top: hoverInfo.y + 10 }}
        >
          <div className="font-semibold">{hoverInfo.name}</div>
          <div className="text-[var(--muted)]">
            Math Prof.: {hoverInfo.avg_math_pct_prof != null ? `${hoverInfo.avg_math_pct_prof.toFixed(1)}%` : '—'}
          </div>
          <div className="text-[var(--muted)]">
            r = {hoverInfo.pearson_r != null ? hoverInfo.pearson_r.toFixed(3) : '—'}
          </div>
          <div className="text-[var(--muted)]">
            Schools: {hoverInfo.school_count != null ? hoverInfo.school_count.toLocaleString() : '—'}
          </div>
        </div>
      )}

      {/* Legend */}
      <div className="absolute bottom-4 left-4 z-10 bg-[var(--surface)] border border-[var(--border)] rounded px-3 py-2 space-y-1">
        <div className="text-xs font-semibold text-[var(--text)] mb-1">Gr. 8 Math Proficiency</div>
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
