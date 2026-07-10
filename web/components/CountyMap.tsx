'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Map, { Source, Layer } from 'react-map-gl/maplibre';
import type { MapRef, LayerProps, MapMouseEvent } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import type * as GeoJSON from 'geojson';
import {
  fetchCountyFeatures,
  getStateBounds,
  type CountyFeatureProps,
  type CountyData,
} from '@/lib/usCounties';

const countiesBorderLayer: LayerProps = {
  id: 'counties-border',
  type: 'line',
  paint: {
    'line-color': '#ffffff',
    'line-width': 0.5,
  },
};

/** Shared plasma step scale (matches the choropleth + legend). */
const MATH_COLOR_STEPS = [
  0,  '#0d0887',
  25, '#6a00a8',
  35, '#b12a90',
  45, '#e16462',
  55, '#fca636',
  65, '#f0f921',
] as const;

const LEGEND = [
  { color: '#dde2ea', label: 'No data' },
  { color: '#0d0887', label: '< 25%' },
  { color: '#6a00a8', label: '25–35%' },
  { color: '#b12a90', label: '35–45%' },
  { color: '#e16462', label: '45–55%' },
  { color: '#fca636', label: '55–65%' },
  { color: '#f0f921', label: '≥ 65%' },
];

/** Minimal school shape needed for map markers. */
export interface SchoolPoint {
  school_name: string;
  lat?: number | null;
  lon?: number | null;
  math_pct_prof?: number | null;
  pct_high_income?: number | null;
  enrollment?: number | null;
}

type HoverInfo =
  | ({ kind: 'county'; x: number; y: number } & CountyFeatureProps)
  | {
      kind: 'school';
      x: number;
      y: number;
      name: string;
      math?: number | null;
      income?: number | null;
      enrollment?: number | null;
    };

interface CountyMapProps {
  stateAbbr: string;
  countyStats: CountyData[];
  onBack: () => void;
  onCountySelect?: (fips: string) => void;
  /** Currently drilled-in county (5-digit FIPS); enables school markers. */
  selectedCounty?: string | null;
  /** Schools to plot when a county is selected. */
  schools?: SchoolPoint[];
  /** Called with the school name when a marker is clicked. */
  onSchoolSelect?: (name: string) => void;
}

/** Bounding box of a single Polygon/MultiPolygon feature. */
function featureBounds(
  geometry: GeoJSON.Geometry
): [[number, number], [number, number]] | null {
  let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity;
  const visit = (coords: any) => {
    if (typeof coords[0] === 'number') {
      const [lon, lat] = coords as [number, number];
      if (lon < minLon) minLon = lon;
      if (lat < minLat) minLat = lat;
      if (lon > maxLon) maxLon = lon;
      if (lat > maxLat) maxLat = lat;
    } else {
      for (const c of coords) visit(c);
    }
  };
  if (geometry.type === 'Polygon' || geometry.type === 'MultiPolygon') {
    visit(geometry.coordinates);
  } else {
    return null;
  }
  if (!isFinite(minLon)) return null;
  return [[minLon, minLat], [maxLon, maxLat]];
}

export default function CountyMap({
  stateAbbr,
  countyStats,
  onBack,
  onCountySelect,
  selectedCounty,
  schools,
  onSchoolSelect,
}: CountyMapProps) {
  const mapRef = useRef<MapRef>(null);
  const [geojson, setGeojson] = useState<GeoJSON.FeatureCollection<GeoJSON.Geometry, CountyFeatureProps> | null>(null);
  const [bounds, setBounds] = useState<[[number, number], [number, number]] | null>(null);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);

  useEffect(() => {
    // Cancellation guard: rapid state switches can resolve out of order;
    // ignore stale results so an older state's counties can't overwrite
    // the currently selected state's map.
    let cancelled = false;
    setFetchError(null);
    setGeojson(null);
    fetchCountyFeatures(stateAbbr, countyStats)
      .then(fc => {
        if (cancelled) return;
        setGeojson(fc);
        setBounds(getStateBounds(fc.features));
      })
      .catch((err: Error) => { if (!cancelled) setFetchError(err.message); });
    return () => { cancelled = true; };
  }, [stateAbbr, countyStats]);

  // Zoom into the selected county; zoom back out on deselect
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !geojson) return;
    if (selectedCounty) {
      const feature = geojson.features.find(f => f.properties.fips === selectedCounty);
      const b = feature ? featureBounds(feature.geometry) : null;
      if (b) map.fitBounds(b, { padding: 60, duration: 700 });
    } else if (bounds) {
      map.fitBounds(bounds, { padding: 40, duration: 700 });
    }
  }, [selectedCounty, geojson, bounds]);

  // County fill: dim non-selected counties when drilled in so schools stand out
  const countiesFillLayer: LayerProps = useMemo(() => ({
    id: 'counties-fill',
    type: 'fill',
    paint: {
      'fill-color': [
        'step',
        ['coalesce', ['get', 'avg_math_pct_prof'], -1],
        '#dde2ea',
        ...MATH_COLOR_STEPS,
      ] as any,
      'fill-opacity': selectedCounty
        ? (['case', ['==', ['get', 'fips'], selectedCounty], 0.35, 0.12] as any)
        : 0.75,
    },
  }), [selectedCounty]);

  // Accent outline on the drilled-in county
  const selectedBorderLayer: LayerProps | null = useMemo(() => {
    if (!selectedCounty) return null;
    return {
      id: 'selected-county-border',
      type: 'line',
      filter: ['==', ['get', 'fips'], selectedCounty] as any,
      paint: {
        'line-color': '#2563eb',
        'line-width': 2,
      },
    };
  }, [selectedCounty]);

  // School markers (only when drilled into a county)
  const schoolsGeojson = useMemo(() => {
    if (!selectedCounty || !schools?.length) return null;
    const features = schools
      .filter(s => s.lat != null && s.lon != null)
      .map(s => ({
        type: 'Feature' as const,
        geometry: { type: 'Point' as const, coordinates: [s.lon!, s.lat!] },
        properties: {
          name: s.school_name,
          math: s.math_pct_prof ?? null,
          income: s.pct_high_income ?? null,
          enrollment: s.enrollment ?? null,
        },
      }));
    if (features.length === 0) return null;
    return { type: 'FeatureCollection' as const, features };
  }, [selectedCounty, schools]);

  const schoolsCircleLayer: LayerProps = {
    id: 'schools-circle',
    type: 'circle',
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 4, 10, 7, 13, 10] as any,
      'circle-color': [
        'step',
        ['coalesce', ['get', 'math'], -1],
        '#9aa3b5',
        ...MATH_COLOR_STEPS,
      ] as any,
      'circle-stroke-color': '#ffffff',
      'circle-stroke-width': 1.5,
      'circle-opacity': 0.95,
    },
  };

  const onMouseMove = useCallback((e: MapMouseEvent) => {
    const feature = e.features?.[0];
    if (!feature) { setHoverInfo(null); return; }
    if (feature.layer?.id === 'schools-circle') {
      const p = feature.properties as any;
      setHoverInfo({
        kind: 'school',
        x: e.point.x,
        y: e.point.y,
        name: p.name,
        math: p.math ?? null,
        income: p.income ?? null,
        enrollment: p.enrollment ?? null,
      });
      return;
    }
    const p = feature.properties as CountyFeatureProps;
    setHoverInfo({ kind: 'county', x: e.point.x, y: e.point.y, ...p });
  }, []);

  const onMouseLeave = useCallback(() => setHoverInfo(null), []);

  const onClick = useCallback((e: MapMouseEvent) => {
    const feature = e.features?.[0];
    if (!feature) return;
    if (feature.layer?.id === 'schools-circle') {
      const name = (feature.properties as any)?.name;
      if (name && onSchoolSelect) onSchoolSelect(name);
      return;
    }
    if (!onCountySelect) return;
    const fips = (feature.properties as CountyFeatureProps).fips;
    if (fips) onCountySelect(fips);
  }, [onCountySelect, onSchoolSelect]);

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
        ref={mapRef}
        initialViewState={{
          bounds,
          fitBoundsOptions: { padding: 40 },
        }}
        mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
        interactiveLayerIds={schoolsGeojson ? ['schools-circle', 'counties-fill'] : ['counties-fill']}
        onMouseMove={onMouseMove}
        onMouseLeave={onMouseLeave}
        onClick={onClick}
        style={{ width: '100%', height: '100%' }}
        attributionControl={false}
      >
        <Source id="counties" type="geojson" data={geojson}>
          <Layer {...countiesFillLayer} />
          <Layer {...countiesBorderLayer} />
          {selectedBorderLayer && <Layer {...selectedBorderLayer} />}
        </Source>
        {schoolsGeojson && (
          <Source id="schools" type="geojson" data={schoolsGeojson}>
            <Layer {...schoolsCircleLayer} />
          </Source>
        )}
      </Map>

      {/* Back button */}
      <button
        onClick={onBack}
        className="absolute top-3 left-3 z-10 bg-[var(--surface)] border border-[var(--border)] text-sm px-3 py-1.5 rounded shadow-sm hover:bg-[var(--row-hover)] transition-colors"
      >
        ← Back
      </button>

      {/* Tooltip */}
      {hoverInfo && (
        <div
          className="pointer-events-none absolute z-10 bg-[var(--surface)] border border-[var(--border)] rounded px-3 py-2 text-sm space-y-0.5 shadow-md"
          style={{ left: hoverInfo.x + 10, top: hoverInfo.y + 10 }}
        >
          {hoverInfo.kind === 'school' ? (
            <>
              <div className="font-semibold max-w-[240px]">{hoverInfo.name}</div>
              <div className="text-[var(--muted)]">
                Math Prof.:{' '}
                {hoverInfo.math != null ? `${Number(hoverInfo.math).toFixed(1)}%` : '—'}
              </div>
              <div className="text-[var(--muted)]">
                High Income:{' '}
                {hoverInfo.income != null ? `${Number(hoverInfo.income).toFixed(1)}%` : '—'}
              </div>
              <div className="text-[var(--muted)]">
                Enrollment:{' '}
                {hoverInfo.enrollment != null
                  ? Number(hoverInfo.enrollment).toLocaleString()
                  : '—'}
              </div>
            </>
          ) : (
            <>
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
            </>
          )}
        </div>
      )}

      {/* Legend */}
      <div className="absolute bottom-4 left-4 z-10 bg-[var(--surface)] border border-[var(--border)] rounded px-3 py-2 space-y-1">
        <div className="text-xs font-semibold text-[var(--text)] mb-1">
          {selectedCounty ? 'Schools · Gr. 8 Math Proficiency' : 'Gr. 8 Math Proficiency'}
        </div>
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
