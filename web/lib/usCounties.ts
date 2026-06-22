import * as topojson from 'topojson-client';
import type * as GeoJSON from 'geojson';
import { FIPS_TO_ABBR } from './usStates';

export const ABBR_TO_FIPS: Record<string, string> = Object.fromEntries(
  Object.entries(FIPS_TO_ABBR).map(([fips, abbr]) => [abbr, fips])
);

export interface CountyFeatureProps {
  name: string;
  fips: string;
  state: string;
  avg_math_pct_prof: number | null;
  avg_pct_high_income: number | null;
  pearson_r: number | null;
  school_count: number | null;
}

export interface CountyData {
  county_fips: string;
  state: string;
  avg_math_pct_prof: number;
  avg_pct_high_income: number;
  pearson_r?: number | null;
  school_count: number;
}

const COUNTIES_URL = 'https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json';

// Patch 2: module-level cache so fetchCountyFeatures + loadCountyNames share one fetch
let cachedTopo: any = null;
async function getCountyTopo() {
  if (!cachedTopo) {
    const resp = await fetch(COUNTIES_URL);
    if (!resp.ok) throw new Error(`Failed to load county atlas: ${resp.status}`);
    cachedTopo = await resp.json();
  }
  return cachedTopo;
}

export async function fetchCountyFeatures(
  stateAbbr: string,
  countyData: CountyData[]
): Promise<GeoJSON.FeatureCollection<GeoJSON.Geometry, CountyFeatureProps>> {
  const stateFips = ABBR_TO_FIPS[stateAbbr];
  if (!stateFips) {
    return { type: 'FeatureCollection', features: [] } as GeoJSON.FeatureCollection<GeoJSON.Geometry, CountyFeatureProps>;
  }

  const topo = await getCountyTopo();
  const fc = topojson.feature(topo, topo.objects.counties) as unknown as GeoJSON.FeatureCollection;

  const statsMap = new Map(countyData.map(c => [c.county_fips, c]));

  // Patch 3: skip features with no id to avoid "undefined" FIPS
  const filtered = fc.features.filter(f =>
    f.id != null && String(f.id).padStart(5, '0').slice(0, 2) === stateFips
  );

  return {
    type: 'FeatureCollection',
    features: filtered.map(f => {
      const fips = String(f.id).padStart(5, '0');
      const stats = statsMap.get(fips);
      return {
        ...f,
        properties: {
          name: (f.properties as any)?.name ?? fips,
          fips,
          state: stateAbbr,
          avg_math_pct_prof: stats?.avg_math_pct_prof ?? null,
          avg_pct_high_income: stats?.avg_pct_high_income ?? null,
          pearson_r: stats?.pearson_r ?? null,
          school_count: stats?.school_count ?? null,
        },
      };
    }),
  };
}

export async function loadCountyNames(stateAbbr: string): Promise<Map<string, string>> {
  const stateFips = ABBR_TO_FIPS[stateAbbr];
  if (!stateFips) return new Map();
  const topo = await getCountyTopo();
  const fc = topojson.feature(topo, topo.objects.counties) as unknown as GeoJSON.FeatureCollection;
  const map = new Map<string, string>();
  for (const f of fc.features) {
    // Patch 3: skip features with no id
    if (f.id == null) continue;
    const fips = String(f.id).padStart(5, '0');
    if (fips.slice(0, 2) === stateFips) {
      map.set(fips, (f.properties as any)?.name ?? fips);
    }
  }
  return map;
}

export function getStateBounds(
  features: GeoJSON.Feature[]
): [[number, number], [number, number]] {
  // Patch 1: guard against empty features — return continental US fallback
  if (features.length === 0) return [[-125, 24], [-66, 50]];

  let minLng = Infinity, minLat = Infinity, maxLng = -Infinity, maxLat = -Infinity;

  function processCoords(coords: any) {
    if (typeof coords[0] === 'number') {
      minLng = Math.min(minLng, coords[0]);
      maxLng = Math.max(maxLng, coords[0]);
      minLat = Math.min(minLat, coords[1]);
      maxLat = Math.max(maxLat, coords[1]);
    } else {
      coords.forEach(processCoords);
    }
  }

  features.forEach(f => {
    if (f.geometry) processCoords((f.geometry as any).coordinates);
  });

  return [[minLng, minLat], [maxLng, maxLat]];
}
