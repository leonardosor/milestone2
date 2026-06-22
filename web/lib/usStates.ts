import * as topojson from 'topojson-client';
import type * as GeoJSON from 'geojson';

export const FIPS_TO_NAME: Record<string, string> = {
  "01":"Alabama","02":"Alaska","04":"Arizona","05":"Arkansas","06":"California",
  "08":"Colorado","09":"Connecticut","10":"Delaware","11":"District of Columbia","12":"Florida",
  "13":"Georgia","15":"Hawaii","16":"Idaho","17":"Illinois","18":"Indiana",
  "19":"Iowa","20":"Kansas","21":"Kentucky","22":"Louisiana","23":"Maine",
  "24":"Maryland","25":"Massachusetts","26":"Michigan","27":"Minnesota","28":"Mississippi",
  "29":"Missouri","30":"Montana","31":"Nebraska","32":"Nevada","33":"New Hampshire",
  "34":"New Jersey","35":"New Mexico","36":"New York","37":"North Carolina","38":"North Dakota",
  "39":"Ohio","40":"Oklahoma","41":"Oregon","42":"Pennsylvania","44":"Rhode Island",
  "45":"South Carolina","46":"South Dakota","47":"Tennessee","48":"Texas","49":"Utah",
  "50":"Vermont","51":"Virginia","53":"Washington","54":"West Virginia","55":"Wisconsin",
  "56":"Wyoming","72":"Puerto Rico",
};

export const FIPS_TO_ABBR: Record<string, string> = {
  "01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA",
  "08":"CO","09":"CT","10":"DE","11":"DC","12":"FL",
  "13":"GA","15":"HI","16":"ID","17":"IL","18":"IN",
  "19":"IA","20":"KS","21":"KY","22":"LA","23":"ME",
  "24":"MD","25":"MA","26":"MI","27":"MN","28":"MS",
  "29":"MO","30":"MT","31":"NE","32":"NV","33":"NH",
  "34":"NJ","35":"NM","36":"NY","37":"NC","38":"ND",
  "39":"OH","40":"OK","41":"OR","42":"PA","44":"RI",
  "45":"SC","46":"SD","47":"TN","48":"TX","49":"UT",
  "50":"VT","51":"VA","53":"WA","54":"WV","55":"WI",
  "56":"WY","72":"PR",
};

/** Abbreviation → full state name, e.g. "MI" → "Michigan" */
export const ABBR_TO_NAME: Record<string, string> = Object.fromEntries(
  Object.entries(FIPS_TO_ABBR).map(([fips, abbr]) => [abbr, FIPS_TO_NAME[fips] ?? abbr])
);

export interface StateFeatureProps {
  name: string;
  abbr: string;
  fips: string;
  avg_math_pct_prof: number | null;
  avg_pct_high_income: number | null;
  pearson_r: number | null;
  school_count: number | null;
}

export interface StateData {
  state: string;
  avg_math_pct_prof: number;
  avg_pct_high_income: number;
  pearson_r?: number | null;
  school_count: number;
}

const US_ATLAS_URL = 'https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json';

export async function fetchUSStateFeatures(
  stateData: StateData[]
): Promise<GeoJSON.FeatureCollection<GeoJSON.Geometry, StateFeatureProps>> {
  const resp = await fetch(US_ATLAS_URL);
  if (!resp.ok) throw new Error(`Failed to load US atlas: ${resp.status}`);
  const topo = await resp.json();
  if (!topo?.objects?.states) throw new Error('US atlas: missing objects.states');
  const fc = topojson.feature(topo, topo.objects.states) as unknown as GeoJSON.FeatureCollection;
  const statsMap = new Map(stateData.map(s => [s.state, s]));

  return {
    type: 'FeatureCollection',
    features: fc.features.flatMap(f => {
      if (f.id == null) return [];
      const fips = String(f.id).padStart(2, '0');
      const abbr = FIPS_TO_ABBR[fips] ?? '';
      const stats = statsMap.get(abbr);
      return [{
        ...f,
        properties: {
          name: FIPS_TO_NAME[fips] ?? abbr,
          abbr,
          fips,
          avg_math_pct_prof: stats?.avg_math_pct_prof ?? null,
          avg_pct_high_income: stats?.avg_pct_high_income ?? null,
          pearson_r: stats?.pearson_r ?? null,
          school_count: stats?.school_count ?? null,
        },
      }];
    }),
  };
}
