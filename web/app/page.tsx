"use client";

import { useQuery } from "convex/react";
import { api } from "@/convex/_generated/api";
import clsx from "clsx";
import dynamic from "next/dynamic";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import PearsonPanel from "@/components/PearsonPanel";
import InsightsPanel, { type InsightsContext } from "@/components/InsightsPanel";
import { loadCountyNames } from "@/lib/usCounties";
import { ABBR_TO_NAME } from "@/lib/usStates";

const ChoroplethMap = dynamic(() => import("@/components/ChoroplethMap"), {
  ssr: false,
  loading: () => (
    <div className="lg:col-span-2 h-96 rounded-lg bg-[var(--surface)] animate-pulse" />
  ),
});

const CountyMap = dynamic(() => import("@/components/CountyMap"), {
  ssr: false,
  loading: () => (
    <div className="w-full h-full rounded-lg bg-[var(--surface)] animate-pulse" />
  ),
});

const ScatterPlot = dynamic(() => import("@/components/ScatterPlot"), {
  ssr: false,
  loading: () => <div className="h-72 rounded-lg bg-[var(--surface)] animate-pulse" />,
});

const IncomeBracketChart = dynamic(() => import("@/components/IncomeBracketChart"), {
  ssr: false,
  loading: () => <div className="h-80 rounded-lg bg-[var(--surface)] animate-pulse" />,
});

/* ── Stats helpers (for the AI insights context) ── */
function mean(v: number[]): number | null {
  return v.length ? +(v.reduce((a, b) => a + b, 0) / v.length).toFixed(2) : null;
}

function median(v: number[]): number | null {
  if (!v.length) return null;
  const s = [...v].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return +(s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2).toFixed(2);
}

function pearsonR(pts: { x: number; y: number }[]): number | null {
  if (pts.length < 3) return null;
  const n = pts.length;
  const mx = pts.reduce((a, p) => a + p.x, 0) / n;
  const my = pts.reduce((a, p) => a + p.y, 0) / n;
  let sxy = 0, sxx = 0, syy = 0;
  for (const p of pts) {
    sxy += (p.x - mx) * (p.y - my);
    sxx += (p.x - mx) ** 2;
    syy += (p.y - my) ** 2;
  }
  if (sxx === 0 || syy === 0) return null;
  return +(sxy / Math.sqrt(sxx * syy)).toFixed(3);
}

/* ── Sorting helpers ── */
type SortDir = "asc" | "desc";
interface SortState {
  key: string;
  dir: SortDir;
}

function compareValues(a: unknown, b: unknown, dir: SortDir): number {
  // Nulls always sort last, regardless of direction
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  const mul = dir === "asc" ? 1 : -1;
  if (typeof a === "string" && typeof b === "string") {
    return mul * a.localeCompare(b);
  }
  return mul * ((a as number) - (b as number));
}

interface SortThProps {
  label: string;
  sortKey: string;
  sort: SortState;
  onSort: (key: string) => void;
  align?: "left" | "right";
}

function SortTh({ label, sortKey, sort, onSort, align = "left" }: SortThProps) {
  const active = sort.key === sortKey;
  return (
    <th
      onClick={() => onSort(sortKey)}
      className={clsx(
        "px-4 py-2.5 cursor-pointer select-none hover:text-[var(--text)] transition-colors",
        align === "right" ? "text-right" : "text-left",
        active && "text-[var(--text)]"
      )}
      title={`Sort by ${label}`}
    >
      {label}
      <span className="inline-block w-4 text-[var(--accent)]">
        {active ? (sort.dir === "desc" ? " ▼" : " ▲") : ""}
      </span>
    </th>
  );
}

export default function Home() {
  const [selectedState, setSelectedState] = useState<string | null>(null);
  const [selectedCounty, setSelectedCounty] = useState<string | null>(null);
  const [countyNames, setCountyNames] = useState<Map<string, string>>(new Map());
  const [highlightSchool, setHighlightSchool] = useState<string | null>(null);
  const highlightRowRef = useRef<HTMLTableRowElement | null>(null);
  const states = useQuery(api.queries.listStates);
  const counties = useQuery(
    api.queries.listCountiesByState,
    selectedState ? { state: selectedState } : 'skip'
  );
  const schools = useQuery(
    api.queries.listDistrictsByCounty,
    selectedState && selectedCounty
      ? { state: selectedState, county_fips: selectedCounty }
      : 'skip'
  );
  const statedistricts = useQuery(
    api.queries.listDistrictsByState,
    selectedState && !selectedCounty ? { state: selectedState } : 'skip'
  );

  // Table sort state (shared slot; reset whenever drill level changes)
  const [sort, setSort] = useState<SortState>({ key: "math", dir: "desc" });
  const defaultDirs: Record<string, SortDir> = {
    name: "asc",
    schools: "desc",
    enrollment: "desc",
    math: "desc",
    income: "desc",
    pearson: "desc",
  };
  const handleSort = useCallback((key: string) => {
    setSort(prev =>
      prev.key === key
        ? { key, dir: prev.dir === "desc" ? "asc" : "desc" }
        : { key, dir: defaultDirs[key] ?? "desc" }
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Patch 5: stable ref — prevents CountyMap useEffect from re-firing on every Convex tick
  const countyStatsMemo = useMemo(() => counties ?? [], [counties]);

  const scatterData = useMemo(() => {
    if (selectedState && selectedCounty) {
      return (schools ?? [])
        .filter((s): s is typeof s & { math_pct_prof: number; pct_high_income: number } =>
          s.pct_high_income != null && s.math_pct_prof != null
        )
        .map(s => ({ x: s.pct_high_income, y: s.math_pct_prof, name: s.school_name }));
    }
    if (selectedState) {
      return (statedistricts ?? [])
        .filter((s): s is typeof s & { math_pct_prof: number; pct_high_income: number } =>
          s.pct_high_income != null && s.math_pct_prof != null
        )
        .map(s => ({ x: s.pct_high_income, y: s.math_pct_prof, name: s.school_name }));
    }
    return (states ?? [])
      .filter((s): s is typeof s & { avg_pct_high_income: number; avg_math_pct_prof: number } =>
        s.avg_pct_high_income != null && s.avg_math_pct_prof != null
      )
      .map(s => ({
        x: s.avg_pct_high_income,
        y: s.avg_math_pct_prof,
        name: s.state,
      }));
  }, [selectedState, selectedCounty, schools, statedistricts, states]);

  const scatterLoading =
    (selectedState !== null && selectedCounty !== null && schools === undefined) ||
    (selectedState !== null && selectedCounty === null && statedistricts === undefined) ||
    (selectedState === null && states === undefined);

  const pearsonData = useMemo(() => {
    if (selectedState && selectedCounty) {
      const c = (counties ?? []).find(c => c.county_fips === selectedCounty);
      return {
        r: c?.pearson_r ?? null,
        count: c?.school_count,
        label: countyNames.get(selectedCounty) ?? selectedCounty,
      };
    }
    if (selectedState) {
      const s = (states ?? []).find(s => s.state === selectedState);
      return {
        r: s?.pearson_r ?? null,
        count: s?.school_count,
        label: ABBR_TO_NAME[selectedState] ?? selectedState,
      };
    }
    return null;
  }, [selectedState, selectedCounty, counties, states, countyNames]);

  const pearsonLoading =
    (selectedState !== null && selectedCounty !== null && counties === undefined) ||
    (selectedState !== null && selectedCounty === null && states === undefined);

  useEffect(() => {
    if (!selectedState) return;
    setSelectedCounty(null);
    // Patch 4: clear stale names before new fetch
    setCountyNames(new Map());
    loadCountyNames(selectedState).then(setCountyNames).catch(() => {});
  }, [selectedState]);

  // Reset sort + highlight when drill level changes
  useEffect(() => {
    setSort({ key: "math", dir: "desc" });
    setHighlightSchool(null);
  }, [selectedState, selectedCounty]);

  // Scroll the highlighted school row into view, then let the flash fade
  useEffect(() => {
    if (!highlightSchool) return;
    highlightRowRef.current?.scrollIntoView({ behavior: "smooth", block: "center" });
    const t = setTimeout(() => setHighlightSchool(null), 3000);
    return () => clearTimeout(t);
  }, [highlightSchool]);

  // Drill-through from scatter chart:
  //  • national view → point = a state → select it
  //  • county view   → point = a school → highlight its row in the table
  const handleScatterClick = useCallback(
    (name: string) => {
      if (!selectedState) {
        setSelectedState(name);
      } else if (selectedCounty) {
        setHighlightSchool(name);
      }
    },
    [selectedState, selectedCounty]
  );

  /* ── Sorted table rows ── */
  const sortedSchools = useMemo(() => {
    const acc: Record<string, (s: NonNullable<typeof schools>[number]) => unknown> = {
      name: s => s.school_name,
      enrollment: s => s.enrollment,
      math: s => s.math_pct_prof,
      income: s => s.pct_high_income,
    };
    const get = acc[sort.key] ?? acc.math;
    return (schools ?? []).slice().sort((a, b) => compareValues(get(a), get(b), sort.dir));
  }, [schools, sort]);

  const sortedCounties = useMemo(() => {
    const acc: Record<string, (c: NonNullable<typeof counties>[number]) => unknown> = {
      name: c => countyNames.get(c.county_fips) ?? c.county_fips,
      schools: c => c.school_count,
      math: c => c.avg_math_pct_prof,
      income: c => c.avg_pct_high_income,
      pearson: c => c.pearson_r,
    };
    const get = acc[sort.key] ?? acc.math;
    return (counties ?? []).slice().sort((a, b) => compareValues(get(a), get(b), sort.dir));
  }, [counties, sort, countyNames]);

  const sortedStates = useMemo(() => {
    const acc: Record<string, (s: NonNullable<typeof states>[number]) => unknown> = {
      name: s => ABBR_TO_NAME[s.state] ?? s.state,
      schools: s => s.school_count,
      math: s => s.avg_math_pct_prof,
      income: s => s.avg_pct_high_income,
      pearson: s => s.pearson_r,
    };
    const get = acc[sort.key] ?? acc.math;
    return (states ?? []).slice().sort((a, b) => compareValues(get(a), get(b), sort.dir));
  }, [states, sort]);

  /* ── AI insights context: snapshot of the on-screen data + derived stats ── */
  const insightsLevel = selectedState && selectedCounty ? 'county' : selectedState ? 'state' : 'national';
  const insightsReady =
    insightsLevel === 'county'
      ? schools !== undefined
      : insightsLevel === 'state'
      ? counties !== undefined
      : states !== undefined;

  const insightsContext = useMemo<InsightsContext>(() => {
    const level = insightsLevel as InsightsContext['level'];
    const label =
      level === 'county'
        ? `${countyNames.get(selectedCounty!) ?? selectedCounty}, ${ABBR_TO_NAME[selectedState!] ?? selectedState}`
        : level === 'state'
        ? ABBR_TO_NAME[selectedState!] ?? selectedState!
        : 'United States';

    let rows: Record<string, unknown>[];
    if (level === 'county') {
      rows = (schools ?? []).map(s => ({
        name: s.school_name,
        enrollment: s.enrollment ?? null,
        math_pct_prof: s.math_pct_prof ?? null,
        pct_high_income: s.pct_high_income ?? null,
      }));
    } else if (level === 'state') {
      rows = (counties ?? []).map(c => ({
        name: countyNames.get(c.county_fips) ?? c.county_fips,
        school_count: c.school_count,
        avg_math_pct_prof: c.avg_math_pct_prof,
        avg_pct_high_income: c.avg_pct_high_income,
        pearson_r: c.pearson_r ?? null,
      }));
    } else {
      rows = (states ?? []).map(s => ({
        name: ABBR_TO_NAME[s.state] ?? s.state,
        school_count: s.school_count,
        avg_math_pct_prof: s.avg_math_pct_prof,
        avg_pct_high_income: s.avg_pct_high_income,
        pearson_r: s.pearson_r ?? null,
      }));
    }

    const byMath = [...scatterData].sort((a, b) => b.y - a.y);
    const stats = {
      row_count: rows.length,
      paired_income_math_points: scatterData.length,
      mean_math_pct: mean(scatterData.map(d => d.y)),
      median_math_pct: median(scatterData.map(d => d.y)),
      mean_high_income_pct: mean(scatterData.map(d => d.x)),
      median_high_income_pct: median(scatterData.map(d => d.x)),
      pearson_r_on_screen: pearsonR(scatterData),
      pearson_r_precomputed: pearsonData?.r ?? null,
      top_3_by_math: byMath.slice(0, 3).map(d => ({ name: d.name, math: d.y, income: d.x })),
      bottom_3_by_math: byMath.slice(-3).reverse().map(d => ({ name: d.name, math: d.y, income: d.x })),
    };

    return { level, label, stats, rows: rows.slice(0, 400) };
  }, [insightsLevel, selectedState, selectedCounty, schools, counties, states, countyNames, scatterData, pearsonData]);

  const loaded = states !== undefined;
  const count = states?.length ?? 0;
  const totalSchools = states?.reduce((sum, s) => sum + s.school_count, 0) ?? 0;

  return (
    <div className="p-6 space-y-8">
      {/* ── Hero ── */}
      <section className="max-w-3xl space-y-2">
        <h1 className="text-4xl font-bold">
          Does income predict math scores?
        </h1>
        <p className="text-[var(--muted)] text-lg leading-relaxed">
          This dashboard maps the correlation between household wealth and
          grade-8 math proficiency across {loaded ? `${totalSchools.toLocaleString()} US schools` : "US schools"}
          {" "}in {loaded ? count : '…'} states. Select a state to explore counties; select a county
          to see individual schools.
        </p>
      </section>

      {/* ── Data status pill ── */}
      <div
        className={clsx(
          "inline-flex items-center gap-2 px-3 py-1.5 rounded-full text-sm border",
          loaded && count > 0
            ? "border-green-300 bg-green-50 text-green-700"
            : "border-yellow-300 bg-yellow-50 text-yellow-700"
        )}
      >
        <span
          className={clsx(
            "w-2 h-2 rounded-full",
            loaded && count > 0 ? "bg-green-500" : "bg-yellow-500"
          )}
        />
        {!loaded
          ? "Connecting to Convex…"
          : count === 0
          ? "No data — run npx convex import"
          : `Live · ${count} states loaded`}
      </div>

      {/* ── Map + side panels ── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {loaded && count > 0 ? (
          <div className="lg:col-span-2 h-96">
            {selectedState ? (
              // Patch 7: show pulse while Convex query is in-flight (counties===undefined)
              counties === undefined ? (
                <div className="w-full h-full rounded-lg bg-[var(--surface)] animate-pulse" />
              ) : (
                <CountyMap
                  stateAbbr={selectedState}
                  countyStats={countyStatsMemo}
                  onBack={() => { setSelectedState(null); setSelectedCounty(null); }}
                  onCountySelect={setSelectedCounty}
                  selectedCounty={selectedCounty}
                  schools={selectedCounty ? schools ?? [] : []}
                  onSchoolSelect={setHighlightSchool}
                />
              )
            ) : (
              <ChoroplethMap stateStats={states!} onStateSelect={setSelectedState} />
            )}
          </div>
        ) : (
          <div className="lg:col-span-2 h-96 rounded-lg border border-dashed border-[var(--border)] flex items-center justify-center text-[var(--muted)] text-sm">
            {!loaded ? 'Connecting to Convex…' : 'No data — run npx convex import'}
          </div>
        )}
        <div className="space-y-4">
          <PearsonPanel
            pearsonR={pearsonData?.r ?? null}
            label={pearsonData?.label}
            schoolCount={pearsonData?.count}
            noSelection={pearsonData === null}
            loading={pearsonLoading}
          />
          <ScatterPlot
            data={scatterData}
            loading={scatterLoading}
            xLabel={selectedState ? 'Income %' : 'Avg Income %'}
            yLabel={selectedState ? 'Math %' : 'Avg Math %'}
            onPointClick={handleScatterClick}
          />
          {!selectedState && (
            <p className="text-sm text-[var(--muted)]">
              Tip: click a dot to drill into that state.
            </p>
          )}
          {selectedState && selectedCounty && (
            <p className="text-sm text-[var(--muted)]">
              Tip: click a dot to find that school in the table.
            </p>
          )}
        </div>
      </div>

      {/* ── AI narrative summary + open-ended Q&A over on-screen data ── */}
      {loaded && count > 0 && (
        <InsightsPanel context={insightsContext} ready={insightsReady} />
      )}

      {/* ── Proficiency vs income brackets (100% stacked, quintile PDF-based) ── */}
      {loaded && count > 0 && (
        <section className="space-y-3">
          <h2 className="text-xl font-semibold">
            Proficiency by Income Bracket
            <span className="text-lg font-normal text-[var(--muted)]">
              {' '}— {selectedState && selectedCounty
                ? `schools in ${countyNames.get(selectedCounty) ?? selectedCounty}`
                : selectedState
                ? `districts in ${ABBR_TO_NAME[selectedState] ?? selectedState}`
                : 'state averages, US'}
              {' '}· brackets are income quintiles
            </span>
          </h2>
          <IncomeBracketChart
            data={scatterData}
            loading={scatterLoading}
            unitLabel={
              selectedState && selectedCounty
                ? 'schools'
                : selectedState
                ? 'districts'
                : 'states'
            }
          />
        </section>
      )}

      {/* ── Dynamic table: schools → counties → states based on selection ── */}
      {loaded && count > 0 && (
        <section className="space-y-3">
          {selectedState && selectedCounty ? (
            /* ── Level 3: School details for selected county ── */
            <>
              <div className="flex items-center gap-3">
                <button
                  onClick={() => setSelectedCounty(null)}
                  className="text-base text-[var(--accent)] hover:underline"
                >
                  ← {ABBR_TO_NAME[selectedState] ?? selectedState}
                </button>
                <h2 className="text-xl font-semibold">
                  {countyNames.get(selectedCounty) ?? selectedCounty}
                  <span className="text-lg font-normal text-[var(--muted)]">
                    {' '}— Schools · Gr. 8 Math Proficiency
                  </span>
                </h2>
              </div>
              <div className="overflow-x-auto rounded-lg border border-[var(--border)]">
                <table className="w-full text-base">
                  <thead>
                    <tr className="bg-[var(--surface)] text-[var(--muted)]">
                      <SortTh label="School" sortKey="name" sort={sort} onSort={handleSort} />
                      <SortTh label="Enrollment" sortKey="enrollment" sort={sort} onSort={handleSort} align="right" />
                      <SortTh label="Math Prof. %" sortKey="math" sort={sort} onSort={handleSort} align="right" />
                      <SortTh label="High Income %" sortKey="income" sort={sort} onSort={handleSort} align="right" />
                    </tr>
                  </thead>
                  <tbody>
                    {schools === undefined ? (
                      <tr>
                        <td colSpan={4} className="px-4 py-4 text-center text-[var(--muted)]">
                          Loading…
                        </td>
                      </tr>
                    ) : sortedSchools.map((s) => {
                        const isHighlighted = highlightSchool === s.school_name;
                        return (
                          <tr
                            key={s._id}
                            ref={isHighlighted ? highlightRowRef : undefined}
                            className={clsx(
                              "border-t border-[var(--border)] hover:bg-[var(--row-hover)] transition-colors duration-500",
                              isHighlighted && "bg-[var(--accent)]/25"
                            )}
                          >
                            <td className="px-4 py-2.5">{s.school_name}</td>
                            <td className="px-4 py-2.5 text-right">
                              {s.enrollment != null ? s.enrollment.toLocaleString() : '—'}
                            </td>
                            <td className="px-4 py-2.5 text-right">
                              {s.math_pct_prof != null ? `${s.math_pct_prof.toFixed(1)}%` : '—'}
                            </td>
                            <td className="px-4 py-2.5 text-right">
                              {s.pct_high_income != null ? `${s.pct_high_income.toFixed(1)}%` : '—'}
                            </td>
                          </tr>
                        );
                      })}
                  </tbody>
                </table>
              </div>
            </>
          ) : selectedState ? (
            /* ── Level 2: Counties for selected state ── */
            <>
              <h2 className="text-xl font-semibold">
                Counties in {ABBR_TO_NAME[selectedState] ?? selectedState}
                <span className="text-lg font-normal text-[var(--muted)]">
                  {' '}— Gr. 8 Math Proficiency · click a county for schools
                </span>
              </h2>
              <div className="overflow-x-auto rounded-lg border border-[var(--border)]">
                <table className="w-full text-base">
                  <thead>
                    <tr className="bg-[var(--surface)] text-[var(--muted)]">
                      <SortTh label="County" sortKey="name" sort={sort} onSort={handleSort} />
                      <SortTh label="Schools" sortKey="schools" sort={sort} onSort={handleSort} align="right" />
                      <SortTh label="Math Prof. %" sortKey="math" sort={sort} onSort={handleSort} align="right" />
                      <SortTh label="High Income %" sortKey="income" sort={sort} onSort={handleSort} align="right" />
                      <SortTh label="Pearson r" sortKey="pearson" sort={sort} onSort={handleSort} align="right" />
                    </tr>
                  </thead>
                  <tbody>
                    {sortedCounties.map((c) => (
                      <tr
                        key={c.county_fips}
                        onClick={() => setSelectedCounty(c.county_fips)}
                        className="border-t border-[var(--border)] hover:bg-[var(--row-hover)] transition-colors cursor-pointer"
                      >
                        <td className="px-4 py-2.5">
                          {countyNames.get(c.county_fips) ?? c.county_fips}
                        </td>
                        <td className="px-4 py-2.5 text-right">{c.school_count.toLocaleString()}</td>
                        <td className="px-4 py-2.5 text-right">{c.avg_math_pct_prof != null ? `${c.avg_math_pct_prof.toFixed(1)}%` : '—'}</td>
                        <td className="px-4 py-2.5 text-right">{c.avg_pct_high_income != null ? `${c.avg_pct_high_income.toFixed(1)}%` : '—'}</td>
                        <td
                          className={clsx(
                            "px-4 py-2.5 text-right font-mono",
                            c.pearson_r != null && c.pearson_r > 0.3
                              ? "text-green-600"
                              : "text-[var(--muted)]"
                          )}
                        >
                          {c.pearson_r != null ? c.pearson_r.toFixed(3) : "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          ) : (
            /* ── Level 1: All states ── */
            <>
              <h2 className="text-xl font-semibold">State Summaries
                <span className="text-lg font-normal text-[var(--muted)]">
                  {' '}— click a state to drill down
                </span>
              </h2>
              <div className="overflow-x-auto rounded-lg border border-[var(--border)]">
                <table className="w-full text-base">
                  <thead>
                    <tr className="bg-[var(--surface)] text-[var(--muted)]">
                      <SortTh label="State" sortKey="name" sort={sort} onSort={handleSort} />
                      <SortTh label="Schools" sortKey="schools" sort={sort} onSort={handleSort} align="right" />
                      <SortTh label="Math Prof. %" sortKey="math" sort={sort} onSort={handleSort} align="right" />
                      <SortTh label="High Income %" sortKey="income" sort={sort} onSort={handleSort} align="right" />
                      <SortTh label="Pearson r" sortKey="pearson" sort={sort} onSort={handleSort} align="right" />
                    </tr>
                  </thead>
                  <tbody>
                    {sortedStates.map((s) => (
                      <tr
                        key={s.state}
                        onClick={() => setSelectedState(s.state)}
                        className="border-t border-[var(--border)] hover:bg-[var(--row-hover)] transition-colors cursor-pointer"
                      >
                        <td className="px-4 py-2.5 font-semibold">{ABBR_TO_NAME[s.state] ?? s.state}</td>
                        <td className="px-4 py-2.5 text-right">{s.school_count.toLocaleString()}</td>
                        <td className="px-4 py-2.5 text-right">{s.avg_math_pct_prof != null ? `${s.avg_math_pct_prof.toFixed(1)}%` : '—'}</td>
                        <td className="px-4 py-2.5 text-right">{s.avg_pct_high_income != null ? `${s.avg_pct_high_income.toFixed(1)}%` : '—'}</td>
                        <td
                          className={clsx(
                            "px-4 py-2.5 text-right font-mono",
                            s.pearson_r != null && s.pearson_r > 0.3
                              ? "text-green-600"
                              : "text-[var(--muted)]"
                          )}
                        >
                          {s.pearson_r != null ? s.pearson_r.toFixed(3) : "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </section>
      )}
    </div>
  );
}
