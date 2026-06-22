"use client";

import { useQuery } from "convex/react";
import { api } from "@/convex/_generated/api";
import clsx from "clsx";
import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import PearsonPanel from "@/components/PearsonPanel";
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
  loading: () => <div className="h-44 rounded-lg bg-[var(--surface)] animate-pulse" />,
});

export default function Home() {
  const [selectedState, setSelectedState] = useState<string | null>(null);
  const [selectedCounty, setSelectedCounty] = useState<string | null>(null);
  const [countyNames, setCountyNames] = useState<Map<string, string>>(new Map());
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

  const loaded = states !== undefined;
  const count = states?.length ?? 0;
  const totalSchools = states?.reduce((sum, s) => sum + s.school_count, 0) ?? 0;

  return (
    <div className="p-6 space-y-8">
      {/* ── Hero ── */}
      <section className="max-w-2xl space-y-2">
        <h1 className="text-3xl font-bold">
          Does income predict math scores?
        </h1>
        <p className="text-[var(--muted)] leading-relaxed">
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
            ? "border-green-800 bg-green-950 text-green-400"
            : "border-yellow-800 bg-yellow-950 text-yellow-400"
        )}
      >
        <span
          className={clsx(
            "w-2 h-2 rounded-full",
            loaded && count > 0 ? "bg-green-400" : "bg-yellow-400"
          )}
        />
        {!loaded
          ? "Connecting to Convex…"
          : count === 0
          ? "No data — run npx convex import"
          : `Live · ${count} states loaded`}
      </div>

      {/* ── Placeholder panels ── */}
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
          />
        </div>
      </div>

      {/* ── Dynamic table: schools → counties → states based on selection ── */}
      {loaded && count > 0 && (
        <section className="space-y-3">
          {selectedState && selectedCounty ? (
            /* ── Level 3: School details for selected county ── */
            <>
              <div className="flex items-center gap-3">
                <button
                  onClick={() => setSelectedCounty(null)}
                  className="text-sm text-[var(--accent)] hover:underline"
                >
                  ← {ABBR_TO_NAME[selectedState] ?? selectedState}
                </button>
                <h2 className="text-lg font-semibold">
                  {countyNames.get(selectedCounty) ?? selectedCounty}
                  <span className="text-base font-normal text-[var(--muted)]">
                    {' '}— Schools · Gr. 8 Math Proficiency
                  </span>
                </h2>
              </div>
              <div className="overflow-x-auto rounded-lg border border-[var(--border)]">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-[var(--surface)] text-[var(--muted)] text-left">
                      <th className="px-4 py-2">School</th>
                      <th className="px-4 py-2 text-right">Enrollment</th>
                      <th className="px-4 py-2 text-right">Math Prof. %</th>
                      <th className="px-4 py-2 text-right">High Income %</th>
                    </tr>
                  </thead>
                  <tbody>
                    {schools === undefined ? (
                      <tr>
                        <td colSpan={4} className="px-4 py-4 text-center text-[var(--muted)]">
                          Loading…
                        </td>
                      </tr>
                    ) : (schools ?? [])
                      .slice()
                      .sort((a, b) => (b.math_pct_prof ?? 0) - (a.math_pct_prof ?? 0))
                      .map((s) => (
                        <tr
                          key={s._id}
                          className="border-t border-[var(--border)] hover:bg-[var(--surface)] transition-colors"
                        >
                          <td className="px-4 py-2">{s.school_name}</td>
                          <td className="px-4 py-2 text-right">
                            {s.enrollment != null ? s.enrollment.toLocaleString() : '—'}
                          </td>
                          <td className="px-4 py-2 text-right">
                            {s.math_pct_prof != null ? `${s.math_pct_prof.toFixed(1)}%` : '—'}
                          </td>
                          <td className="px-4 py-2 text-right">
                            {s.pct_high_income != null ? `${s.pct_high_income.toFixed(1)}%` : '—'}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </>
          ) : selectedState ? (
            /* ── Level 2: Counties for selected state ── */
            <>
              <h2 className="text-lg font-semibold">
                Counties in {ABBR_TO_NAME[selectedState] ?? selectedState}
                <span className="text-base font-normal text-[var(--muted)]">
                  {' '}— Gr. 8 Math Proficiency · click a county for schools
                </span>
              </h2>
              <div className="overflow-x-auto rounded-lg border border-[var(--border)]">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-[var(--surface)] text-[var(--muted)] text-left">
                      <th className="px-4 py-2">County</th>
                      <th className="px-4 py-2 text-right">Schools</th>
                      <th className="px-4 py-2 text-right">Math Prof. %</th>
                      <th className="px-4 py-2 text-right">High Income %</th>
                      <th className="px-4 py-2 text-right">Pearson r</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(counties ?? [])
                      .slice()
                      .sort((a, b) => (b.avg_math_pct_prof ?? 0) - (a.avg_math_pct_prof ?? 0))
                      .map((c) => (
                        <tr
                          key={c.county_fips}
                          onClick={() => setSelectedCounty(c.county_fips)}
                          className="border-t border-[var(--border)] hover:bg-[var(--surface)] transition-colors cursor-pointer"
                        >
                          <td className="px-4 py-2">
                            {countyNames.get(c.county_fips) ?? c.county_fips}
                          </td>
                          <td className="px-4 py-2 text-right">{c.school_count.toLocaleString()}</td>
                          <td className="px-4 py-2 text-right">{c.avg_math_pct_prof != null ? `${c.avg_math_pct_prof.toFixed(1)}%` : '—'}</td>
                          <td className="px-4 py-2 text-right">{c.avg_pct_high_income != null ? `${c.avg_pct_high_income.toFixed(1)}%` : '—'}</td>
                          <td
                            className={clsx(
                              "px-4 py-2 text-right font-mono",
                              c.pearson_r != null && c.pearson_r > 0.3
                                ? "text-green-400"
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
              <h2 className="text-lg font-semibold">State Summaries
                <span className="text-base font-normal text-[var(--muted)]">
                  {' '}— click a state to drill down
                </span>
              </h2>
              <div className="overflow-x-auto rounded-lg border border-[var(--border)]">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-[var(--surface)] text-[var(--muted)] text-left">
                      <th className="px-4 py-2">State</th>
                      <th className="px-4 py-2 text-right">Schools</th>
                      <th className="px-4 py-2 text-right">Math Prof. %</th>
                      <th className="px-4 py-2 text-right">High Income %</th>
                      <th className="px-4 py-2 text-right">Pearson r</th>
                    </tr>
                  </thead>
                  <tbody>
                    {states.map((s) => (
                      <tr
                        key={s.state}
                        onClick={() => setSelectedState(s.state)}
                        className="border-t border-[var(--border)] hover:bg-[var(--surface)] transition-colors cursor-pointer"
                      >
                        <td className="px-4 py-2 font-semibold">{ABBR_TO_NAME[s.state] ?? s.state}</td>
                        <td className="px-4 py-2 text-right">{s.school_count.toLocaleString()}</td>
                        <td className="px-4 py-2 text-right">{s.avg_math_pct_prof != null ? `${s.avg_math_pct_prof.toFixed(1)}%` : '—'}</td>
                        <td className="px-4 py-2 text-right">{s.avg_pct_high_income != null ? `${s.avg_pct_high_income.toFixed(1)}%` : '—'}</td>
                        <td
                          className={clsx(
                            "px-4 py-2 text-right font-mono",
                            s.pearson_r != null && s.pearson_r > 0.3
                              ? "text-green-400"
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
