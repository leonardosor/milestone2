import { query } from "./_generated/server";
import { v } from "convex/values";

/**
 * EduInsight — Convex query functions
 *
 * All queries are read-only. Data is pre-loaded at import time.
 */

// ── State list (for the map selector) ────────────────────────────────────────

/** Return all state_stats rows, sorted by state code. */
export const listStates = query({
  args: {},
  handler: async (ctx) => {
    const rows = await ctx.db.query("state_stats").collect();
    return rows.sort((a, b) => (a.state ?? "").localeCompare(b.state ?? ""));
  },
});

// ── County choropleth ─────────────────────────────────────────────────────────

/** Return all county_stats for a given state. */
export const listCountiesByState = query({
  args: { state: v.string() },
  handler: async (ctx, { state }) => {
    return ctx.db
      .query("county_stats")
      .withIndex("by_state", (q) => q.eq("state", state))
      .collect();
  },
});

/** Return all county_stats (for full US choropleth). */
export const listAllCounties = query({
  args: {},
  handler: async (ctx) => {
    return ctx.db.query("county_stats").collect();
  },
});

// ── School scatter (drill-down) ───────────────────────────────────────────────

/** Return districts in a specific county (for scatter plot). */
export const listDistrictsByCounty = query({
  args: { state: v.string(), county_fips: v.string() },
  handler: async (ctx, { state, county_fips }) => {
    return ctx.db
      .query("districts")
      .withIndex("by_state_county", (q) =>
        q.eq("state", state).eq("county_fips", county_fips)
      )
      .collect();
  },
});

/** Return all districts in a state (for state-level scatter). */
export const listDistrictsByState = query({
  args: { state: v.string() },
  handler: async (ctx, { state }) => {
    return ctx.db
      .query("districts")
      .withIndex("by_state", (q) => q.eq("state", state))
      .collect();
  },
});

// ── Single-record lookups ─────────────────────────────────────────────────────

/** Return state_stats for one state. */
export const getStateStats = query({
  args: { state: v.string() },
  handler: async (ctx, { state }) => {
    const rows = await ctx.db
      .query("state_stats")
      .withIndex("by_state", (q) => q.eq("state", state))
      .collect();
    return rows[0] ?? null;
  },
});

/** Return county_stats for one county. */
export const getCountyStats = query({
  args: { county_fips: v.string() },
  handler: async (ctx, { county_fips }) => {
    const rows = await ctx.db
      .query("county_stats")
      .withIndex("by_county_fips", (q) => q.eq("county_fips", county_fips))
      .collect();
    return rows[0] ?? null;
  },
});
