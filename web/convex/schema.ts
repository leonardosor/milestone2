import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

/**
 * EduInsight — Convex schema
 *
 * Three collections, all pre-computed at import time (no runtime DB queries):
 *   districts    — one row per school (~23K)
 *   county_stats — one row per county  (aggregated + Pearson r)
 *   state_stats  — one row per state   (aggregated + Pearson r)
 *
 * Income proxy: pct_high_income = % households earning $150k+ in the school's zip
 * Math outcome: math_pct_prof   = midpoint of proficiency band (grade 8, 2020)
 */
export default defineSchema({
  // ── School-level records ──────────────────────────────────────────────────
  districts: defineTable({
    school_name:       v.string(),
    ncessch:           v.optional(v.string()),   // NCES school ID
    state:             v.optional(v.string()),   // 2-letter state code
    county_fips:       v.optional(v.string()),   // 5-digit FIPS
    zip:               v.optional(v.string()),
    lat:               v.optional(v.number()),
    lon:               v.optional(v.number()),
    math_pct_prof:     v.optional(v.number()),   // % proficient (0–100)
    pct_high_income:   v.optional(v.number()),   // % HH earning $150k+ (0–100)
    pct_hhi_150k_200k: v.optional(v.number()),
    pct_hhi_220k_plus: v.optional(v.number()),
    enrollment:        v.optional(v.number()),
    teachers_fte:            v.optional(v.number()),
    grade_eight_enrollment:  v.optional(v.number()),
    math_counts:             v.optional(v.number()),
    read_counts:             v.optional(v.number()),
    read_high_pct:           v.optional(v.number()),
    avg_natwalkind:          v.optional(v.number()),
    total_10_14:             v.optional(v.number()),
    schools_in_zip:          v.optional(v.number()),
    student_teacher_ratio:   v.optional(v.number()),  // enrollment / teachers_fte
  })
    .index("by_state",        ["state"])
    .index("by_county_fips",  ["county_fips"])
    .index("by_state_county", ["state", "county_fips"]),

  // ── County-level aggregates ───────────────────────────────────────────────
  county_stats: defineTable({
    state:                v.string(),
    county_fips:          v.string(),
    avg_math_pct_prof:    v.number(),
    avg_pct_high_income:  v.number(),
    pearson_r:            v.optional(v.number()),  // null when school_count < 3 (income vs math)
    school_count:         v.number(),
    // ── Extra correlations vs math_pct_prof (each has its own paired sample size) ──
    avg_student_teacher_ratio:    v.optional(v.number()),
    pearson_r_student_teacher_ratio: v.optional(v.number()),
    n_student_teacher_ratio:      v.optional(v.number()),
    avg_walkability:              v.optional(v.number()),  // avg_natwalkind
    pearson_r_walkability:        v.optional(v.number()),
    n_walkability:                v.optional(v.number()),
    avg_read_high_pct:            v.optional(v.number()),
    pearson_r_reading:            v.optional(v.number()),
    n_reading:                    v.optional(v.number()),
  })
    .index("by_state",      ["state"])
    .index("by_county_fips", ["county_fips"]),

  // ── LLM usage tracking (AI insights cost controls) ───────────────────────
  llm_usage: defineTable({
    day:        v.string(),   // UTC date "YYYY-MM-DD"
    session_id: v.string(),   // client-generated per browser session
    tokens:     v.number(),   // input + output tokens consumed
    cost_usd:   v.number(),   // estimated spend
  })
    .index("by_day",         ["day"])
    .index("by_day_session", ["day", "session_id"]),

  // ── State-level aggregates ────────────────────────────────────────────────
  state_stats: defineTable({
    state:                v.string(),
    avg_math_pct_prof:    v.number(),
    avg_pct_high_income:  v.number(),
    pearson_r:            v.optional(v.number()),
    school_count:         v.number(),
    // ── Extra correlations vs math_pct_prof (each has its own paired sample size) ──
    avg_student_teacher_ratio:    v.optional(v.number()),
    pearson_r_student_teacher_ratio: v.optional(v.number()),
    n_student_teacher_ratio:      v.optional(v.number()),
    avg_walkability:              v.optional(v.number()),  // avg_natwalkind
    pearson_r_walkability:        v.optional(v.number()),
    n_walkability:                v.optional(v.number()),
    avg_read_high_pct:            v.optional(v.number()),
    pearson_r_reading:            v.optional(v.number()),
    n_reading:                    v.optional(v.number()),
  })
    .index("by_state", ["state"]),
});
