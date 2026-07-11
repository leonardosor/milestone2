import { query, mutation } from "./_generated/server";
import { v } from "convex/values";

/**
 * LLM usage tracking for the AI insights feature.
 *
 * One row per (day, session). The /api/insights route checks limits before
 * each LLM call and records actual token usage afterwards.
 */

export const getUsageStatus = query({
  args: { day: v.string(), session_id: v.string() },
  handler: async (ctx, { day, session_id }) => {
    const rows = await ctx.db
      .query("llm_usage")
      .withIndex("by_day", (q) => q.eq("day", day))
      .collect();
    const session = rows.find((r) => r.session_id === session_id);
    return {
      totalCostUsd: rows.reduce((s, r) => s + r.cost_usd, 0),
      sessionCount: rows.length, // distinct sessions that spent tokens today
      sessionTokens: session?.tokens ?? 0,
      sessionExists: session !== undefined,
    };
  },
});

export const recordUsage = mutation({
  args: {
    day: v.string(),
    session_id: v.string(),
    tokens: v.number(),
    cost_usd: v.number(),
  },
  handler: async (ctx, { day, session_id, tokens, cost_usd }) => {
    const existing = await ctx.db
      .query("llm_usage")
      .withIndex("by_day_session", (q) =>
        q.eq("day", day).eq("session_id", session_id)
      )
      .unique();
    if (existing) {
      await ctx.db.patch(existing._id, {
        tokens: existing.tokens + tokens,
        cost_usd: existing.cost_usd + cost_usd,
      });
    } else {
      await ctx.db.insert("llm_usage", { day, session_id, tokens, cost_usd });
    }
  },
});
