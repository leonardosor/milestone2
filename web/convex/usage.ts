import { query, mutation } from "./_generated/server";
import { v } from "convex/values";

/**
 * LLM usage tracking for the AI insights feature.
 *
 * One row per (day, session). /api/insights calls `authorizeUsage` (a
 * mutation, so the limit check + session registration are one atomic
 * transaction) before each LLM call, and `recordUsage` afterwards with
 * actual token counts.
 *
 * Both mutations defensively merge duplicate (day, session) rows so a bad
 * import or historical race can never inflate counts or crash `.unique()`.
 */

type Row = {
  _id: any;
  day: string;
  session_id: string;
  tokens: number;
  cost_usd: number;
};

/** Merge duplicate rows per session (self-healing); returns deduped map. */
async function dedupedSessions(ctx: any, day: string): Promise<Map<string, Row>> {
  const rows: Row[] = await ctx.db
    .query("llm_usage")
    .withIndex("by_day", (q: any) => q.eq("day", day))
    .collect();
  const bySession = new Map<string, Row>();
  for (const r of rows) {
    const prev = bySession.get(r.session_id);
    if (prev) {
      const tokens = prev.tokens + r.tokens;
      const cost_usd = prev.cost_usd + r.cost_usd;
      await ctx.db.patch(prev._id, { tokens, cost_usd });
      await ctx.db.delete(r._id);
      prev.tokens = tokens;
      prev.cost_usd = cost_usd;
    } else {
      bySession.set(r.session_id, { ...r });
    }
  }
  return bySession;
}

/** Read-only usage snapshot (dashboard/debug; duplicates collapsed, not written). */
export const getUsageStatus = query({
  args: { day: v.string(), session_id: v.string() },
  handler: async (ctx, { day, session_id }) => {
    const rows: Row[] = await ctx.db
      .query("llm_usage")
      .withIndex("by_day", (q) => q.eq("day", day))
      .collect();
    const sessions = new Map<string, number>();
    let totalCostUsd = 0;
    for (const r of rows) {
      totalCostUsd += r.cost_usd;
      sessions.set(r.session_id, (sessions.get(r.session_id) ?? 0) + r.tokens);
    }
    return {
      totalCostUsd,
      sessionCount: sessions.size,
      sessionTokens: sessions.get(session_id) ?? 0,
      sessionExists: sessions.has(session_id),
    };
  },
});

/**
 * Atomically check limits AND register the session. Convex mutations are
 * serializable transactions, so two concurrent calls cannot both slip past
 * the caps or double-register a session.
 */
export const authorizeUsage = mutation({
  args: {
    day: v.string(),
    session_id: v.string(),
    daily_budget_usd: v.number(),
    session_token_cap: v.number(),
    max_sessions_per_day: v.number(),
  },
  handler: async (
    ctx,
    { day, session_id, daily_budget_usd, session_token_cap, max_sessions_per_day }
  ) => {
    const bySession = await dedupedSessions(ctx, day);
    let totalCostUsd = 0;
    for (const r of bySession.values()) totalCostUsd += r.cost_usd;
    const session = bySession.get(session_id);

    if (totalCostUsd >= daily_budget_usd) {
      return { allowed: false, reason: "budget" as const };
    }
    if (session && session.tokens >= session_token_cap) {
      return { allowed: false, reason: "session_tokens" as const };
    }
    if (!session && bySession.size >= max_sessions_per_day) {
      return { allowed: false, reason: "max_sessions" as const };
    }
    // Register the session inside this same transaction so the session cap
    // can't be raced by concurrent first requests.
    if (!session) {
      await ctx.db.insert("llm_usage", { day, session_id, tokens: 0, cost_usd: 0 });
    }
    return { allowed: true as const };
  },
});

/** Add actual spend for a session (upsert; merges duplicates defensively). */
export const recordUsage = mutation({
  args: {
    day: v.string(),
    session_id: v.string(),
    tokens: v.number(),
    cost_usd: v.number(),
  },
  handler: async (ctx, { day, session_id, tokens, cost_usd }) => {
    const bySession = await dedupedSessions(ctx, day);
    const existing = bySession.get(session_id);
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
