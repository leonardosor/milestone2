'use client';

import { useCallback, useEffect, useRef, useState } from 'react';

/** The on-screen data snapshot sent to the LLM. */
export interface InsightsContext {
  level: 'national' | 'state' | 'county';
  label: string;
  stats: Record<string, unknown>;
  rows: Record<string, unknown>[];
}

interface InsightsPanelProps {
  context: InsightsContext;
  /** False while the underlying Convex queries are still loading. */
  ready: boolean;
}

interface ChatTurn {
  role: 'user' | 'assistant';
  content: string;
}

const SUGGESTIONS = [
  'Which areas punch above their income level?',
  'How strong is the income–score relationship here?',
  'What are the biggest outliers and why might that be?',
];

/** Stable per-browser-session ID for server-side usage caps. */
function getSessionId(): string {
  if (typeof window === 'undefined') return 'ssr';
  let id = sessionStorage.getItem('insights-session-id');
  if (!id) {
    id = crypto.randomUUID();
    sessionStorage.setItem('insights-session-id', id);
  }
  return id;
}

async function streamInsights(
  body: unknown,
  onChunk: (text: string) => void,
  signal: AbortSignal
): Promise<void> {
  const res = await fetch('/api/insights', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal,
  });
  if (!res.ok || !res.body) {
    const detail = await res.text().catch(() => '');
    throw new Error(detail || `Request failed (${res.status})`);
  }
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    onChunk(decoder.decode(value, { stream: true }));
  }
}

export default function InsightsPanel({ context, ready }: InsightsPanelProps) {
  const [summary, setSummary] = useState('');
  const [summaryStatus, setSummaryStatus] = useState<'idle' | 'loading' | 'done' | 'error'>('idle');
  const [summaryError, setSummaryError] = useState('');
  const [chat, setChat] = useState<ChatTurn[]>([]);
  const [input, setInput] = useState('');
  const [chatBusy, setChatBusy] = useState(false);
  const [chatError, setChatError] = useState('');

  // Latest context for chat sends; avoids stale closures
  const contextRef = useRef(context);
  contextRef.current = context;

  const summaryAbort = useRef<AbortController | null>(null);
  const chatAbort = useRef<AbortController | null>(null);
  const chatEndRef = useRef<HTMLDivElement | null>(null);

  const generateSummary = useCallback(() => {
    summaryAbort.current?.abort();
    const ctrl = new AbortController();
    summaryAbort.current = ctrl;
    setSummary('');
    setSummaryError('');
    setSummaryStatus('loading');
    streamInsights(
      { mode: 'summary', context: contextRef.current, sessionId: getSessionId() },
      chunk => setSummary(prev => prev + chunk),
      ctrl.signal
    )
      .then(() => { if (!ctrl.signal.aborted) setSummaryStatus('done'); })
      .catch((err: Error) => {
        if (ctrl.signal.aborted) return;
        setSummaryError(err.message);
        setSummaryStatus('error');
      });
  }, []);

  // Auto-generate when the drill level / selection changes (once data is ready)
  useEffect(() => {
    if (!ready || context.rows.length === 0) return;
    generateSummary();
    return () => summaryAbort.current?.abort();
    // Re-run only when the view identity changes, not on every data tick
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ready, context.level, context.label]);

  const ask = useCallback((questionRaw: string) => {
    const question = questionRaw.trim();
    if (!question || chatBusy) return;
    chatAbort.current?.abort();
    const ctrl = new AbortController();
    chatAbort.current = ctrl;
    setChatError('');
    setChatBusy(true);
    setInput('');

    setChat(prev => {
      const history = prev;
      const next: ChatTurn[] = [
        ...prev,
        { role: 'user', content: question },
        { role: 'assistant', content: '' },
      ];
      streamInsights(
        {
          mode: 'chat',
          context: contextRef.current,
          question,
          history,
          sessionId: getSessionId(),
        },
        chunk =>
          setChat(cur => {
            const copy = cur.slice();
            const last = copy[copy.length - 1];
            if (last?.role === 'assistant') {
              copy[copy.length - 1] = { ...last, content: last.content + chunk };
            }
            return copy;
          }),
        ctrl.signal
      )
        .then(() => { if (!ctrl.signal.aborted) setChatBusy(false); })
        .catch((err: Error) => {
          if (ctrl.signal.aborted) return;
          setChatError(err.message);
          setChatBusy(false);
        });
      return next;
    });
  }, [chatBusy]);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }, [chat]);

  return (
    <div className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-5 space-y-4">
      {/* ── Narrative summary ── */}
      <div className="flex items-center justify-between gap-3">
        <h3 className="font-semibold flex items-center gap-2">
          <span className="text-[var(--accent)]">✦</span>
          AI Insights — {context.label}
        </h3>
        <button
          onClick={generateSummary}
          disabled={summaryStatus === 'loading' || !ready}
          className="text-sm text-[var(--accent)] border border-[var(--border)] rounded px-3 py-1 hover:bg-[var(--row-hover)] transition-colors disabled:opacity-50"
        >
          {summaryStatus === 'loading' && summary === '' ? 'Thinking…' : 'Regenerate'}
        </button>
      </div>

      {summaryStatus === 'error' ? (
        <p className="text-sm text-red-600">{summaryError}</p>
      ) : summary === '' && summaryStatus === 'loading' ? (
        <div className="space-y-2">
          <div className="h-4 rounded bg-[var(--row-hover)] animate-pulse w-full" />
          <div className="h-4 rounded bg-[var(--row-hover)] animate-pulse w-11/12" />
          <div className="h-4 rounded bg-[var(--row-hover)] animate-pulse w-4/5" />
        </div>
      ) : (
        <p className="text-[var(--text)] leading-relaxed whitespace-pre-wrap">
          {summary || 'Select data to generate a summary.'}
          {summaryStatus === 'loading' && <span className="animate-pulse">▍</span>}
        </p>
      )}

      {/* ── Q&A ── */}
      <div className="border-t border-[var(--border)] pt-4 space-y-3">
        {chat.length === 0 && (
          <div className="flex flex-wrap gap-2">
            {SUGGESTIONS.map(s => (
              <button
                key={s}
                onClick={() => ask(s)}
                disabled={chatBusy || !ready}
                className="text-sm text-[var(--muted)] border border-[var(--border)] rounded-full px-3 py-1.5 hover:bg-[var(--row-hover)] hover:text-[var(--text)] transition-colors disabled:opacity-50"
              >
                {s}
              </button>
            ))}
          </div>
        )}

        {chat.length > 0 && (
          <div className="space-y-3 max-h-80 overflow-y-auto pr-1">
            {chat.map((turn, i) => (
              <div
                key={i}
                className={
                  turn.role === 'user'
                    ? 'ml-auto max-w-[85%] rounded-lg bg-[var(--accent)] text-white px-3 py-2 text-sm w-fit'
                    : 'max-w-[95%] text-[var(--text)] text-sm leading-relaxed whitespace-pre-wrap'
                }
              >
                {turn.content}
                {turn.role === 'assistant' &&
                  chatBusy &&
                  i === chat.length - 1 && <span className="animate-pulse">▍</span>}
              </div>
            ))}
            <div ref={chatEndRef} />
          </div>
        )}

        {chatError && <p className="text-sm text-red-600">{chatError}</p>}

        <form
          onSubmit={e => {
            e.preventDefault();
            ask(input);
          }}
          className="flex gap-2"
        >
          <input
            value={input}
            onChange={e => setInput(e.target.value)}
            placeholder="Ask anything about the data on screen…"
            disabled={!ready}
            className="flex-1 rounded-lg border border-[var(--border)] bg-[var(--bg)] px-3 py-2 text-sm placeholder:text-[var(--muted)] focus:outline-none focus:border-[var(--accent)]"
          />
          <button
            type="submit"
            disabled={chatBusy || !input.trim() || !ready}
            className="rounded-lg bg-[var(--accent)] text-white text-sm px-4 py-2 hover:opacity-90 transition-opacity disabled:opacity-50"
          >
            {chatBusy ? '…' : 'Ask'}
          </button>
        </form>
      </div>
    </div>
  );
}
