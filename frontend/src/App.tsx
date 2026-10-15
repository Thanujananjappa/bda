import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchHealth, fetchScores, refreshScores, type ScoresPayload } from "./api";

type Tab = "flagged" | "all" | "pipeline";

export default function App() {
  const [data, setData] = useState<ScoresPayload | null>(null);
  const [tab, setTab] = useState<Tab>("flagged");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [schedulerOn, setSchedulerOn] = useState(false);
  const [query, setQuery] = useState("");

  const load = useCallback(async () => {
    setError(null);
    try {
      const [scores, health] = await Promise.all([fetchScores(), fetchHealth()]);
      setData(scores);
      setSchedulerOn(health.scheduler_enabled);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Load failed");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
    const id = window.setInterval(load, 60_000);
    return () => window.clearInterval(id);
  }, [load]);

  const flagged = useMemo(() => {
    if (!data) return [];
    return data.scores
      .filter((s) => s.flagged_for_advisor)
      .sort((a, b) => b.dropout_risk_score - a.dropout_risk_score);
  }, [data]);

  const filteredAll = useMemo(() => {
    if (!data) return [];
    const q = query.trim().toLowerCase();
    let rows = [...data.scores].sort(
      (a, b) => b.dropout_risk_score - a.dropout_risk_score,
    );
    if (q) rows = rows.filter((s) => s.student_id.toLowerCase().includes(q));
    return rows.slice(0, 500);
  }, [data, query]);

  async function onRefresh() {
    setRefreshing(true);
    setError(null);
    try {
      await refreshScores();
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Refresh failed");
    } finally {
      setRefreshing(false);
    }
  }

  const threshold = data?.risk_threshold ?? 0.6;

  return (
    <div className="layout">
      <header className="top">
        <div>
          <p className="kicker">Miniproject #57 · Student Performance & Dropout Risk</p>
          <h1>Advisor dashboard</h1>
          <p className="lede">
            Kafka → HDFS ingestion, weekly classification scores, outreach when risk ≥
            {threshold.toFixed(1)}.
          </p>
        </div>
        <div className="actions">
          <button type="button" className="btn primary" onClick={onRefresh} disabled={refreshing}>
            {refreshing ? "Scoring…" : "Run weekly scoring"}
          </button>
          <span className={`pill ${schedulerOn ? "on" : "off"}`}>
            Scheduler {schedulerOn ? "ON" : "OFF"}
          </span>
        </div>
      </header>

      <section className="metrics">
        <Metric label="Flagged for follow-up" value={data?.flagged_count ?? "—"} />
        <Metric label="Students scored" value={data?.total_students ?? "—"} />
        <Metric label="Risk threshold" value={threshold.toFixed(2)} />
        <Metric label="Data source" value={data?.scoring_source ?? "—"} small />
      </section>

      {error && <div className="banner error">{error}</div>}
      {loading && <p className="muted">Loading scores…</p>}

      <nav className="tabs">
        {(
          [
            ["flagged", "Priority queue"],
            ["all", "All students"],
            ["pipeline", "Architecture"],
          ] as const
        ).map(([id, label]) => (
          <button
            key={id}
            type="button"
            className={tab === id ? "tab active" : "tab"}
            onClick={() => setTab(id)}
          >
            {label}
          </button>
        ))}
      </nav>

      <main className="panel">
        {tab === "flagged" && (
          <>
            <p className="meta">
              Last run: {data?.generated_at ?? "Not scored yet — click Run weekly scoring"}
            </p>
            <ScoreTable rows={flagged} threshold={threshold} empty="No flagged students." />
          </>
        )}

        {tab === "all" && (
          <>
            <input
              className="search"
              placeholder="Filter by student ID…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
            <p className="meta">Showing up to 500 rows (sorted by risk).</p>
            <ScoreTable rows={filteredAll} threshold={threshold} showFlag />
          </>
        )}

        {tab === "pipeline" && (
          <div className="pipeline">
            <h2>End-to-end flow</h2>
            <ol>
              <li>
                <strong>UCI dataset</strong> — baseline student profiles (
                <code>scripts/download_dataset.py</code>).
              </li>
              <li>
                <strong>Kafka producer</strong> — weekly attendance, assignment, and assessment
                events (<code>pipeline/kafka_producer.py</code>).
              </li>
              <li>
                <strong>HDFS consumer</strong> — JSONL landing zone (
                <code>pipeline/kafka_to_hdfs_consumer.py</code>).
              </li>
              <li>
                <strong>Classifier</strong> — Gradient Boosting on UCI + stream engagement
                features (<code>ml/train_model.py</code>).
              </li>
              <li>
                <strong>Weekly scoring</strong> — batch job + APScheduler cron (
                <code>ml/weekly_scoring.py</code>, <code>jobs/scheduler.py</code>).
              </li>
              <li>
                <strong>This React UI</strong> — advisor queue via FastAPI REST.
              </li>
            </ol>
            <p className="note">
              See <code>docs/PROJECT_REPORT.md</code> for scope notes (simulated LMS stream vs
              production SIS integration).
            </p>
          </div>
        )}
      </main>
    </div>
  );
}

function Metric({
  label,
  value,
  small,
}: {
  label: string;
  value: string | number;
  small?: boolean;
}) {
  return (
    <div className="metric">
      <span className="metric-label">{label}</span>
      <span className={small ? "metric-value small" : "metric-value"}>{value}</span>
    </div>
  );
}

function ScoreTable({
  rows,
  threshold,
  empty,
  showFlag,
}: {
  rows: ScoresPayload["scores"];
  threshold: number;
  empty?: string;
  showFlag?: boolean;
}) {
  if (!rows.length) {
    return <p className="muted">{empty ?? "No rows."}</p>;
  }
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Student</th>
            <th>Week</th>
            <th>Risk</th>
            {showFlag && <th>Flagged</th>}
            <th>Source</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((s) => {
            const pct = Math.round(s.dropout_risk_score * 100);
            const level =
              s.dropout_risk_score >= 0.8
                ? "critical"
                : s.dropout_risk_score >= threshold
                  ? "warn"
                  : "ok";
            return (
              <tr key={`${s.student_id}-${s.week}`} className={level}>
                <td>
                  <code>{s.student_id}</code>
                </td>
                <td>{s.week}</td>
                <td>
                  <div className="bar-cell">
                    <div className="bar-track">
                      <div className="bar-fill" style={{ width: `${pct}%` }} />
                    </div>
                    <span>{s.dropout_risk_score.toFixed(2)}</span>
                  </div>
                </td>
                {showFlag && (
                  <td>{s.flagged_for_advisor ? "Yes" : "No"}</td>
                )}
                <td className="source">{s.source}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
