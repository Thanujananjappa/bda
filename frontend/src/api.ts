export type StudentScore = {
  student_id: string;
  week: number;
  dropout_risk_score: number;
  flagged_for_advisor: boolean;
  source: string;
};

export type ScoresPayload = {
  generated_at: string | null;
  risk_threshold: number;
  total_students: number;
  flagged_count: number;
  scoring_source?: string | null;
  scores: StudentScore[];
};

const base = import.meta.env.VITE_API_BASE ?? "";

export async function fetchScores(): Promise<ScoresPayload> {
  const res = await fetch(`${base}/api/scores`);
  if (!res.ok) throw new Error("Failed to load scores");
  return res.json();
}

export async function refreshScores(): Promise<{
  ok: boolean;
  flagged_count: number;
  scoring_source?: string;
  generated_at?: string;
}> {
  const res = await fetch(`${base}/api/scores/refresh`, { method: "POST" });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail ?? "Refresh failed");
  }
  return res.json();
}

export async function fetchHealth(): Promise<{ status: string; scheduler_enabled: boolean }> {
  const res = await fetch(`${base}/api/health`);
  if (!res.ok) throw new Error("API unreachable");
  return res.json();
}
