"""Feature engineering from UCI CSV and aggregated streaming events."""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from pipeline.event_schema import weekly_events_for_student

STREAM_METRIC_COLUMNS = (
    "attendance_rate",
    "avg_assignment_score",
    "assignment_submit_ratio",
    "latest_exam_score",
    "cumulative_gpa_proxy",
)


def load_uci_dataframe(path: Path) -> pd.DataFrame:
    with path.open(encoding="utf-8") as f:
        sample = f.read(4096)
        f.seek(0)
        delimiter = ";" if sample.count(";") > sample.count(",") else ","
        df = pd.read_csv(f, delimiter=delimiter)
    df.columns = [c.strip() for c in df.columns]
    return df


def stream_features_table_from_uci(
    df: pd.DataFrame, week: int = 52, seed: int = 42
) -> pd.DataFrame:
    """Build attendance / assignment / assessment features (same logic as Kafka events)."""
    rng = random.Random(seed)
    rows: list[dict[str, Any]] = []
    for idx, row in enumerate(df.to_dict(orient="records"), start=1):
        events = weekly_events_for_student(row, idx, week, rng)
        agg = aggregate_events(events)
        if not agg.empty:
            rows.append(agg.iloc[0].to_dict())
    return pd.DataFrame(rows)


def feature_matrix_from_uci(
    df: pd.DataFrame,
    stream_agg: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.Series | None, pd.Series, pd.Series]:
    """
    Build model matrix: UCI demographics + stream metrics (attendance, assignments, assessments).
    Returns X, y (None when scoring), student_id, report_week.
    """
    work = df.copy()
    work.columns = [c.strip() for c in work.columns]
    work["student_id"] = [f"STU-{i:05d}" for i in range(1, len(work) + 1)]
    report_week = pd.Series(52, index=work.index, dtype=int)

    if stream_agg is not None and not stream_agg.empty:
        sa = stream_agg.copy()
        merge_cols = ["student_id"] + [
            c for c in sa.columns if c in STREAM_METRIC_COLUMNS or c == "week"
        ]
        sa = sa[merge_cols].drop_duplicates(subset=["student_id"], keep="last")
        work = work.merge(sa, on="student_id", how="left", suffixes=("", "_stream"))
        if "week" in work.columns:
            report_week = work["week"].fillna(52).astype(int)
            work = work.drop(columns=["week"])
        for col in STREAM_METRIC_COLUMNS:
            if col in work.columns:
                fill = float(work[col].median()) if work[col].notna().any() else 0.0
                work[col] = work[col].fillna(fill)

    y: pd.Series | None = None
    if "Target" in work.columns:
        y = (work["Target"] == "Dropout").astype(int)

    student_ids = work["student_id"].copy()
    drop_cols = {"Target", "student_id"}
    X = work.drop(columns=[c for c in drop_cols if c in work.columns])
    X = pd.get_dummies(X, drop_first=True)
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0)
    return X, y, student_ids, report_week


def uci_training_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Binary target: 1 = Dropout, 0 = Graduate or Enrolled (static UCI only)."""
    X, y, _, _ = feature_matrix_from_uci(df, stream_agg=None)
    assert y is not None
    return X, y


def aggregate_events(events: list[dict[str, Any]]) -> pd.DataFrame:
    """Latest-week features per student from Kafka/HDFS JSONL."""
    if not events:
        return pd.DataFrame()

    df = pd.DataFrame(events)
    if df.empty:
        return df

    latest_week = df.groupby("student_id")["week"].transform("max")
    df = df[df["week"] == latest_week]

    rows = []
    for sid, grp in df.groupby("student_id"):
        row: dict[str, Any] = {"student_id": sid, "week": int(grp["week"].max())}
        att = grp[grp["event_type"] == "attendance"]
        asn = grp[grp["event_type"] == "assignment"]
        asm = grp[grp["event_type"] == "assessment"]
        prof = grp[grp["event_type"] == "profile_snapshot"]

        if not att.empty:
            row["attendance_rate"] = float(att.iloc[-1]["attendance_rate"])
        if not asn.empty:
            row["avg_assignment_score"] = float(asn.iloc[-1]["avg_assignment_score"])
            row["assignment_submit_ratio"] = float(
                asn.iloc[-1]["assignments_submitted"]
            ) / max(1, float(asn.iloc[-1]["assignments_due"]))
        if not asm.empty:
            row["latest_exam_score"] = float(asm.iloc[-1]["latest_exam_score"])
            row["cumulative_gpa_proxy"] = float(asm.iloc[-1]["cumulative_gpa_proxy"])
        if not prof.empty:
            p = prof.iloc[-1]
            for k in (
                "admission_grade",
                "previous_qualification_grade",
                "age_at_enrollment",
                "daytime_attendance",
            ):
                if k in p:
                    row[k] = p[k]
        rows.append(row)

    out = pd.DataFrame(rows)
    return out.fillna(0)


def align_feature_columns(X: pd.DataFrame, training_columns: list[str]) -> pd.DataFrame:
    for col in training_columns:
        if col not in X.columns:
            X[col] = 0
    return X[training_columns]


def save_feature_config(path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"feature_columns": columns}, indent=2), encoding="utf-8")


def load_feature_config(path: Path) -> list[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return list(data["feature_columns"])
