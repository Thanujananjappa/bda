"""Score each student weekly; flag dropout risk > threshold for advisors."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import (  # noqa: E402
    DATASET_PATH,
    FEATURES_PATH,
    HDFS_RAW_PATH,
    HDFS_SCORES_PATH,
    MODEL_PATH,
    RISK_THRESHOLD,
    SCORES_DB_PATH,
)
from ml.features import (  # noqa: E402
    aggregate_events,
    align_feature_columns,
    feature_matrix_from_uci,
    load_feature_config,
    load_uci_dataframe,
    stream_features_table_from_uci,
)
from pipeline.hdfs_client import HdfsClient  # noqa: E402


def run_weekly_scoring(week: int = 0) -> dict:
    """Compute scores from HDFS/Kafka events when present, else UCI stream simulation."""
    if not MODEL_PATH.exists():
        raise FileNotFoundError("Train first: python ml/train_model.py")

    model = joblib.load(MODEL_PATH)
    feature_columns = load_feature_config(FEATURES_PATH)
    df = load_uci_dataframe(DATASET_PATH)

    hdfs = HdfsClient()
    events = hdfs.read_jsonl_dir(HDFS_RAW_PATH)

    if events:
        stream_agg = aggregate_events(events)
        source = "kafka_hdfs"
    else:
        target_week = week or 52
        stream_agg = stream_features_table_from_uci(df, week=target_week)
        source = "uci_stream_simulation"

    X, _, student_ids, report_week = feature_matrix_from_uci(df, stream_agg=stream_agg)
    X = align_feature_columns(X, feature_columns)
    proba = model.predict_proba(X)[:, 1]

    scores = pd.DataFrame(
        {
            "student_id": student_ids.values,
            "week": report_week.values.astype(int),
            "dropout_risk_score": proba.round(4),
            "flagged_for_advisor": proba >= RISK_THRESHOLD,
            "source": source,
        }
    )

    flagged = scores[scores["flagged_for_advisor"]].sort_values(
        "dropout_risk_score", ascending=False
    )
    week_label = int(scores["week"].max())
    print(
        f"Week {week_label} — {len(flagged)} students flagged (>={RISK_THRESHOLD}) "
        f"[{source}]"
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "risk_threshold": RISK_THRESHOLD,
        "total_students": len(scores),
        "flagged_count": int(flagged.shape[0]),
        "scoring_source": source,
        "scores": scores.to_dict(orient="records"),
    }

    SCORES_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCORES_DB_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    hdfs_path = f"{HDFS_SCORES_PATH}/weekly_scores_w{week_label}.json"
    hdfs.write_json(hdfs_path, payload)
    print(f"Wrote dashboard store -> {SCORES_DB_PATH}")
    print(f"Wrote HDFS scores -> {hdfs_path}")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", type=int, default=0, help="Report week (0=auto)")
    args = parser.parse_args()
    run_weekly_scoring(week=args.week)


if __name__ == "__main__":
    main()
