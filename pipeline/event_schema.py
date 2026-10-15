"""Map UCI rows to streaming attendance / assignment / assessment events."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

# Column names in UCI CSV (semicolon-separated)
TARGET_COL = "Target"


def _get(row: dict[str, str], key: str, default: str = "0") -> str:
    if key in row:
        return row[key]
    key_stripped = key.strip()
    for k, v in row.items():
        if k.strip() == key_stripped:
            return v
    return default


def _attendance_flag(row: dict[str, str]) -> int:
    raw = _get(row, "Daytime/evening attendance", "1")
    return int(float(raw or 1))


def student_id_from_row(row: dict[str, str], index: int) -> str:
    return f"STU-{index:05d}"


def base_profile(row: dict[str, str], index: int) -> dict[str, Any]:
    return {
        "student_id": student_id_from_row(row, index),
        "target_label": _get(row, TARGET_COL, "Unknown").strip(),
        "marital_status": _get(row, "Marital status"),
        "application_mode": _get(row, "Application mode"),
        "course": _get(row, "Course"),
        "previous_qualification_grade": float(
            _get(row, "Previous qualification (grade)", "0") or 0
        ),
        "admission_grade": float(_get(row, "Admission grade", "0") or 0),
        "age_at_enrollment": int(float(_get(row, "Age at enrollment", "18") or 18)),
        "daytime_attendance": _attendance_flag(row),
    }


def weekly_events_for_student(
    row: dict[str, str],
    index: int,
    week: int,
    rng: random.Random,
) -> list[dict[str, Any]]:
    """Simulate one week of Kafka events from static UCI features."""
    profile = base_profile(row, index)
    sid = profile["student_id"]
    ts = datetime.now(timezone.utc) - timedelta(weeks=52 - week)

    # Attendance: daytime=1 higher attendance rate
    base_rate = 0.92 if profile["daytime_attendance"] == 1 else 0.78
    if profile["target_label"] == "Dropout":
        base_rate -= 0.15
    attendance_pct = max(0.35, min(1.0, base_rate + rng.uniform(-0.08, 0.08)))

    sem = 1 if week <= 26 else 2
    prefix = f"Curricular units {sem}{'st' if sem == 1 else 'nd'} sem"
    enrolled = float(_get(row, f"{prefix} (enrolled)", "0") or 0)
    approved = float(_get(row, f"{prefix} (approved)", "0") or 0)
    grade = float(_get(row, f"{prefix} (grade)", "0") or 0)
    evaluations = float(_get(row, f"{prefix} (evaluations)", "0") or 0)

    progress = min(1.0, week / 52.0)
    assignment_score = (
        (approved / enrolled * 100) if enrolled > 0 else grade
    ) * (0.5 + 0.5 * progress) + rng.uniform(-5, 5)
    assignment_score = max(0, min(100, assignment_score))

    assessment_score = grade * (0.4 + 0.6 * progress) + rng.uniform(-3, 3)
    assessment_score = max(0, min(20, assessment_score))

    common = {
        "student_id": sid,
        "week": week,
        "timestamp": ts.isoformat(),
        "target_label": profile["target_label"],
    }

    return [
        {
            **common,
            "event_type": "attendance",
            "attendance_rate": round(attendance_pct, 4),
            "sessions_held": 5,
            "sessions_attended": int(round(5 * attendance_pct)),
        },
        {
            **common,
            "event_type": "assignment",
            "assignments_due": max(1, int(evaluations / 8) or 1),
            "assignments_submitted": max(
                0, int((approved / max(enrolled, 1)) * 3) + rng.randint(0, 1)
            ),
            "avg_assignment_score": round(assignment_score, 2),
        },
        {
            **common,
            "event_type": "assessment",
            "assessments_taken": 1 if week % 4 == 0 else 0,
            "latest_exam_score": round(assessment_score, 2),
            "cumulative_gpa_proxy": round(assessment_score / 20 * 4, 2),
        },
        {
            **common,
            "event_type": "profile_snapshot",
            **{k: v for k, v in profile.items() if k != "target_label"},
        },
    ]
