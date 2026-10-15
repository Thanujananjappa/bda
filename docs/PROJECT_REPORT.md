# Miniproject Report — Student Performance & Dropout Risk Prediction

## Problem alignment

| Requirement | Implementation |
|-------------|----------------|
| Ingest attendance, assignment, assessment via **Kafka** | `pipeline/kafka_producer.py`, topic `student-events`, event types in `pipeline/event_schema.py` |
| Persist on **HDFS** | `pipeline/kafka_to_hdfs_consumer.py`, WebHDFS client `pipeline/hdfs_client.py`, paths under `/student-data/raw` |
| **Classification** model | `GradientBoostingClassifier` in `ml/train_model.py` (UCI + stream engagement features) |
| **Weekly** dropout-risk score per student | `ml/weekly_scoring.py`, output `storage/weekly_scores.json` + HDFS `/student-data/scores/` |
| Flag risk **> 0.6** for advisors | `RISK_THRESHOLD`, field `flagged_for_advisor`, UI queue |
| **UCI** dataset | `scripts/download_dataset.py` (ID 697) |
| **Advisor dashboard** | React SPA (`frontend/`) + FastAPI backend (`dashboard/app.py`) |
| Automated weekly runs | APScheduler in `jobs/scheduler.py` (cron Monday 06:00 UTC, or interval for demos) |

## Architecture

```
UCI CSV → Kafka Producer → Kafka → HDFS Consumer → HDFS raw JSONL
                ↓
         Train classifier (UCI + stream features)
                ↓
         Weekly scoring job (reads HDFS events or simulates week)
                ↓
         FastAPI REST + React advisor UI
```

## Scope notes (for viva / report)

1. **LMS integration** — Events are generated from UCI rows using the same rules as a campus LMS would publish (attendance %, assignment submission, exam scores). Replacing the producer with a real SIS/LMS connector is a deployment change, not a new pipeline stage.
2. **Model features** — Training and scoring both use **static UCI attributes plus stream metrics** (`attendance_rate`, `avg_assignment_score`, etc.). When Kafka/HDFS has data, scores use live aggregates; otherwise the pipeline uses deterministic week-52 simulation from UCI.
3. **Scheduling** — Production would use Airflow/cron on a cluster; this project uses **APScheduler** inside the API process (and documents equivalent cron in README).
4. **Frontend** — Separate **React (Vite)** client; backend is **FastAPI** JSON API only.

## Demo checklist

- [ ] Docker: Kafka + HDFS up (`docker compose up -d`)
- [ ] Producer + consumer run
- [ ] `python ml/train_model.py` and `python ml/weekly_scoring.py`
- [ ] `cd frontend && npm run build` then `python dashboard/app.py`
- [ ] Open http://localhost:8080 — flagged queue visible
