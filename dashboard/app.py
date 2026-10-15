"""Advisor API + React frontend for weekly dropout risk scores."""

from __future__ import annotations

import json
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import (  # noqa: E402
    CORS_ORIGINS,
    DASHBOARD_HOST,
    DASHBOARD_PORT,
    FRONTEND_DIST,
    RISK_THRESHOLD,
    SCORES_DB_PATH,
    WEEKLY_SCHEDULER_ENABLED,
)
from jobs.scheduler import start_scheduler, stop_scheduler  # noqa: E402
from ml.weekly_scoring import run_weekly_scoring  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard")


def _load_scores() -> dict:
    if not SCORES_DB_PATH.exists():
        return {
            "generated_at": None,
            "risk_threshold": RISK_THRESHOLD,
            "total_students": 0,
            "flagged_count": 0,
            "scoring_source": None,
            "scores": [],
        }
    return json.loads(SCORES_DB_PATH.read_text(encoding="utf-8"))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(
    title="Student Dropout Risk — Advisor Platform",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in CORS_ORIGINS if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict:
    return {
        "status": "ok",
        "scheduler_enabled": WEEKLY_SCHEDULER_ENABLED,
        "frontend_built": FRONTEND_DIST.exists(),
    }


@app.get("/api/scores")
def api_scores() -> dict:
    return _load_scores()


@app.get("/api/flagged")
def api_flagged() -> dict:
    data = _load_scores()
    flagged = [s for s in data["scores"] if s.get("flagged_for_advisor")]
    flagged.sort(key=lambda x: x["dropout_risk_score"], reverse=True)
    return {
        "risk_threshold": data.get("risk_threshold", RISK_THRESHOLD),
        "flagged_count": len(flagged),
        "scoring_source": data.get("scoring_source"),
        "generated_at": data.get("generated_at"),
        "students": flagged,
    }


@app.post("/api/scores/refresh")
def api_refresh_scores() -> dict:
    try:
        payload = run_weekly_scoring()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Manual scoring failed")
        raise HTTPException(status_code=500, detail="Scoring job failed") from exc
    return {
        "ok": True,
        "flagged_count": payload.get("flagged_count"),
        "scoring_source": payload.get("scoring_source"),
        "generated_at": payload.get("generated_at"),
    }


if FRONTEND_DIST.is_dir() and (FRONTEND_DIST / "index.html").is_file():
    assets_dir = FRONTEND_DIST / "assets"
    if assets_dir.is_dir():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/")
    def spa_root() -> FileResponse:
        return FileResponse(FRONTEND_DIST / "index.html")

    @app.get("/{full_path:path}")
    def spa_fallback(full_path: str) -> FileResponse:
        if full_path.startswith("api"):
            raise HTTPException(status_code=404, detail="Not found")
        candidate = FRONTEND_DIST / full_path
        if candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(FRONTEND_DIST / "index.html")
else:
    logger.warning(
        "React build not found at %s — run: cd frontend && npm install && npm run build",
        FRONTEND_DIST,
    )


def main() -> None:
    uvicorn.run(
        "dashboard.app:app",
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        reload=False,
    )


if __name__ == "__main__":
    main()
