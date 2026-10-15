import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "student-events")

HDFS_NAMENODE_HOST = os.getenv("HDFS_NAMENODE_HOST", "localhost")
HDFS_NAMENODE_PORT = int(os.getenv("HDFS_NAMENODE_PORT", "9870"))
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_RAW_PATH = os.getenv("HDFS_RAW_PATH", "/student-data/raw")
HDFS_PROCESSED_PATH = os.getenv("HDFS_PROCESSED_PATH", "/student-data/processed")
HDFS_SCORES_PATH = os.getenv("HDFS_SCORES_PATH", "/student-data/scores")

LOCAL_HDFS_MIRROR = ROOT / "storage" / "hdfs_mirror"

RISK_THRESHOLD = float(os.getenv("RISK_THRESHOLD", "0.6"))

DATASET_PATH = ROOT / "data" / "raw" / "dataset.csv"
MODEL_PATH = ROOT / "models" / "dropout_classifier.joblib"
FEATURES_PATH = ROOT / "models" / "feature_config.json"
SCORES_DB_PATH = ROOT / "storage" / "weekly_scores.json"

DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8080"))

FRONTEND_DIST = ROOT / "frontend" / "dist"
CORS_ORIGINS = os.getenv(
    "CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173"
).split(",")

WEEKLY_SCHEDULER_ENABLED = os.getenv("WEEKLY_SCHEDULER_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
)
# If > 0, run scoring on this interval (demo). If 0, use weekly cron below.
SCORING_INTERVAL_MINUTES = int(os.getenv("SCORING_INTERVAL_MINUTES", "0"))
SCORING_CRON_DAY = os.getenv("SCORING_CRON_DAY", "mon")
SCORING_CRON_HOUR = int(os.getenv("SCORING_CRON_HOUR", "6"))
