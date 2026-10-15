"""Train dropout risk classifier on UCI + stream-style engagement features."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DATASET_PATH, FEATURES_PATH, MODEL_PATH  # noqa: E402
from ml.features import (  # noqa: E402
    align_feature_columns,
    feature_matrix_from_uci,
    load_uci_dataframe,
    save_feature_config,
    stream_features_table_from_uci,
)


def main() -> None:
    if not DATASET_PATH.exists():
        print("Run: python scripts/download_dataset.py")
        sys.exit(1)

    df = load_uci_dataframe(DATASET_PATH)
    stream_feats = stream_features_table_from_uci(df, week=52)
    X, y, _, _ = feature_matrix_from_uci(df, stream_agg=stream_feats)
    assert y is not None
    columns = list(X.columns)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = Pipeline(
        steps=[
            ("scale", StandardScaler(with_mean=False)),
            (
                "clf",
                GradientBoostingClassifier(
                    random_state=42,
                    n_estimators=200,
                    learning_rate=0.08,
                    max_depth=4,
                ),
            ),
        ]
    )
    model.fit(X_train, y_train)

    X_test = align_feature_columns(X_test, columns)
    proba = model.predict_proba(X_test)[:, 1]
    preds = (proba >= 0.5).astype(int)
    auc = roc_auc_score(y_test, proba)
    print(classification_report(y_test, preds, target_names=["Retain", "Dropout"]))
    print(f"ROC-AUC: {auc:.4f}")

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    save_feature_config(FEATURES_PATH, columns)

    meta = {
        "model": str(MODEL_PATH.name),
        "roc_auc_holdout": round(float(auc), 4),
        "positive_class": "dropout",
        "training_rows": len(X),
        "includes_stream_features": True,
        "stream_metrics": list(stream_feats.columns),
    }
    (MODEL_PATH.parent / "training_meta.json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )
    print(f"Saved model -> {MODEL_PATH}")


if __name__ == "__main__":
    main()
