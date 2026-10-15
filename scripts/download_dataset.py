"""Download UCI Predict Students' Dropout and Academic Success dataset."""

from __future__ import annotations

import sys
import zipfile
from io import BytesIO
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "raw" / "dataset.csv"

URL = "https://archive.ics.uci.edu/static/public/697/predict+students+dropout+and+academic+success.zip"


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    if OUT.exists() and OUT.stat().st_size > 1000:
        print(f"Dataset already present: {OUT}")
        return

    print("Downloading UCI dataset...")
    resp = requests.get(URL, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(BytesIO(resp.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("No CSV found in UCI zip")
        # Prefer training file name from UCI bundle
        pick = next(
            (n for n in csv_names if "dataset" in n.lower()),
            csv_names[0],
        )
        OUT.write_bytes(zf.read(pick))

    print(f"Saved {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Download failed: {e}", file=sys.stderr)
        sys.exit(1)
