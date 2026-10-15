"""WebHDFS client with local filesystem fallback for dev without Docker."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (  # noqa: E402
    HDFS_NAMENODE_HOST,
    HDFS_NAMENODE_PORT,
    HDFS_USER,
    LOCAL_HDFS_MIRROR,
)


class HdfsClient:
    def __init__(self) -> None:
        self.base = (
            f"http://{HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}/webhdfs/v1"
        )
        self.user = HDFS_USER
        self._use_local = False

    def _local_path(self, hdfs_path: str) -> Path:
        rel = hdfs_path.lstrip("/")
        return LOCAL_HDFS_MIRROR / rel

    def ping(self) -> bool:
        try:
            r = requests.get(
                f"{self.base}/?op=GETFILESTATUS&user.name={self.user}",
                timeout=3,
            )
            return r.status_code in (200, 404)
        except requests.RequestException:
            return False

    def mkdirs(self, hdfs_path: str) -> None:
        if self._use_local or not self.ping():
            self._use_local = True
            p = self._local_path(hdfs_path)
            p.mkdir(parents=True, exist_ok=True)
            return
        url = f"{self.base}{hdfs_path}?op=MKDIRS&user.name={self.user}"
        requests.put(url, timeout=30).raise_for_status()

    def append_json_line(self, hdfs_dir: str, record: dict[str, Any]) -> str:
        """Append one JSON line; returns file path used."""
        week = datetime.now(timezone.utc).strftime("%Y-%W")
        filename = f"events_{week}.jsonl"
        hdfs_file = f"{hdfs_dir.rstrip('/')}/{filename}"
        line = json.dumps(record, default=str) + "\n"

        if self._use_local or not self.ping():
            self._use_local = True
            local_file = self._local_path(hdfs_file)
            local_file.parent.mkdir(parents=True, exist_ok=True)
            with local_file.open("a", encoding="utf-8") as f:
                f.write(line)
            return str(local_file)

        self.mkdirs(hdfs_dir)
        create_url = (
            f"{self.base}{hdfs_file}?op=CREATE&overwrite=false&user.name={self.user}"
        )
        r = requests.put(create_url, allow_redirects=False, timeout=30)
        if r.status_code == 307:
            upload_url = r.headers["Location"]
            requests.put(
                upload_url,
                data=line.encode("utf-8"),
                headers={"Content-Type": "application/octet-stream"},
                timeout=30,
            ).raise_for_status()
            return hdfs_file
        if r.status_code == 409:
            append_url = (
                f"{self.base}{hdfs_file}?op=APPEND&user.name={self.user}"
            )
            r2 = requests.post(append_url, allow_redirects=False, timeout=30)
            if r2.status_code == 307:
                requests.post(
                    r2.headers["Location"],
                    data=line.encode("utf-8"),
                    timeout=30,
                ).raise_for_status()
                return hdfs_file
        r.raise_for_status()
        return hdfs_file

    def read_jsonl_dir(self, hdfs_dir: str) -> list[dict[str, Any]]:
        if self._use_local or not self.ping():
            self._use_local = True
            local_dir = self._local_path(hdfs_dir)
            if not local_dir.exists():
                return []
            rows: list[dict[str, Any]] = []
            for fp in sorted(local_dir.glob("*.jsonl")):
                with fp.open(encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            rows.append(json.loads(line))
            return rows

        list_url = (
            f"{self.base}{hdfs_dir}?op=LISTSTATUS&user.name={self.user}"
        )
        r = requests.get(list_url, timeout=30)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        files = r.json().get("FileStatuses", {}).get("FileStatus", [])
        rows = []
        for entry in files:
            name = entry.get("pathSuffix", "")
            if not name.endswith(".jsonl"):
                continue
            read_url = (
                f"{self.base}{hdfs_dir}/{name}?op=OPEN&user.name={self.user}"
            )
            content = requests.get(read_url, timeout=60).text
            for line in content.splitlines():
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    def write_json(self, hdfs_path: str, payload: Any) -> None:
        data = json.dumps(payload, indent=2, default=str).encode("utf-8")
        if self._use_local or not self.ping():
            self._use_local = True
            fp = self._local_path(hdfs_path)
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_bytes(data)
            return
        self.mkdirs(str(Path(hdfs_path).parent).replace("\\", "/"))
        url = f"{self.base}{hdfs_path}?op=CREATE&overwrite=true&user.name={self.user}"
        r = requests.put(url, allow_redirects=False, timeout=30)
        r.raise_for_status()
        if r.status_code == 307:
            requests.put(
                r.headers["Location"],
                data=data,
                headers={"Content-Type": "application/json"},
                timeout=30,
            ).raise_for_status()
