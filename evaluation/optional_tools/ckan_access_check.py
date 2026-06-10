#!/usr/bin/env python3
"""Optional CKAN access check.

This script is not part of the thesis performance evaluation. It measures CKAN
catalogue/resource access behaviour, not FAIR Bridge telemetry processing.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RESULTS_DIR = REPO_ROOT / "evaluation" / "results"

RESULT_COLUMNS = [
    "operation",
    "attempt",
    "success",
    "status_code",
    "elapsed_ms",
    "error",
    "resource_url",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return round(ordered[int(rank)], 6)
    weight = rank - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 6)


def _headers(api_key: str | None) -> dict[str, str]:
    headers = {"User-Agent": "fair-bridge-optional-ckan-access-check/1.0"}
    if api_key:
        headers["Authorization"] = api_key
    return headers


def _action_url(ckan_url: str, action: str) -> str:
    return f"{ckan_url.rstrip('/')}/api/action/{action}"


def _request_action(
    *,
    ckan_url: str,
    action: str,
    params: dict[str, Any],
    api_key: str | None,
    timeout_s: int = 15,
) -> tuple[bool, int | None, float, dict[str, Any] | None, str]:
    start = time.perf_counter()
    try:
        response = requests.get(
            _action_url(ckan_url, action),
            params=params,
            headers=_headers(api_key),
            timeout=timeout_s,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        try:
            payload = response.json()
        except ValueError:
            payload = None
        success = response.status_code == 200 and bool(payload and payload.get("success"))
        error = "" if success else str((payload or {}).get("error", response.text[:200]))
        return success, response.status_code, elapsed_ms, payload, error
    except requests.RequestException as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return False, None, elapsed_ms, None, str(exc)


def _find_csv_resource_url(
    ckan_url: str,
    dataset_id: str,
    api_key: str | None,
) -> str | None:
    success, _status, _elapsed, payload, _error = _request_action(
        ckan_url=ckan_url,
        action="package_show",
        params={"id": dataset_id},
        api_key=api_key,
    )
    if not success or not payload:
        return None
    resources = (payload.get("result") or {}).get("resources") or []
    for resource in resources:
        url = resource.get("url")
        if not url:
            continue
        fmt = str(resource.get("format") or "").lower()
        mimetype = str(resource.get("mimetype") or "").lower()
        name = str(resource.get("name") or "").lower()
        if "csv" in fmt or "csv" in mimetype or name.endswith(".csv"):
            return urljoin(ckan_url.rstrip("/") + "/", url)
    return None


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=RESULT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {"generated_at": _now_iso(), "operations": {}}
    for operation in sorted({row["operation"] for row in rows}):
        operation_rows = [row for row in rows if row["operation"] == operation]
        successes = [row for row in operation_rows if row["success"]]
        latencies = [float(row["elapsed_ms"]) for row in successes]
        summary["operations"][operation] = {
            "attempts": len(operation_rows),
            "success_count": len(successes),
            "success_rate": (
                round(len(successes) / len(operation_rows), 6)
                if operation_rows
                else None
            ),
            "mean_ms": round(statistics.mean(latencies), 6) if latencies else None,
            "p50_ms": _percentile(latencies, 50),
            "p95_ms": _percentile(latencies, 95),
            "max_ms": round(max(latencies), 6) if latencies else None,
        }
    return summary


def run(args: argparse.Namespace) -> tuple[Path, Path]:
    rows: list[dict[str, Any]] = []
    csv_resource_url = _find_csv_resource_url(args.ckan_url, args.dataset_id, args.api_key)

    for attempt in range(1, args.repeat + 1):
        operations = [
            ("package_search", "package_search", {"q": args.dataset_id}, ""),
            ("package_show", "package_show", {"id": args.dataset_id}, ""),
            ("resource_listing_via_package_show", "package_show", {"id": args.dataset_id}, ""),
        ]
        for operation, action, params, resource_url in operations:
            success, status_code, elapsed_ms, _payload, error = _request_action(
                ckan_url=args.ckan_url,
                action=action,
                params=params,
                api_key=args.api_key,
            )
            rows.append(
                {
                    "operation": operation,
                    "attempt": attempt,
                    "success": success,
                    "status_code": status_code,
                    "elapsed_ms": round(elapsed_ms, 6),
                    "error": error,
                    "resource_url": resource_url,
                }
            )

        if csv_resource_url:
            start = time.perf_counter()
            status_code = None
            error = ""
            success = False
            try:
                response = requests.get(
                    csv_resource_url,
                    headers=_headers(args.api_key),
                    timeout=20,
                    stream=True,
                )
                status_code = response.status_code
                for _chunk in response.iter_content(chunk_size=8192):
                    break
                success = 200 <= response.status_code < 400
                if not success:
                    error = response.text[:200]
            except requests.RequestException as exc:
                error = str(exc)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            rows.append(
                {
                    "operation": "csv_resource_download",
                    "attempt": attempt,
                    "success": success,
                    "status_code": status_code,
                    "elapsed_ms": round(elapsed_ms, 6),
                    "error": error,
                    "resource_url": csv_resource_url,
                }
            )

    results_dir = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = results_dir / "optional_ckan_access_check.csv"
    summary_path = results_dir / "optional_ckan_access_check_summary.json"
    _write_csv(csv_path, rows)

    summary = _summarize(rows)
    summary["ckan_url"] = args.ckan_url
    summary["dataset_id"] = args.dataset_id
    summary["csv_resource_url"] = csv_resource_url
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, sort_keys=True)
        fh.write("\n")

    print(f"Wrote {csv_path}")
    print(f"Wrote {summary_path}")
    if not csv_resource_url:
        print("No CSV resource URL found; skipped csv_resource_download operation.")
    return csv_path, summary_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Optional CKAN catalogue/resource access check; not part of the "
            "main FAIR Bridge performance evaluation."
        )
    )
    parser.add_argument("--ckan-url", default="http://localhost:5000")
    parser.add_argument("--dataset-id", default="sensor-example")
    parser.add_argument("--repeat", type=int, default=30)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    args = parser.parse_args()

    if args.repeat <= 0:
        raise SystemExit("--repeat must be positive")
    run(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
