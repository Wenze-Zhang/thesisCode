#!/usr/bin/env python3
"""Delete benchmark test data created by the evaluation harnesses.

Anything whose name carries the '-bench-' marker is test-only:
  * ThingsBoard devices  (tenant devices with '-bench-' in the name)
  * CKAN datasets        (packages with '-bench-' in the name; deleted + purged)
  * local exporter CSVs  (EXPORT_DIR subdirs with '-bench-' in the name)

Dry-run by default; pass --apply to actually delete. Run inside the project /
fair-bridge image so CKAN, ThingsBoard and the exports volume are reachable
(set TB_HOST=http://thingsboard:8080).
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _p in (REPO_ROOT / "fair-bridge", REPO_ROOT / "simulator"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import config  # noqa: E402  (fair-bridge/config.py)
import tb_client  # noqa: E402  (simulator/tb_client.py)
import requests  # noqa: E402

MARKER = "-bench-"


# --------------------------------------------------------------------------- #
# ThingsBoard.
# --------------------------------------------------------------------------- #
def tb_bench_devices(jwt: str) -> list[tuple[str, str]]:
    headers = {"X-Authorization": f"Bearer {jwt}"}
    out: list[tuple[str, str]] = []
    page = 0
    while True:
        r = requests.get(
            f"{tb_client.TB_HOST}/api/tenant/devices",
            params={"pageSize": 200, "page": page},
            headers=headers, timeout=20,
        )
        r.raise_for_status()
        body = r.json()
        for d in body.get("data", []):
            if MARKER in d["name"]:
                out.append((d["name"], d["id"]["id"]))
        if not body.get("hasNext"):
            break
        page += 1
    return out


def tb_delete_device(jwt: str, device_id: str) -> bool:
    headers = {"X-Authorization": f"Bearer {jwt}"}
    try:
        r = requests.delete(
            f"{tb_client.TB_HOST}/api/device/{device_id}",
            headers=headers, timeout=20,
        )
        return r.status_code in (200, 204)
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# CKAN.
# --------------------------------------------------------------------------- #
def _ckan_headers() -> dict[str, str]:
    return {"Authorization": config.CKAN_API_KEY} if config.CKAN_API_KEY else {}


def ckan_bench_datasets() -> list[str]:
    base = config.CKAN_URL.rstrip("/")
    r = requests.get(
        f"{base}/api/3/action/package_list", headers=_ckan_headers(), timeout=30
    )
    r.raise_for_status()
    return [name for name in r.json().get("result", []) if MARKER in name]


def ckan_purge_dataset(name: str) -> bool:
    base = config.CKAN_URL.rstrip("/")
    headers = _ckan_headers()
    try:
        requests.post(
            f"{base}/api/3/action/package_delete", json={"id": name},
            headers=headers, timeout=30,
        )
        r = requests.post(
            f"{base}/api/3/action/dataset_purge", json={"id": name},
            headers=headers, timeout=30,
        )
        return r.status_code == 200
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# Local exporter CSV dirs.
# --------------------------------------------------------------------------- #
def export_bench_dirs() -> list[Path]:
    export_dir = Path(config.EXPORT_DIR)
    if not export_dir.exists():
        return []
    return sorted(
        p for p in export_dir.iterdir() if p.is_dir() and MARKER in p.name
    )


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Actually delete (default: dry-run preview).")
    args = ap.parse_args()

    jwt = tb_client.login()
    devices = tb_bench_devices(jwt)
    datasets = ckan_bench_datasets()
    dirs = export_bench_dirs()
    print(f"[cleanup] found {len(devices)} TB devices, {len(datasets)} CKAN "
          f"datasets, {len(dirs)} export dirs matching '{MARKER}'.")

    if not args.apply:
        for name, _ in devices[:8]:
            print(f"  device : {name}")
        for name in datasets[:8]:
            print(f"  dataset: {name}")
        for d in dirs[:8]:
            print(f"  dir    : {d.name}")
        print("[cleanup] dry-run; pass --apply to delete.")
        return 0

    deleted_devices = 0
    for index, (_name, device_id) in enumerate(devices):
        if index and index % 100 == 0:
            jwt = tb_client.login()  # refresh before the JWT expires
        if tb_delete_device(jwt, device_id):
            deleted_devices += 1
    purged = sum(1 for name in datasets if ckan_purge_dataset(name))
    removed_dirs = 0
    for d in dirs:
        try:
            shutil.rmtree(d)
            removed_dirs += 1
        except Exception:
            pass

    print(f"[cleanup] deleted {deleted_devices}/{len(devices)} TB devices, "
          f"purged {purged}/{len(datasets)} CKAN datasets, "
          f"removed {removed_dirs}/{len(dirs)} export dirs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
