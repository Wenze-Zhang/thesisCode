from __future__ import annotations

import csv
from pathlib import Path


REQUIRED_COLUMNS = [
    "id",
    "dimension",
    "requirement_or_claim",
    "test_case",
    "input_description",
    "expected_output",
    "evidence_file_or_method",
    "automated",
]

REQUIRED_IDS = {f"T{idx}" for idx in range(1, 15)}


def test_functional_matrix_contains_required_cases():
    matrix_path = Path(__file__).resolve().parents[1] / "evaluation" / "functional_test_matrix.csv"

    with matrix_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    assert reader.fieldnames == REQUIRED_COLUMNS
    assert {row["id"] for row in rows}.issuperset(REQUIRED_IDS)
    assert all(row["automated"] in {"yes", "no", "partial"} for row in rows)
