from __future__ import annotations

from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
SOURCES_DIR = ROOT / "rules" / "sources"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.fetch_probe import _classify_error_kind


def _load_one(path: Path) -> dict:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict)
    rows = doc.get("sources", [])
    assert isinstance(rows, list) and rows
    row = rows[0]
    assert isinstance(row, dict)
    return row


def test_pr16_core_source_files_exist() -> None:
    names = [
        "procurement_core_ted_cpv33100000.yaml",
        "procurement_core_pcc_medical_rss.yaml",
        "procurement_core_ungm_who_rss.yaml",
        "procurement_core_sam_api.yaml",
    ]
    for n in names:
        assert (SOURCES_DIR / n).exists(), n


def test_pr16_core_yaml_required_fields() -> None:
    for n in [
        "procurement_core_ted_cpv33100000.yaml",
        "procurement_core_pcc_medical_rss.yaml",
        "procurement_core_ungm_who_rss.yaml",
        "procurement_core_sam_api.yaml",
    ]:
        row = _load_one(SOURCES_DIR / n)
        assert str(row.get("id", "")).strip()
        assert str(row.get("url", "")).strip()
        assert str(row.get("source_group", "")).strip() == "procurement"
        fetch = row.get("fetch", {})
        assert isinstance(fetch, dict)
        assert str(fetch.get("mode", "")).strip()


def test_pr16_ted_url_has_cpv_33100000_1() -> None:
    row = _load_one(SOURCES_DIR / "procurement_core_ted_cpv33100000.yaml")
    url = str(row.get("url", ""))
    assert "cpv=33100000-1" in url


def test_pr16_pcc_url_has_encoded_medical_keyword() -> None:
    row = _load_one(SOURCES_DIR / "procurement_core_pcc_medical_rss.yaml")
    url = str(row.get("url", ""))
    assert "keyword=%E9%86%AB%E7%99%82" in url


def test_pr16_ungm_url_has_business_unit_14() -> None:
    row = _load_one(SOURCES_DIR / "procurement_core_ungm_who_rss.yaml")
    url = str(row.get("url", ""))
    assert "BusinessUnitIds=14" in url


def test_pr16_sam_key_placeholder_classified_as_needs_api_key() -> None:
    row = _load_one(SOURCES_DIR / "procurement_core_sam_api.yaml")
    url = str(row.get("url", ""))
    kind, _ = _classify_error_kind(
        {"url": url, "fetch": {"mode": "api_json"}},
        {"error_type": "needs_api_key", "error_message": "missing key"},
    )
    assert "YOUR_KEY" in url
    assert kind == "needs_api_key"
