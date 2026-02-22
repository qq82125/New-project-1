# Procurement Core Sources

This document defines the production-preferred procurement source pack for PR16.

## Core Source Pack (RSS/API-first)

- `procurement_core_ted_cpv33100000`
  - EU TED RSS, CPV `33100000-1` (medical devices)
  - mode: `rss`
- `procurement_core_pcc_medical_rss`
  - Taiwan PCC tender RSS with medical keyword
  - mode: `rss`
- `procurement_core_ungm_who_rss`
  - UNGM RSS for WHO-related notices (`BusinessUnitIds=14`)
  - mode: `rss`
- `procurement_core_sam_api`
  - SAM.gov Opportunities API (requires API key)
  - mode: `api_json`
  - default: disabled

## Validation Workflow

1. Dry probe (no asset write):
   - `python3 -m app.workers.cli procurement-probe --force true --write-assets 0`
2. Gray rollout:
   - `python3 -m app.workers.cli procurement-probe --force true --write-assets 1`
3. Promote stable sources in `/admin/sources` after repeated successful probes.

## Key Handling

- `procurement_core_sam_api` keeps `YOUR_KEY` placeholder in repo.
- Inject real key via DB override or environment-based auth wiring.
- `procurement-probe` should classify missing key as `needs_api_key`.

