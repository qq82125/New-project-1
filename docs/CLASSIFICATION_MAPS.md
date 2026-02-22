# Classification Maps

## Purpose
This layer reduces `__unknown__` for `region` and `lane` before opportunity signals are written.

## Files
- `rules/mappings/region_map.v1.yaml`
- `rules/mappings/lane_map.v1.yaml`

## How To Maintain
Region map:
- Add host-level overrides under `domain_contains` for high-confidence domains.
- Add TLD-level fallbacks under `domain_suffix` for broad defaults.

Lane map:
- Add keywords to `lanes.<lane>.any`.
- Matching is lower-cased substring match over title/summary/evidence text.
- First matched lane wins (order matters).

## KPI Targets
- `unknown_region_rate < 0.20`
- `unknown_lane_rate < 0.30`

## Triage Flow
1. Check `unknown_region_top_domains` from H/opportunity KPI output.
2. Add/adjust `region_map` entries for recurring unknown hosts.
3. Check `unknown_lane_top_terms` (source_id based breakdown).
4. Add lane keywords or adjust source grouping/trust/interval for noisy feeds.
