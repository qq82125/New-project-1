# Opportunity Index

## Scope
Opportunity Index is an additive data layer. It does not change A-G logic, relevance, model routing, or analysis cache policy.

## Signal Schema
Each line in `artifacts/opportunity/opportunity_signals-YYYYMMDD.jsonl` includes:
- `date`
- `region`
- `lane`
- `event_type`
- `weight`
- `source_id`
- `url_norm`
- `signal_key`
- `observed_at`
- `run_id` (optional)
- `interval_source` (optional)

Unknown normalization:
- empty/None -> `__unknown__`

Probe filtering:
- if `region` or `lane` starts with `__window_probe__`, signal is dropped.

## Dedup Strategy
Same-day dedup key:
- `signal_key = sha1("{date}|{url_norm}|{event_type}|{region}|{lane}")`

Behavior:
- first write: `written=1, deduped=0`
- duplicate write: `written=0, deduped=1`
- probe drop: `dropped_probe=1`

Dedup combines:
- in-memory seen set for current process
- tail scan of current day file (`tail_lines_scan`, default 2000) for cross-run/process compatibility

## H Section Explain
H section prints top opportunities with:
- direction delta (`▲/▼/→`)
- `score`
- `contrib` top2: `event_type=weight_sum (count)`

If filtered list is insufficient and unknown pairs are backfilled, item is marked `[LOW_CONF]`.

## Unknown KPIs
Computed in window:
- `unknown_region_rate`
- `unknown_lane_rate`
- `unknown_event_type_rate`

Recommended targets:
- `unknown_region_rate < 0.2`
- `unknown_lane_rate < 0.2`

## Runtime Controls
`content_rules.defaults.opportunity_index` supports:
- `enabled`
- `window_days`
- `dedupe.enabled`
- `dedupe.tail_lines_scan`
- `display.top_n`
- `display.suppress_unknown_both`
- `display.unknown_min_score`
- `display.mark_low_conf`
