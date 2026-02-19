# Enhanced 评分与强去重

本模块仅在 `profile=enhanced` 且 `SCORING_DEDUPE_ENABLED=true` 时启用。
`legacy` 完全不受影响。

## 1. 能力目标
- 为每条候选项计算：`evidence_grade`、`source_weight`、`signal_level`、`quality_score`、`dedupe_key`。
- 做跨信源强去重，保留 canonical 条目，并保存 `other_sources` 与 `deduped_from_ids`。
- 在最终入选阶段使用多样性配额，防止 media/aggregator 淹没监管、期刊、公司官方信息。
- dry-run/实跑都输出统计与 explain（artifact + stderr）。

## 2. 证据等级
- A：监管/官方/标准组织，或正式期刊原文。
- B：公司官方 IR/Press，或预印本原文。
- C：垂直媒体/行业媒体可追溯报道。
- D：聚合转载/不可追溯来源。

## 3. 信源权重
基础权重由 `rules/scoring.yaml` 的 `base_weight_by_tag` 决定：
- regulatory=1.00
- journal=0.95
- company=0.90
- market_research/thinktank=0.85
- media=0.70
- aggregator=0.30

再叠加 `trust_tier_adjust`：A=+0.10，B=0，C=-0.10。
最终 clamp 到 `[0.10, 1.10]`。

## 4. 质量分公式（0-100）
- `evidence_points`: A/B/C/D = 45/35/25/10
- `source_points`: `source_weight * 30`
- `recency_points`: <24h=15，1-3d=10，3-7d=6，7-14d=2，>14d=0
- `completeness_points`: summary/published/source/original_source_url，最多 10
- `penalties`: 聚合源无原文链接 -12，过短摘要 -6
- `signal_bonus`: 红+6、橙+3、黄+1、灰+0

`quality_score` 会在 explain 中以 `score_breakdown` 展示构成。

## 5. 强去重
去重键优先级：
1) canonical_url
2) normalized host+path
3) normalized_title + domain + date 的 hash

同簇 canonical 选择顺序：
1) quality_score
2) evidence_grade
3) source_weight
4) completeness

输出字段：
- `story_id`
- `dedupe_key`
- `deduped_from_ids`
- `other_sources`
- `dedupe_reason`

## 6. 多样性约束
默认配额（可在 `rules/scoring.yaml` 调整）：
- regulatory 至少 4
- journal/preprint 合计至少 4
- company 至少 4
- media 最多 14
- aggregator 最多 2（且必须可追溯 original_source_url）

## 7. 回滚
- 环境变量：`SCORING_DEDUPE_ENABLED=false`
- 仅 enhanced 回退到旧排序/旧去重路径；legacy 本来就不启用此模块。

## 8. 验证
```bash
python3 -m app.workers.cli rules:validate --profile enhanced
python3 -m app.workers.cli rules:dryrun --profile enhanced --date 2026-02-19
```

查看 artifacts：
- `artifacts/<run_id>/scoring_explain.json`
- `artifacts/<run_id>/scoring_summary.json`
- `artifacts/<run_id>/cluster_explain.json`
