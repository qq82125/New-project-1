# Collect Asset Contract

适用范围：`enhanced` 的 `collect` 高频任务。`legacy` 不变。  
目标：`collect` 只做轻量真实抓取并落资产；`digest` 从资产读窗口，不直接依赖实时抓网。

## 1. 资产目录与命名
- 默认目录：`artifacts/collect/`
- 默认文件：`items-YYYYMMDD.jsonl`
- 可按 run 分片（可选）：`items-YYYYMMDD-<run_id>.jsonl`
- 写入策略：追加写入；失败不中断进程但要记录到 run meta。

## 2. JSONL 最小字段 Schema（每行一条）
```json
{
  "run_id": "collect-1771603297",
  "collected_at": "2026-02-20T16:01:12+08:00",
  "source_id": "fda-medwatch-rss",
  "source_name": "FDA MedWatch",
  "url": "https://...",
  "canonical_url": "https://...",
  "title": "....",
  "published_at": "2026-02-20T08:12:00+00:00",
  "summary": "....",
  "raw_text": "....",
  "normalized_text": "....",
  "region": "北美",
  "track": "core",
  "relevance_level": 3,
  "relevance_explain": {
    "anchors_hit": ["diagnostic"],
    "negatives_hit": [],
    "rules_applied": ["compute_relevance_v1"],
    "final_reason": "core_anchor_or_regulatory_signal"
  },
  "dedupe_key": "sha1:..."
}
```

必填最小集合：
- `run_id,collected_at,source_id,url,title,track,relevance_level`

## 3. 去重与刷盘策略（collect 阶段）
- 目标：高频采集不重复刷盘。
- 去重键优先级：
1. `canonical_url`
2. `url` 归一化（去 query/fragment）
3. `title` 归一化 hash（兜底）
- 写入前检查：
  - 同日文件中若已有相同 dedupe key，则跳过；
  - 记录 `skip_reason=duplicate_in_collect` 到统计。

## 4. 保留策略
- 默认保留：最近 14 天（与 scheduler rules `artifacts.retain_days` 对齐）。
- 清理策略：每日清理一次，删除超期 JSONL。
- 任何清理动作需记录日志：`deleted_files`, `deleted_rows_estimate`（可估算）。

## 5. 运行契约（collect vs digest）
- `collect`：
  - 高频（如 60min）
  - 行为：抓取 + 轻量标准化 + relevance 标注 + 写 JSONL
  - 不发信
- `digest`：
  - 低频（如每日 08:30）
  - 行为：读取窗口资产 -> 聚合/分流/渲染 -> 可发信
  - 允许“无资产缺口解释”，不允许静默成功。

## 6. 错误与可观测
- 失败不可静默：`run_meta.json` 必须写：
  - `ok`
  - `error`（若失败）
  - `collect_written_count`
  - `collect_skipped_duplicate_count`
  - `collect_asset_file`
- CLI 退出码：
  - 成功 `0`
  - 失败非 `0`（并输出原因）

## 7. 回滚点
- 回滚到旧行为（不改 legacy）：
  - 在 `enhanced` 关闭 `use_collect_assets`（若有该开关）
  - 或把调度改回仅 digest（不建议长期）
- 回滚后仍保留已有资产文件，不做破坏性删除。

## 8. 本地验收命令
```bash
python3 -m app.workers.cli collect-now --profile enhanced
python3 -m app.workers.cli digest-now --profile enhanced --send false --use-collect-assets true
ls -lh artifacts/collect/
```

验收点：
- collect 后 `artifacts/collect/items-*.jsonl` 存在且有数据
- digest 在不抓网前提可读资产并生成预览
- 失败路径可在 `run_meta.json` 找到明确错误

