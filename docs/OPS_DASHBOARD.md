# Ops Dashboard Lite (PR17)

## 页面与接口

- 页面：`/admin/ops`
- 只读接口：`/admin/api/ops/summary?limit=20`

页面通过接口读取最新 artifacts 指标，不写数据库、不改配置。

## 展示字段

页面分四块卡片：

- Digest
  - `timestamp`
  - `items_before_dedupe` / `items_after_dedupe`
  - `analysis_cache_hit` / `analysis_cache_miss`
  - `analysis_degraded_count`
  - `evidence_missing_core_count`
  - `unknown_metrics`（`unknown_*` 聚合）
  - `opportunity_signals_written` / `opportunity_signals_deduped`
- Collect
  - `timestamp`
  - `assets_written_count`
  - `sources_failed_count`
  - `dropped_static_or_listing_count`
  - `dropped_static_or_listing_top_domains`
- Acceptance
  - `timestamp`
  - `ok`
  - `checks_passed`
  - `quality_pack_selected_total`
- Procurement Probe
  - `timestamp`
  - `totals`
  - `by_error_kind`（TopN）
  - `per_source`（简表，TopN）

页面底部 `Warnings` 显示非致命错误（例如文件缺失、JSON 解析失败）。

## 数据来源路径规则

服务在 `artifacts/` 下查找最新文件，默认返回相对路径：

- Digest：
  - `artifacts/run_meta.json`
  - 或最新 `artifacts/run-*/run_meta.json`
  - 或 `artifacts/*digest*.json`
- Collect：
  - `artifacts/collect_meta.json`
  - 或最新 `artifacts/collect-*/run_meta.json`
  - 或 `artifacts/*collect*.json`
- Acceptance：
  - `artifacts/acceptance/acceptance_report.json`
- Procurement Probe：
  - 最新 `artifacts/procurement/probe_report-*.json`

## 常见问题

### 为什么显示“暂无数据/文件缺失”？

- 对应 artifacts 文件不存在，或路径下尚未产生该类型报告。
- 文件存在但 JSON 非法，接口会降级返回并在 `Warnings` 说明。

### 如何确认 artifacts 正常写入？

- 检查运行任务是否产生目标文件：
  - `artifacts/run-*/run_meta.json`
  - `artifacts/collect-*/run_meta.json`
  - `artifacts/acceptance/acceptance_report.json`
  - `artifacts/procurement/probe_report-*.json`
- 先执行相关任务后再刷新 `/admin/ops`。
