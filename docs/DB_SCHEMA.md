# DB Schema (MVP Control Plane)

本文件描述 PostgreSQL/SQLite 统一底座的最小控制面 Schema（不改变现有业务行为）。

## 表清单

1. `email_rules_versions`
2. `content_rules_versions`
3. `qc_rules_versions`
4. `output_rules_versions`
5. `scheduler_rules_versions`
6. `rules_drafts`
7. `sources`
8. `dual_write_failures`
9. `db_compare_log`
10. `run_executions`
11. `source_fetch_events`
12. `report_artifacts`
13. `send_attempts`
14. `dedupe_keys`

## 规则版本表（5张）

通用字段：
- `id` (PK, 自增)
- `profile` (规则档位)
- `version` (版本号)
- `config_json` (规则配置 JSON)
- `created_at`
- `created_by`
- `is_active` (0/1)

约束与索引：
- 唯一约束：`(profile, version)`
- 活跃查询索引：`(profile, is_active, id)`

用途：
- 运营流程：草稿校验 -> 发布生效 -> 回滚
- 运行侧只读取 active 版本

## `rules_drafts`

字段：
- `id` (PK, 自增)
- `ruleset`
- `profile`
- `config_json`
- `validation_json`
- `created_at`
- `created_by`

索引：
- `idx_rules_drafts_lookup (ruleset, profile, id)`

用途：
- 控制台草稿暂存与发布前校验

## `sources`

字段：
- 基础：`id(PK), name, connector, url, enabled, priority, trust_tier`
- JSON：`tags_json, rate_limit_json, fetch_json, parsing_json`
- 审计：`created_at, updated_at`
- 运行状态：
  - `last_fetched_at, last_fetch_status, last_fetch_http_status, last_fetch_error`
  - `last_success_at, last_http_status, last_error`

索引：
- `idx_sources_enabled_priority (enabled, priority)`

用途：
- 信源开关/优先级/抓取参数
- 最近抓取与测试状态回写展示

## JSON 字段类型策略

- PostgreSQL：优先 `JSONB`
- SQLite：`TEXT` 存 JSON 字符串（兼容离线/本地）

## Alembic 迁移

- 基线迁移：`alembic/versions/20260219_0001_baseline.py`
- 灰度可观测：`alembic/versions/20260219_0002_dual_shadow_logs.py`
- 运行台账：`alembic/versions/20260219_0003_run_ledger.py`
- 幂等键约束：`alembic/versions/20260219_0004_keys_and_send_attempts.py`
- 目标：`alembic upgrade head` 在 SQLite/PG 均可创建等价结构

## 幂等键（DB 约束）

- `profile + version`：规则版本表唯一约束（5 张版本表）
- `send_key`：`send_attempts` 唯一约束
- `run_key`：`run_executions` 唯一约束
- `dedupe_key`：`dedupe_keys` 唯一约束
