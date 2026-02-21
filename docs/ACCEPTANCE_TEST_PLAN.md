# Acceptance Test Plan

目标：提供一套本地可重复执行的验收工具链，用于验证 collect/digest/增强能力是否可用，不改变生产默认行为。

## 1. 范围与边界
- 只验证以下能力：
  - collect 资产写入
  - digest 出刊结构
  - enhanced 聚合（story clustering）
  - track(core/frontier) 分流
  - analysis cache 命中
  - 模型 fallback/降级链路
- 不依赖外网：使用本地合成样本，不要求真实信源全量可达。
- 不改默认 rules / scheduler / 输出结构：验收仅写 `artifacts/acceptance/*`。

## 2. 运行命令
```bash
python -m app.workers.cli acceptance-run --mode smoke
python -m app.workers.cli acceptance-run --mode regression
python -m app.workers.cli acceptance-run --mode full --as-of 2026-02-21 --keep-artifacts
```

输出：
- `artifacts/acceptance/acceptance_report.md`
- `artifacts/acceptance/acceptance_report.json`
- `artifacts/acceptance/logs/digest_preview.txt`

## 2.1 Smoke（PR-A1 最小可用验收）
- 目标：30 分钟内完成 collect/digest/cache 的端到端最小验收。
- 流程：
  1. `collect-now --force` 运行一次（enhanced）
  2. 检查 `artifacts/collect/items-YYYYMMDD.jsonl` 行数增量 > 0
  3. `digest-now` 连续运行两次（enhanced，`--send false`）
  4. 自动检查：
     - A-G 段存在
     - G 段含 dedupe 指标（enhanced）
     - G 段含 core/frontier 指标（enhanced）
     - 第二次 `analysis_cache_hit` 上升，或 `analysis_cache_miss` 下降（若首次已全命中允许相等）
- 通过标准：以上 smoke 断言全部 PASS。

## 2.2 Regression（PR-A2 稳定性验收）
- 目标：验证坏源、离线、模型失败三类故障下，系统仍可出刊且缺口可解释。
- 注入开关（仅 acceptance 进程内生效，不影响生产）：
  - `ACCEPTANCE_INJECT_BAD_SOURCE=1`
  - `ACCEPTANCE_OFFLINE=1`
  - `ACCEPTANCE_MODEL_FAIL=1`
- 流程：
  1. Bad source：模拟坏源失败并写入 `regression_collect_run_meta.json`
  2. Offline：在离线输入下生成 digest，验证 G 段缺口说明
  3. Model failure：模拟模型失败，验证 fallback/degraded 后仍能生成 digest
- 必选断言：
  - `bad_source_failure_recorded == true`
  - `offline_digest_still_generates == true`
  - `model_failure_does_not_break == true`
  - `gap_explained_in_G == true`
- 输出证据：
  - `artifacts/acceptance/logs/regression_collect_run_meta.json`
  - `artifacts/acceptance/logs/regression_offline_digest.txt`
  - `artifacts/acceptance/logs/regression_model_fail_digest.txt`

## 2.3 Quality Pack（PR-A3 人工复核包）
- 运行：`python -m app.workers.cli acceptance-run --mode full`
- 自动产出：
  - `artifacts/acceptance/quality_pack.md`
  - `artifacts/acceptance/quality_pack.json`
- 采样策略：
  - core 20 条
  - frontier 10 条
  - 若不足，报告中写明原因（来源不足/阈值过严/采集断流）
- 每条样本包含：
  - 基础信息：title/url/source
  - 结构字段：track/relevance_level/relevance_explain
  - 生成字段：summary/impact/action（优先来自 analysis cache）
  - evidence snippet（来自候选摘要截断）
  - 人工复核勾选框

## 3. 测试矩阵

| 检查 ID | smoke | regression | full | 判定 |
|---|---|---|---|---|
| collect_jsonl_written | Y | Y | Y | 生成 `items-YYYYMMDD.jsonl` 且有行数 |
| collect_dedupe_effective | Y | Y | Y | 同 URL 重复写入被 skipped |
| digest_structure_a_to_g | Y | Y | Y | 报告包含 A-G 段 |
| enhanced_clustering_reduction | N | Y | Y | 聚合后条目数下降 |
| track_split_core_frontier | Y | Y | Y | core/frontier 样例判别符合阈值 |
| analysis_cache_hit_after_second_run | N | Y | Y | 第二次 digest 出现 cache hit |
| model_fallback_and_degraded_path | N | Y | Y | primary 失败可 fallback，双失败走 degraded |
| rules_validate_enhanced_ok | N | N | Y | `rules:validate --profile enhanced` 通过 |

## 4. 通过/失败判定
- 通过：所有已启用检查均为 `pass`。
- 失败：任一检查为 `fail`，CLI 返回非 0；报告给出证据和修复建议。

## 5. 失败证据要求
每个失败项必须包含：
- `evidence`：文件路径、关键计数或字段值
- `suggestion`：可执行修复动作（例如调整配置、检查目录权限、修复规则字段）

## 6. 典型失败与修复方向
- collect 无文件：检查 `artifacts/acceptance/collect` 权限和磁盘空间。
- collect 行数不增：检查源是否全部 disabled、due gating 是否生效（可先用 `--force true`）。
- digest 缺段：检查渲染逻辑是否破坏 A-G 段模板。
- G 缺 dedupe 指标：补充 `items_before/after` 或 `reduction_ratio` 到 G 段与 run_meta。
- cache 无命中：检查 `always_generate`、`item_key` 稳定性和 `asset_dir`。
- fallback 失败：检查 `MODEL_PRIMARY/MODEL_FALLBACK` 与重试/超时配置。

## 8. 质量复核标准（人工）
人工复核只看三项：
1. 事实性：是否胡编（与来源不一致/无依据）
2. 可行动：`action` 是否具体、可执行
3. 一致性：同类条目结构是否稳定（summary/impact/action 格式一致）

## 7. 回滚说明
- 该验收链路是独立脚本，不参与生产调度。
- 若不需要可直接停止使用 `acceptance-run`，不会影响日常出刊链路。
