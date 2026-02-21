# Analysis Cache Contract

适用范围：`enhanced` 的 digest 分析链路。`legacy` 默认关闭（可按规则覆盖）。  
目标：分析结果资产化（jsonl），支持回放、重算、比对；失败可降级但不可静默。

## 1. 资产目录与文件
- 默认目录：`artifacts/analysis/`
- 文件：`artifacts/analysis/items-YYYYMMDD.jsonl`
- 一行一条缓存记录（JSONL）

## 2. Cache Key 设计（稳定可复现）
- key 组成（当前实现）：
  - `story_id + "|" + url_norm`（优先）
  - 回退：`url_norm`

说明：同一条目第二次 digest 会优先命中同日 key，减少重复生成。

## 3. 单条缓存 Schema（最小）
```json
{
  "item_key": "story123|example.com/news/abc",
  "created_at": "2026-02-20T16:20:10+08:00",
  "url": "https://example.com/news/abc",
  "story_id": "story123",
  "source_id": "reuters-health-rss",
  "title": "...",
  "summary": "摘要：...",
  "impact": "影响：...",
  "action": "建议：...",
  "model": "local-heuristic-v1",
  "prompt_version": "v2",
  "used_model": "gpt-4.1",
  "token_usage": { "prompt_tokens": 11, "completion_tokens": 22, "total_tokens": 33 },
  "generated_at": "2026-02-20T08:30:00Z",
  "degraded": false,
  "degraded_reason": "",
  "ok": true
}
```

## 4. digest 读取策略
- 先读 cache：命中 `item_key` 则直接复用。
- 未命中或失效：调用分析器生成并写回 cache。
- 分析失败：允许降级到旧逻辑（例如规则摘要/原文截断），但必须：
  - 在 G 段写入 `degraded_count + degraded_reason_top3`
  - 在 `run_meta.json` 写入 analysis 统计

## 5. 模型策略（PR5B）
- 支持环境变量：
  - `MODEL_PRIMARY`（高质量模型）
  - `MODEL_FALLBACK`（兜底模型）
  - `MODEL_POLICY=tiered|always_primary`
- 支持规则字段（`content_rules.defaults.analysis_cache`）：
  - `model_primary` / `model_fallback` / `model_policy`
  - `core_model` / `frontier_model`
  - `temperature` / `timeout_seconds` / `retries` / `backoff_seconds`
- 分层调用默认：
  - `track=core` 且 `relevance_level>=3`：优先 primary
  - 其他：优先 fallback（可改）
- 自动回退：
  - 主模型失败/限流/超时后自动使用 fallback
  - cache 必须记录 `used_model` 与 `fallback_from`（如果发生回退）

## 6. 重算与可比对
- 强制记录：`model`、`prompt_version`、`token_usage`、`generated_at`
- 提供两种重算策略：
  - `force_recompute=true`：忽略命中，重算并覆盖/新版本写入
  - `recompute_if_prompt_changed=true`：仅 prompt_version 变化时重算
  - 当前通过 `analysis_cache.always_generate=true` 实现强制重算

## 7. 保留策略
- 默认保留 30 天（可配置）。
- 清理原则：
  - 仅清理过期 key，不删最近窗口内 run 的缓存。
- 清理动作必须记录数量与耗时。

## 8. 失败与可观测
- 失败不可静默：
  - G 段必含 `analysis_cache_hit/miss` 与 `degraded_count`
  - `run_meta.json` 必含 `analysis` 统计
- CLI 结果必须有 `ok`，并区分：
  - `ok=true + degraded=true`
  - `ok=false`

## 9. 回滚点
- 关闭 cache：`content_rules.defaults.analysis_cache.enable_analysis_cache=false`
- 强制重算：`content_rules.defaults.analysis_cache.always_generate=true`
- 关闭分层：`model_policy=always_primary` 或 `core_model/frontier_model` 指向同一模型
- 回滚后不删除历史 cache 文件。

## 10. 本地验收命令（示例）
```bash
python3 -m app.workers.cli digest-now --profile enhanced --send false
python3 -m app.workers.cli digest-now --profile enhanced --send false  # 第二次应提升 cache hit
python3 -m app.workers.cli analysis-clean --keep-days 30
python3 -m app.workers.cli analysis-recompute --model primary --prompt-version v2 --sample 20
```

验收点：
- 第二次执行 `analysis_cache_hit` 高于第一次
- 关闭 cache 开关后仍可产出日报（但 hit=0）
- `run_meta.json` 包含模型与 prompt_version 记录
