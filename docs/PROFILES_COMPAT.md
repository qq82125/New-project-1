# Profiles Compatibility Matrix

本文说明本轮改造后 `legacy` 与 `enhanced` 的行为边界，确保向后兼容可核验。

## 1. 总体原则
- `legacy`：默认行为不变（结构、默认开关、输出口径保持）。
- `enhanced`：逐步启用新能力（可回滚）。
- 回滚优先：通过 profile/开关恢复，不做破坏性迁移。

## 2. 差异总表（本轮范围）

| 维度 | legacy | enhanced |
|---|---|---|
| collect 高频轻量采集 | 默认关闭/不依赖 | 默认启用（PR1） |
| digest 读取 collect 资产 | 默认关闭 | 可启用并优先读取（PR1） |
| story clustering 默认 | 维持旧默认（通常关闭） | 默认开启（PR2） |
| dedupe 统计写入 G 段 | 不新增 | 新增 dedupe 统计（PR2） |
| track + relevance_level | 不强制 split | 默认启用（PR3） |
| A/F/G 分流 | 维持原规则 | A(core)、F(frontier)、G补充覆盖统计（PR3） |
| analysis cache | 不依赖 | 启用（PR4A） |
| 模型分层调用（高模/低模） | 不变 | 启用（PR5B） |
| prompt_version 固定并可比对 | 非强制 | 强制记录（PR5B） |
| source policy（min_trust_tier/exclude） | 默认宽松（C） | 默认更严格（B，PR8） |
| frontier policy（诊断锚点必需） | 关闭（兼容旧口径） | 开启（PR9，抑制泛生物学论文） |
| evidence policy（core 证据片段必需） | 关闭（兼容旧口径） | 开启（PR10，缺证据自动降级） |
| opportunity index（机会指数/H段） | 关闭（兼容旧口径） | 开启（PR11，新增非破坏性信号层） |

## 3. 不可破坏项（验收基线）
- A–G 结构：legacy 不变。
- 默认开关：legacy 不引入新默认开启项。
- 失败可见：两档位均不能静默失败（`ok/error/run_meta` 需可追溯）。

## 4. 回滚映射

| 场景 | 回滚方式 |
|---|---|
| enhanced 新逻辑异常 | 切回 `profile=legacy` |
| collect 资产链路异常 | `use_collect_assets=false`，digest 回到旧路径 |
| clustering 异常 | `dedupe_cluster.enabled=false` |
| track 分流异常 | 关闭 `track_routing` / 提高阈值或回 legacy |
| analysis cache 异常 | `ANALYSIS_CACHE_ENABLED=false` |
| 分层模型效果不稳 | 关闭分层开关，退回单模型/旧模型 |

## 5. 本地对比验收（建议）
```bash
# legacy 基线
python3 -m app.workers.cli rules:dryrun --profile legacy

# enhanced 对照
python3 -m app.workers.cli rules:dryrun --profile enhanced
```

重点对比：
- 结构：A–G 都存在，G 置尾
- 兼容：legacy 与改造前关键输出一致
- 增强：enhanced 出现新增 explain/coverage/dedupe 统计字段

## 6. 文档联动
- collect 契约：`/Users/GY/Documents/New project 1/docs/COLLECT_ASSET_CONTRACT.md`
- analysis cache 契约：`/Users/GY/Documents/New project 1/docs/ANALYSIS_CACHE_CONTRACT.md`
- 规则系统总览：`/Users/GY/Documents/New project 1/docs/RULES_SYSTEM.md`
