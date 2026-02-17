# Rules System（规则系统）设计与使用说明

## 1. 目标与边界

本规则系统将现有逻辑拆分为两套互不影响的规则：

- `email_rules`：发信规则（发给谁、主题、重试、兜底等）
- `content_rules`：采集与内容规则（时间窗、来源、去重、排序、质量约束等）

当前阶段仅新增规则资产与校验契约，不改变默认执行路径。  
即：**不显式启用新 profile 时，行为保持与旧逻辑一致**。

## 2. 目录结构

```text
rules/
  email_rules/
    default.v1.yaml
    strict.v2.yaml
  content_rules/
    default.v1.yaml
    strict.v2.yaml
  schemas/
    email_rules.schema.json
    content_rules.schema.json
```

## 3. 配置模型

每份规则文件都包含：

- `ruleset`：`email_rules` 或 `content_rules`
- `version`：规则版本（可版本化）
- `profile`：profile 名称（可灰度）
- `feature_flags`：特性开关
- `compatibility`：兼容策略
- 规则主体字段（`delivery` / `collection` / `quality_gates` 等）

## 4. Profile 策略（灰度）

推荐通过环境变量选择 profile（后续由 RuleEngine 实现）：

- `RULES_EMAIL_PROFILE=default.v1`
- `RULES_CONTENT_PROFILE=default.v1`

当 profile 未指定时，默认使用 `default.v1`，保持旧行为。

## 5. 校验策略（Schema）

规则文件使用 JSON Schema 校验：

- `rules/schemas/email_rules.schema.json`
- `rules/schemas/content_rules.schema.json`

校验要求：

- 字段类型正确
- 必填字段完整
- 枚举值合法
- 阈值范围合法（如比例在 0~1）

## 6. 运行模式（已提供 CLI）

当前已提供：

- `python -m app.workers.cli rules:validate`
- `python -m app.workers.cli rules:dryrun`
- `python -m app.workers.cli rules:replay`

语义：

- `validate`：仅校验规则与配置
- `dryrun`：输出“会采集什么/会发什么”，不落库不发信
- `replay`：按 `date` 或 `run_id` 重放，默认不发信

示例：

```bash
python -m app.workers.cli rules:validate
python -m app.workers.cli rules:dryrun
python -m app.workers.cli rules:replay --date 2026-02-16
python -m app.workers.cli rules:replay --date 2026-02-16 --send
```

## 7. 向后兼容契约

- `default.v1` 对齐当前线上/本地默认逻辑
- 新 profile（如 `strict.v2`）需显式启用才生效
- 校验失败时允许按 `compatibility.fallback_to_legacy_on_error` 回退到旧逻辑

## 8. 最小可观测字段（后续接入）

统一输出字段（日志/产物）：

- `run_id`
- `profile`
- `rules_version`
- `mode`（normal/dryrun/replay）

## 9. 示例

### 9.1 使用默认 profile（兼容模式）

```bash
export RULES_EMAIL_PROFILE=default.v1
export RULES_CONTENT_PROFILE=default.v1
```

### 9.2 灰度 strict profile

```bash
export RULES_EMAIL_PROFILE=strict.v2
export RULES_CONTENT_PROFILE=strict.v2
```

> 说明：在 RuleEngine 完成接入前，以上环境变量仅作为约定，不改变当前脚本执行行为。
