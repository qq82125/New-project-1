# RULES_SYSTEM

## 概念
本项目规则系统拆为四套互不影响的规则（彼此有清晰边界）：
- `email_rules`：定义发信行为（主题、收件人、重试、栏目输出偏好等）。
- `content_rules`：定义“入选候选集合”（来源选择、过滤、分类、去重、可信度分层、摘要素材）。
- `qc_rules`：定义“质量评估 + 可选补齐/降级策略”（不得改邮件模板结构，不得重新抓网）。
- `output_rules`：定义“如何渲染输出 A–G”（不得改变 content 入选集合，最多裁剪展示）。

默认执行链路保持原样，`profile=legacy` 保持现有行为；仅当显式启用 `profile=enhanced` 时，增强规则才生效，并且可回退。

## 目录结构
```text
rules/
  email_rules/
    legacy.yaml
    enhanced.yaml
    default.v1.yaml
    strict.v2.yaml
  content_rules/
    legacy.yaml
    enhanced.yaml
    default.v1.yaml
    strict.v2.yaml
  qc_rules/
    legacy.yaml
    enhanced.yaml
  output_rules/
    legacy.yaml
    enhanced.yaml
  schemas/
    email_rules.schema.json
    content_rules.schema.json
    qc_rules.schema.json
    output_rules.schema.json
```

## Profile 与版本
- `profile=legacy`：显式表达现有行为（默认建议）。
- `profile=enhanced`：增强示例，用于后续灰度。
- `version`：规则文件版本（建议语义化）。

建议约定：
- `legacy` 只做“行为显式化”，避免引入新策略。
- `enhanced` 可引入新阈值、新过滤、新来源分层，但必须可回退。

## 统一规则文件结构
每个规则文件均包含以下顶层字段：
- `version`
- `profile`
- `defaults`
- `overrides`
- `rules`（数组）
- `output`

同时保留 `ruleset`（`email_rules` / `content_rules`）用于校验与路由。

## 四套规则的边界（必须遵守）
- `content_rules` 只允许影响：来源选择、过滤、分类、去重、可信度分层、摘要素材。
- `qc_rules` 只允许影响：质量评估、指标审计、补齐/降级策略（补齐只能从本次候选池选择，不得重新抓网）。
- `output_rules` 只允许影响：A–G 结构开关与排序、每段条数、摘要长度、标签展示、是否展示 other_sources、趋势/缺口条数、热力图策略；不得改变候选集合。
- `email_rules` 只允许影响：收件人、主题模板、发信策略、栏目结构（投递侧）；不得反向影响候选集合。

代码层通过边界断言防耦合：当规则越界时触发 `RULES_BOUNDARY_VIOLATION`，并对该 ruleset 自动回退 `legacy`（不全局崩）。

## 如何验证
```bash
python3 -m app.workers.cli rules:validate
python3 -m app.workers.cli rules:validate --profile legacy
python3 -m app.workers.cli rules:validate --profile enhanced
```
说明：该命令会校验规则与 sources registry（若已启用）。

## 如何打印最终决策对象
```bash
python3 -m app.workers.cli rules:print --profile legacy
python3 -m app.workers.cli rules:print --profile enhanced
python3 -m app.workers.cli rules:print --profile enhanced --strategy priority_merge
```
说明：输出统一 JSON 决策对象，包含：
- `content_decision`（allow/deny sources、keyword_sets、categories_map、dedupe_window 等）
- `qc_decision`（min_24h_items、share targets、repeat-rate、required_sources、fail_policy 等）
- `output_decision`（A–G rendering knobs + constraints）
- `email_decision`（subject_template、sections、recipients、schedule、thresholds 等）
- `explain`（为何命中/为何排除/冲突如何解决）

## 冲突策略
当同一决策字段被多条规则命中时：
- 基础顺序：按 `priority` 升序执行（高优先级后执行），同优先级按规则出现顺序（last_match）。
- 默认策略：`priority_last_match`（后命中覆盖先命中）。
- 可选策略：
  - `priority_append`：列表字段追加去重。
  - `priority_merge`：字典字段深度合并。
- 规则可显式指定 `merge_strategy`（`last_match|append|merge`）覆盖全局策略。

## 如何 dry-run
```bash
python3 -m app.workers.cli rules:dryrun
python3 -m app.workers.cli rules:dryrun --profile legacy --date 2026-02-16
python3 -m app.workers.cli rules:dryrun --profile enhanced --date 2026-02-16
```
说明：dry-run 会执行“采集->过滤->QC->渲染->生成邮件预览”，但不发信、不写库。产物写入 `artifacts/<run_id>/`：
- `run_id.json`（explain）
- `newsletter_preview.md`
- `items.json`
- `qc_report.json`
- `output_render.json`
- `run_meta.json`（四套 ruleset 的版本号）

如启用故事级聚合，还会生成：
- `clustered_items.json`（含 `story_id/other_sources`）
- `cluster_explain.json`

如启用事件类型判定 explain，会生成：
- `event_type_explain.json`（每条 event_type 判定依据）

## 如何 replay（只读复现）
```bash
python3 -m app.workers.cli rules:replay --run-id dryrun-xxxx --send false
```
说明：
- replay 仅从 `artifacts/<run_id>/` 读取已落地数据复现，不重新抓取网络，避免结果漂移。
- `--send` 默认 `false`；只有明确传 `true` 才会触发发信。

## 示例

### 示例1：新增一个赛道栏目
目标：在内容输出中增加“慢病管理检测”赛道。

1. 修改 `rules/content_rules/enhanced.yaml`
2. 在 `defaults.coverage_tracks` 追加 `慢病管理检测`
3. 在 `rules` 里新增 `lane_mapping` 规则，将关键词映射到该赛道
4. 在 `output.sections` 确认赛道速览模块仍启用

示例片段：
```yaml
defaults:
  coverage_tracks:
    - 肿瘤检测
    - 感染检测
    - 生殖与遗传检测
    - 其他
    - 慢病管理检测

rules:
  - id: lane-map-chronic
    enabled: true
    priority: 70
    type: lane_mapping
    description: 将慢病关键词映射为新赛道
    params:
      lane: 慢病管理检测
      include_keywords: ["糖化血红蛋白", "心血管风险", "代谢综合征", "chronic"]
```

### 示例2：新增一个数据源
目标：新增亚太监管源。

1. 修改 `rules/content_rules/enhanced.yaml`
2. 在 `defaults.sources.regulatory_apac` 新增源对象
3. 如需更高权重，在 `rules` 中配置 `source_priority`

示例片段：
```yaml
defaults:
  sources:
    regulatory_apac:
      - name: HSA News
        url: https://www.hsa.gov.sg/announcements
        region: 亚太
        trust_tier: A

rules:
  - id: source-priority-reg-apac
    enabled: true
    priority: 85
    type: source_priority
    description: 亚太监管优先
    params:
      groups: ["regulatory_cn", "regulatory_apac"]
```

### 示例3：新增一个过滤条件
目标：排除“纯财报/纯裁员”噪音。

1. 修改 `rules/content_rules/enhanced.yaml`
2. 在 `rules` 增加 `exclude_filter` 规则
3. 设置黑名单词，同时保留诊断锚点白名单

示例片段：
```yaml
rules:
  - id: exclude-business-noise
    enabled: true
    priority: 95
    type: exclude_filter
    description: 排除非IVD业务噪音
    params:
      exclude_keywords: ["earnings", "quarterly revenue", "layoff"]
      keep_if_has_keywords: ["diagnostic", "assay", "ivd", "pcr", "ngs"]
```

## 最佳实践
- 新规则先在 `enhanced` 试跑，再灰度到正式任务。
- 每次变更只改一个维度（来源/过滤/阈值）便于回溯。
- 版本升级时同步记录变更说明与回退策略。

## 防耦合边界
- `content_rules` 只允许影响：来源选择、过滤、分类、去重、可信度分层、摘要素材。
- `email_rules` 只允许影响：栏目结构、排序、阈值、标题模板、收件人、发信策略、内容裁剪。
- 禁止跨界：
  - `email_rules` 不能改变“哪些条目被采集/入选”。
  - `content_rules` 不能改变“发给谁/何时发/模板结构”。
- 代码层通过决策边界断言实现：跨界字段会触发 `RULES_BOUNDARY_VIOLATION`，并自动回退 `legacy`。

## 常见改规则示例
1. 新增一个栏目（只改 `email_rules`）
   - 文件：`rules/email_rules/enhanced.yaml`
   - 修改：`output.sections` 增加栏目；必要时在 `rules` 增加 `content_format` 排序策略。
2. 新增一个来源（只改 `content_rules`）
   - 文件：`rules/content_rules/enhanced.yaml`
   - 修改：`defaults.sources.*` 新增 source；可在 `rules` 的 `source_priority` 调整优先级。
3. 调整去重窗口（只改 `content_rules`）
   - 文件：`rules/content_rules/enhanced.yaml`
   - 修改：`rules` 中 `type=dedupe` 的窗口/重复率参数。
4. 调整摘要长度与结论前置（只改 `email_rules`）
   - 文件：`rules/email_rules/enhanced.yaml`
   - 修改：`output.summary_max_chars` 与 `rules(type=content_format)` 的版式阈值参数。

## Story-level Dedupe / Clustering
在 `profile=enhanced` 下，内容侧可启用“故事级去重聚合”：
- 同一新闻多源转载时，仅保留一个 primary item 进入日报候选。
- 其余来源保存在 primary 的 `other_sources[]`。
- 解释对象提供聚合键、窗口判定、primary 选择依据。

可配置项（`content_rules`）：
- `dedupe_cluster.enabled`
- `dedupe_cluster.window_hours`
- `dedupe_cluster.key_strategies`
  - `canonical_url`：最精准，但依赖源提供 canonical。
  - `normalized_url_host_path`：对同站重复有效，跨站转载效果有限。
  - `title_fingerprint_v1`：跨站聚合能力强，但有过度合并风险。
- `dedupe_cluster.primary_select`
  - 常用：`source_priority -> evidence_grade -> published_at_earliest`
- `dedupe_cluster.max_other_sources`

风险与适用性：
- `canonical_url`：低误合并、低召回（部分站点无 canonical）。
- `normalized_url_host_path`：低误合并，适合同站去重。
- `title_fingerprint_v1`：高召回，需配合 `window_hours` 控制误合并。

## QC Rules（质控规则）示例
`qc_rules` 用于评估质量并给出动作策略（仅 dry-run 生效，不影响线上定时触发本身）：

示例片段（enhanced）：
```yaml
ruleset: qc_rules
profile: enhanced
defaults:
  min_24h_items: 10
  apac_min_share: 0.40
  daily_repeat_rate_max: 0.25
  recent_7d_repeat_rate_max: 0.40
  quality_policy:
    required_sources_checklist: [NMPA, CMDE, CCGP, TGA, PMDA/MHLW]
    event_groups:
      regulatory: [监管审批与指南]
      commercial: [并购融资/IPO与合作, 注册上市, 产品发布, 临床与科研证据, 支付与招采, 政策与市场动态]
  fail_policy:
    mode: only_warn   # 或 auto_topup / degrade_output_legacy / require_manual_review
```

## Output Rules（渲染规则）示例
`output_rules` 只决定 A–G 的渲染，不改变候选集合：

示例片段（enhanced）：
```yaml
ruleset: output_rules
profile: enhanced
defaults:
  sections: [{id:A,enabled:true},{id:B,enabled:true},{id:C,enabled:true},{id:D,enabled:true},{id:E,enabled:true},{id:F,enabled:true},{id:G,enabled:true}]
  A:
    items_range: {min: 8, max: 15}
    summary_sentences: {min: 2, max: 3}
    show_other_sources: true
  constraints:
    g_must_be_last: true
    a_to_f_must_not_include_quality_metrics: true
output:
  sections_order: [A,B,C,D,E,F,G]
```

## 常见问题（FAQ）
1) QC fail 时怎么处理？
- 推荐流程：先查看 `/admin/qc` 的 QC 面板与 `fail_reasons`，再调整：信源、关键词、区域纠偏目标或 fail_policy。
- `fail_policy.mode=only_warn`：只提示，不自动补齐。
- `auto_topup`：只允许从本次候选池内的“7天补充”条目回补，不重新抓网，保证可复现。
- `degrade_output_legacy`：渲染降级为 legacy 输出（内容候选不变）。
- `require_manual_review`：标记需要人工复核（依赖运维流程）。

2) 为什么 G 必须在末尾？
- 合规与可读性：A–F 是业务正文，G 是质量审计，必须分离。
- 可测试性：系统有断言/单测确保 “A–F 禁止质量指标字段，G 必须置尾”，避免误把指标写进正文。

3) 去重聚合如何选主来源？
- `content_rules.defaults.source_priority` 定义来源优先级（数值越大越优先）。
- `dedupe_cluster.primary_select` 定义比较顺序（默认：`source_priority -> evidence_grade -> published_at_earliest`）。
- 被聚合的其他来源会挂载到 primary 的 `other_sources[]`，并在 explain 中记录原因。

enhanced 示例片段：
```yaml
defaults:
  source_priority:
    reuters: 100
    bloomberg: 95
    nature: 90
    statnews: 85
    endpoints: 80
    fiercebiotech: 75
    generic_rss: 50
  dedupe_cluster:
    enabled: true
    window_hours: 72
    key_strategies:
      - canonical_url
      - normalized_url_host_path
      - title_fingerprint_v1
    primary_select:
      - source_priority
      - evidence_grade
      - published_at_earliest
    max_other_sources: 5
```

## 旁路接入开关
- 默认：`legacy`（与当前线上/本地行为一致）。
- 仅当环境变量显式开启时启用增强规则：
  - `ENHANCED_RULES_PROFILE=enhanced`
- 若 `enhanced` 规则加载失败：自动回退 `legacy`，打印 `[RULES_WARN]` 告警，不中断任务。

## Docker Compose 运行
本项目提供最小 `docker-compose.yml`（两服务）：
- `admin-api`：规则控制台（FastAPI）
- `scheduler-worker`：常驻调度（APScheduler，读取 `scheduler_rules`）

启动：
```bash
docker compose up -d --build
```

验证（容器内执行 CLI）：
```bash
docker compose exec admin-api python3 -m app.workers.cli rules:validate --profile enhanced
docker compose exec admin-api python3 -m app.workers.cli rules:dryrun --profile enhanced --date 2026-02-16
```

注意：
- Docker 模式默认将控制台映射到宿主机 `8090` 端口（避免与本机 launchd 模式的 `8789` 冲突）。
- 常驻模式启用后，GitHub Actions 仍可保留作为兜底补发（可按需停用定时触发）。
- `scheduler-worker` 默认会按 `scheduler_rules(enhanced)` 注册任务；若容器内要实际发信，需要提供 `TO_EMAIL` 与 SMTP 环境（参见 `send_mail_icloud.sh` 读取的 `.mail.env` 变量）。

## 信源管理（可运营化）
信源与采集规则解耦，统一放在：
- `rules/sources/rss.yaml`
- `rules/sources/web.yaml`
- `rules/sources/api.yaml`

新增信源流程：
1. 只修改 `rules/sources/rss.yaml`（或 `web.yaml/api.yaml`）新增 source。
2. 运行 `python -m app.workers.cli sources:validate`。
3. 运行 `python -m app.workers.cli sources:test --source-id <id> --limit 3`。
4. 运行 `python -m app.workers.cli rules:dryrun --profile enhanced --date 2026-02-16` 观察入选与统计。

下线信源流程：
- 推荐保留记录并下线，不硬删：`python -m app.workers.cli sources:retire --source-id <id> --reason \"xxx\"`
- 这样可减少 replay 漂移风险。

可用命令：
```bash
python -m app.workers.cli sources:list --profile enhanced
python -m app.workers.cli sources:validate
python -m app.workers.cli sources:test --source-id reuters-health-rss --limit 3
python -m app.workers.cli sources:diff --from legacy --to enhanced
python -m app.workers.cli sources:retire --source-id reuters-health-rss --reason "duplicate feed"
```

## Rules Console（网页规则控制台）
控制台功能：编辑/校验/预览/发布/回滚；运行侧只读取“已发布版本”。

版本目录：
- `rules/console/versions/<version>/rules/...`
- `rules/console/versions/<version>/meta.json`
- `rules/console/published.json`（active_version/previous_version/history）

运行契约：
- 默认首次启动会 bootstrap 当前规则为 `v0001` 发布版本。
- 之后 RuleEngine 读取 `published` 对应版本。
- 控制台未发布前，运行侧不会读取草稿。

启动控制台（Basic Auth）：
```bash
export RULES_CONSOLE_USER=admin
export RULES_CONSOLE_PASS=change-me
python -m app.web.rules_console
```

或 Token：
```bash
export RULES_CONSOLE_TOKEN=your_token
python -m app.web.rules_console
```

主要接口：
- `GET /api/rules/current`
- `POST /api/rules/validate`
- `POST /api/rules/preview`（dry-run，不发信）
- `POST /api/rules/publish`
- `POST /api/rules/rollback`
- `GET /api/rules/versions`
