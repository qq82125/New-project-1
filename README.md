# New-project-1: 全球 IVD 晨报（Rules Console + Always-on Scheduler + Ops Dashboard）

面向 IVD 行业的自动化晨报系统：
- 采集（collect）
- 相关性/分流（core/frontier/drop）
- 分析与缓存（analysis cache）
- 质量审计（G 段）
- 机会指数（H 段）
- 邮件发送与云端兜底

默认保持 `legacy` 行为；增强策略在 `enhanced` 下启用（可回滚）。

---

## 1. 当前最新状态（2026-02）

项目已落地并集成以下主线能力（PR7–PR18）：

- **PR7 CacheKeyAudit**
  - analysis cache key 统一为 `url_norm(url)`
  - hit/miss/mismatch 审计进入 digest 与 acceptance
- **PR8 SourcePolicy**
  - 源级策略：`min_trust_tier` / `exclude_domains` / `exclude_source_ids`
  - collect 与 digest 双保险过滤
- **PR9 FrontierNarrowing**
  - frontier 聚焦 IVD 技术雷达
  - 生物泛论文（无诊断锚点）可降级/剔除
- **PR10 EvidenceEnforcement**
  - core 要求 evidence_snippet，缺失时降级模板输出（不硬编）
- **PR11 Dynamic Source Management**
  - source/source_group 动态管理、组默认间隔、软删除/恢复
  - 运行时优先级：`DB overrides > rules workspace > YAML defaults`
- **PR12 Opportunity Hardening**
  - signal 同日去重、H 段 contrib 解释、unknown KPI
- **PR13 Region/Lane Mapping Hardening**
  - `rules/mappings/*.yaml` 映射归类，降低 `__unknown__`
- **PR14 Source Item Hardening**
  - `rss/html_article/html_list/api_json` 模式治理
  - 列表/静态页守卫，减少污染项
- **PR15~PR16 Procurement Pack/Core Sources**
  - 采购源接入、probe 诊断、核心 RSS/API 信源包
- **PR17 Ops Dashboard Lite**
  - `/admin/ops` 聚合 digest/collect/acceptance/probe 健康态
- **PR18 Event Type Hardening**
  - event_type 关键词优先级 + domain/path fallback
  - 输出 `unknown_event_type_rate` 与 `event_type_distribution_topN`

---

## 2. 核心目录

- 管理台入口：`app/admin_server.py`
- 管理台页面/API：`app/web/rules_admin_api.py`
- 调度 Worker：`app/workers/scheduler_worker.py`
- CLI 入口：`app/workers/cli.py`
- 晨报生成：`scripts/generate_ivd_report.py`
- 验收：`scripts/acceptance_run.py`
- 规则：`rules/`
- 文档：`docs/`

重点文档：
- `docs/RULES_SYSTEM.md`
- `docs/ALWAYS_ON_RUNBOOK.md`
- `docs/ANALYSIS_CACHE_CONTRACT.md`
- `docs/SOURCES_OPERATIONS.md`
- `docs/SOURCE_FETCH_MODES.md`
- `docs/OPPORTUNITY_INDEX.md`
- `docs/PROCUREMENT_ONBOARDING.md`
- `docs/OPS_DASHBOARD.md`

---

## 3. 快速启动（Docker，推荐）

### 3.1 环境准备

1. 复制并填写：
- `.docker.env.example` -> `.docker.env`
- `.mail.env.example` -> `.mail.env`

2. 关键依赖来源：
- 以 `requirements.txt` 为准（Docker 与本机一致）

### 3.2 启动服务

```bash
docker compose up -d --build
```

服务：
- `admin-api`：`http://127.0.0.1:8090/admin`
- `scheduler-worker`：常驻调度执行

健康检查：
```bash
curl -fsS http://127.0.0.1:8090/healthz
```

---

## 4. 本机常用命令（CLI）

环境自检：
```bash
python3 -m app.workers.cli env-check
```

验收（全量）：
```bash
python3 -m app.workers.cli acceptance-run --mode full
```

单次采集（不依赖常驻 scheduler）：
```bash
python3 -m app.workers.cli collect-now --force
```

采购探测（仅 probe，不强制写资产）：
```bash
python3 -m app.workers.cli procurement-probe --force true --fetch-limit 5
```

采购探测并写入 collect assets：
```bash
python3 -m app.workers.cli procurement-probe --force true --fetch-limit 5 --write-assets 1
```

---

## 5. 管理台页面

- `/admin/content`：采集/分流/机会指数相关规则
- `/admin/qc`：质控规则
- `/admin/output`：输出规则
- `/admin/scheduler`：调度策略与触发
- `/admin/sources`：信源管理（含分组高级参数）
- `/admin/runs`：运行状态
- `/admin/ops`：Ops Dashboard Lite
- `/admin/versions`：版本对比与回滚

说明：
- 信源分组能力已合并进 `/admin/sources`，`/admin/source-groups` 跳转到 `/admin/sources#groups`。

---

## 6. 信源与抓取模型

### 6.1 Source 配置能力

支持字段（按规则/DB 覆盖）：
- `enabled`, `source_group`, `trust_tier`, `tags`
- `fetch.interval_minutes`（源级覆盖，可继承组默认）
- `fetch.mode`：`rss | html_article | html_list | api_json`

### 6.2 运行时优先级

`DB overrides > rules workspace > YAML defaults`

### 6.3 页面守卫（PR14）

- 静态/栏目页判定器（listing/nav/about/privacy 等）
- article-only 提取约束
- collect 与 digest 双保险 drop

---

## 7. Opportunity Index（H 段）

- 写入路径：`artifacts/opportunity/opportunity_signals-YYYYMMDD.jsonl`
- 去重：同日 `signal_key` 去重
- H 段输出：TopN + `contrib`（Top2 event_type 贡献）
- KPI：
  - `unknown_region_rate`
  - `unknown_lane_rate`
  - `unknown_event_type_rate`
  - `event_type_distribution_topN`

---

## 8. Procurement（采购信号）

- 推荐核心源：`rules/sources/procurement_core_*.yaml`
- 需要 key 的 API 源默认 `enabled=false`
- probe 报告输出：
  - `artifacts/procurement/probe_report-*.json`
  - `artifacts/procurement/probe_report-*.md`

详见：`docs/PROCUREMENT_CORE_SOURCES.md`、`docs/PROCUREMENT_ONBOARDING.md`

---

## 9. 开发与发布建议

- 提交前至少执行：
  ```bash
  python3 -m app.workers.cli acceptance-run --mode full
  ```
- 规则严格化务必保留开关，支持一键回滚。
- 高风险改动优先落在 `enhanced`，`legacy` 保持兼容。

---

## 10. 安全注意事项

- 不要提交：`.mail.env`、`.docker.env`、API key、密码。
- 管理台必须启用鉴权（`ADMIN_USER/ADMIN_PASS` 或 token）。
- 外网源可能波动，优先看 `run_meta`、acceptance 报告与 probe 分类定位问题。

---

## 11. 常见问题与排障

### 11.1 `docker compose up -d --build` 失败（`python:3.11-slim ... EOF`）

现象：
- 拉取 `docker.io/library/python:3.11-slim` 时失败
- 常见报错：`EOF` / `connection reset by peer` / `failed to fetch anonymous token`

原因：
- Docker Hub 链路抖动或本机网络策略（代理/DNS/出口）波动

建议：
```bash
# 先单独拉基础镜像
docker pull python:3.11-slim

# 再重建服务
docker compose up -d --build
```

如仍失败，优先检查：
- Docker Desktop 代理设置
- 镜像加速器配置
- 企业网络出口策略

### 11.2 管理台看起来“代码没更新”

现象：
- 本地代码已修改，但 `/admin` 页面仍是旧文案/旧逻辑

原因：
- 运行中的容器镜像仍是旧版本（仅 `up -d` 不会替换镜像）

处理：
```bash
docker compose up -d --build
```
然后强刷浏览器（`Cmd+Shift+R`）。

### 11.3 信源“编辑已保存但列表没变化”

排查顺序：
1. 先确认容器是否已更新到最新代码（见 11.2）
2. 再检查保存返回：
   - `/admin/api/sources` 返回 `ok=true`
3. 刷新列表后确认 `mode/fetcher/fetch.mode` 是否变化

补充：
- 当前信源管理应以前台编辑结果（registry/overrides）为准。
- 如果出现“写入与展示不一致”，通常是容器未更新或缓存未刷新。
