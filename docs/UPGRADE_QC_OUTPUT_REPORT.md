# 升级自检报告（QC Rules + Output Rules）

日期：2026-02-17  
仓库路径：`/Users/GY/Documents/New project 1`

本报告覆盖 prompt9 要求的自检项：规则校验、Admin 控制台页面可用性、unified dry-run 输出完整性、关键约束校验、回滚能力验证。

---

## 1) rules:validate（legacy/enhanced，含 qc/output）

执行命令：

```bash
python3 -m app.workers.cli rules:validate --profile legacy
python3 -m app.workers.cli rules:validate --profile enhanced
```

结果摘要（均为 `ok=true`）：

- `legacy`：`email_rules@db:/email_rules/legacy`、`content_rules@db:/content_rules/legacy`、`qc_rules@rules/qc_rules/legacy.yaml`、`output_rules@rules/output_rules/legacy.yaml`
- `enhanced`：`email_rules@db:/email_rules/enhanced`、`content_rules@db:/content_rules/enhanced`、`qc_rules@rules/qc_rules/enhanced.yaml`、`output_rules@rules/output_rules/enhanced.yaml`

补充：全量扫描（包含 workspace + repo fallback）：

```bash
python3 -m app.workers.cli rules:validate
```

应覆盖 `email_rules/content_rules/qc_rules/output_rules` 四套规则集。

---

## 2) Admin 服务页面可打开性（/admin/*）

自检方式：使用 FastAPI `TestClient`（不依赖本机端口监听）模拟浏览器访问并返回 HTTP 状态码。

检查路径与结果：

- `/admin/email`：200
- `/admin/content`：200
- `/admin/qc`：200
- `/admin/output`：200
- `/admin/sources`：200
- `/admin/versions`：200

---

## 3) Unified dry-run 输出字段齐全性

执行命令：

```bash
python3 -m app.workers.cli rules:dryrun --profile enhanced --date 2026-02-16
```

本次 dry-run 运行结果：

- `run_id`：`dryrun-3df1e5d700`
- `artifacts_dir`：`/Users/GY/Documents/New project 1/artifacts/dryrun-3df1e5d700`

返回字段检查（关键 load-bearing 字段）：

- content 统计：`items_before_count` / `items_after_count` / `items_count` / `top_clusters`
- QC：`qc_report`（文件）+ `qc`（对象，含 `pass/fail_reasons/fail_policy/panel`）
- Output：`output_render.json`（A..G 结构化渲染）
- Email 预览：`email_preview.subject_template` / `email_preview.recipients` / `email_preview.preview_text`
- explain：`run_id.json`（含 rulesets/version + explain 汇总）

Artifacts（应存在）：

- `run_id.json`
- `newsletter_preview.md`
- `items.json`
- `qc_report.json`
- `output_render.json`
- `run_meta.json`
- `clustered_items.json`（若启用 story clustering）
- `cluster_explain.json`
- `source_stats.json`
- `event_type_explain.json`（event_type_classifier 可解释输出）

---

## 4) 约束校验

### 4.1 A–F 不含质量指标字段；G 置尾且包含质量指标

验证方式：

- 单测：`tests/test_workers.py::WorkerTests::test_dryrun_generates_artifacts_and_no_send`
- 实跑复核：检查 `artifacts/dryrun-3df1e5d700/newsletter_preview.md`

结果：

- A–F：不包含 `24H条目数/7D补充数/亚太占比/Quality Audit` 等质量指标字段
- G：存在且位于文末（置尾）

### 4.2 多源同事件聚合（story clustering）：只保留主条目，other_sources 保留转载源

验证方式：单测 `tests/test_story_clusterer.py`

覆盖点：

- 3 条相似标题在窗口内聚合成 1 条 primary
- `other_sources` 包含其余来源
- 主条目选择遵循 `source_priority`（例如 Reuters 优先）

### 4.3 APAC 占比不足时触发补齐策略（不重新抓网，仅候选池二次选择）

验证方式：单测 `tests/test_workers.py::WorkerTests::test_qc_apac_share_auto_topup_triggers`

覆盖点：

- 初始候选 APAC 占比不足触发 QC fail
- `fail_policy.mode=auto_topup` 时，从 `clustered_items.json` 的 `7天补充` 候选池补齐
- 补齐后 `apac_share >= apac_min_share`，`qc.pass=true` 且 `action_taken=auto_topup`

---

## 5) 回滚验证（qc/output）

验证方式：单测 `tests/test_rules_store.py::RulesStoreTests::test_qc_and_output_rules_draft_publish_active_rollback`

覆盖点：

- `qc_rules`：draft -> publish(新版本激活) -> publish(再发新版本) -> rollback（恢复上一 active）
- `output_rules`：同上

---

## 6) 结论

- 规则校验：`legacy/enhanced` 均可通过（含 `qc_rules/output_rules`），并保持边界断言有效。
- 控制台页面：`/admin/email/content/qc/output/sources/versions` 均可访问（200）。
- dry-run：可实跑，输出字段与 artifacts 齐全；A–F/G 结构约束满足。
- story 聚合与 APAC 补齐策略：均有可解释实现与单测覆盖。
- qc/output 版本发布与回滚：单测验证通过。

