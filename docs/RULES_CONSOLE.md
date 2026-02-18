# Rules Console / Admin API

## 目标
在不影响现有脚本（如 `send_mail_icloud.sh`、GitHub Actions）前提下，提供一套可鉴权的规则管理 API：
- 邮件规则：查看 active / 提交 draft / 发布 / 回滚
- 采集规则：查看 active / 提交 draft / 发布 / 回滚
- 质控规则：查看 active / 提交 draft / 发布 / 回滚
- 输出规则：查看 active / 提交 draft / 发布 / 回滚
- 信源管理：查询 / 新增或编辑 / 启停 / 测试抓取

服务入口：`app/web/rules_admin_api.py`

## 启动方式
最小启动命令（本地）：
```bash
cd "/Users/GY/Documents/New project 1"
ADMIN_USER=admin ADMIN_PASS='your_strong_password' python3 -m app.web.rules_admin_api
```

默认监听：
- `ADMIN_API_HOST=127.0.0.1`
- `ADMIN_API_PORT=8789`

可通过环境变量覆盖：
```bash
export ADMIN_API_HOST=127.0.0.1
export ADMIN_API_PORT=8789
python3 -m app.web.rules_admin_api
```

## 鉴权
支持两种方式（优先 Bearer Token）：

1. Bearer Token
```bash
export ADMIN_TOKEN="your_token"
```
请求头：
`Authorization: Bearer your_token`

2. Basic Auth
```bash
export ADMIN_USER="admin"
export ADMIN_PASS="your_strong_password"
```
请求示例：
`curl -u admin:your_strong_password ...`

说明：
- 若未配置上述鉴权变量，仅允许本机回环地址访问（127.0.0.1/::1）。
- 若部署到公网，必须配置 `ADMIN_TOKEN` 或 `ADMIN_USER/ADMIN_PASS`。

## 路由
### 管理页面（轻前端）
- `GET /admin/email`
- `GET /admin/content`
- `GET /admin/qc`
- `GET /admin/output`
- `GET /admin/sources`
- `GET /admin/versions`

页面特性：
- 所有“保存”先提交 draft 并返回校验结果。
- 仅当 draft 校验通过时，页面才启用 publish 按钮。
- publish 成功后会显示“已生效版本号”。

右侧预览：
- Email 页面：dry-run 预览生成的邮件内容。
- Content 页面：dry-run 预览候选条数、去重后条数、top clusters。
- QC/Output 页面：dry-run 预览 QC 面板 + A–G 邮件预览（不发信）。

### Email Rules
- `GET /admin/api/email_rules/active?profile=enhanced`
- `POST /admin/api/email_rules/draft`
- `POST /admin/api/email_rules/publish`
- `POST /admin/api/email_rules/rollback`
- `POST /admin/api/email_rules/dryrun`

### Content Rules
- `GET /admin/api/content_rules/active?profile=enhanced`
- `POST /admin/api/content_rules/draft`
- `POST /admin/api/content_rules/publish`
- `POST /admin/api/content_rules/rollback`
- `POST /admin/api/content_rules/dryrun`

### QC Rules
- `GET /admin/api/qc_rules/active?profile=enhanced`
- `POST /admin/api/qc_rules/draft`
- `POST /admin/api/qc_rules/publish`
- `POST /admin/api/qc_rules/rollback`
- `POST /admin/api/qc_rules/dryrun`（可选：只返回 qc_report）

### Output Rules
- `GET /admin/api/output_rules/active?profile=enhanced`
- `POST /admin/api/output_rules/draft`
- `POST /admin/api/output_rules/publish`
- `POST /admin/api/output_rules/rollback`
- `POST /admin/api/output_rules/dryrun`（可选：只返回 output_render）

### Unified Dry-run（推荐）
- `POST /admin/api/dryrun?date=YYYY-MM-DD&profile=enhanced`
  - 返回：`items_before/items_after`、`preview_text/preview_html`、`qc_report`、`output_render`、`run_meta`、`explain`（以及 artifacts 路径），适合“一键预览 + 审计”。

### Sources
- `GET /admin/api/sources`
- `POST /admin/api/sources`
- `POST /admin/api/sources/{id}/toggle`
- `POST /admin/api/sources/{id}/test`

### Versions
- `GET /admin/api/versions?profile=enhanced`
- `GET /admin/api/versions/diff?ruleset=email_rules&profile=enhanced&from_version=...&to_version=...`

## 运营流程（推荐）
1) 改规则（页面表单）
2) 保存草稿（draft 校验）
3) dry-run 预览（不发信）
4) publish 发布生效（active 指针切换）
5) 版本回滚（rollback 到上一版本）

## Draft -> Publish -> Rollback 示例
### 1) 提交 draft（email_rules）
```bash
curl -u admin:your_pass \
  -H "Content-Type: application/json" \
  -X POST "http://127.0.0.1:8789/admin/api/email_rules/draft" \
  -d '{
    "profile": "enhanced",
    "created_by": "ops",
    "config_json": {
      "ruleset": "email_rules",
      "version": "2.0.0",
      "profile": "enhanced",
      "defaults": {
        "timezone": "Asia/Shanghai",
        "subject_template": "全球IVD晨报 - {{date}}",
        "recipient": "qq82125@gmail.com",
        "send_window": {"hour": 8, "minute": 30},
        "retry": {"max_retries": 3, "connect_timeout_sec": 10, "max_time_sec": 60}
      },
      "overrides": {"enabled": true},
      "rules": [],
      "output": {"format": "plain_text", "sections": ["A","B","C","D","E","F","G"], "summary_max_chars": 200, "charts_enabled": false}
    }
  }'
```

返回中的 `draft.id` 用于发布。

### 2) 发布 draft（激活新版本）
```bash
curl -u admin:your_pass \
  -H "Content-Type: application/json" \
  -X POST "http://127.0.0.1:8789/admin/api/email_rules/publish" \
  -d '{"profile":"enhanced","draft_id":123,"created_by":"ops"}'
```

### 3) 回滚到上一版本
```bash
curl -u admin:your_pass \
  -H "Content-Type: application/json" \
  -X POST "http://127.0.0.1:8789/admin/api/email_rules/rollback" \
  -d '{"profile":"enhanced"}'
```

## QC/Output 最小 curl 示例
### 读取 active（qc_rules）
```bash
curl -u admin:your_pass \
  "http://127.0.0.1:8789/admin/api/qc_rules/active?profile=enhanced"
```

### 读取 active（output_rules）
```bash
curl -u admin:your_pass \
  "http://127.0.0.1:8789/admin/api/output_rules/active?profile=enhanced"
```

## Sources 示例
### 新增/编辑 source
```bash
curl -u admin:your_pass \
  -H "Content-Type: application/json" \
  -X POST "http://127.0.0.1:8789/admin/api/sources" \
  -d '{
    "id":"demo-api-source",
    "name":"Demo API Source",
    "connector":"api",
    "url":"",
    "enabled":true,
    "priority":50,
    "trust_tier":"B",
    "tags":["demo","api"],
    "rate_limit":{"rps":1,"burst":1}
  }'
```

## Secret / auth_ref（不存明文）
Sources 支持通过 `fetch.auth_ref` 引用环境变量名，用于请求鉴权：
- `auth_ref` 只能是环境变量名（例如 `NMPA_API_KEY`），不会把密钥明文写入 DB。
- 运行时会从 env 读取该变量并注入到请求头（默认写入 `Authorization`）。
  - 若 env 值包含空格（例如 `Bearer xxx` / `Basic yyy`），将原样写入。
  - 若 env 值不含空格（例如纯 token），将按 `Bearer <token>` 写入。
- 控制台 UI 只显示“鉴权✓/鉴权×”（是否已配置），不会展示明文。

### Docker Compose 里配置 env（推荐）
本项目默认用 `docker-compose.yml` 加载 `.docker.env`：
1) 在 `.docker.env` 增加一行（示例）：
```bash
NMPA_API_KEY=your_real_secret_here
```
2) 重启容器让 env 生效（env 变化需要重启进程才能读取到）：
```bash
docker compose up -d
```

### 启停 source
```bash
curl -u admin:your_pass \
  -H "Content-Type: application/json" \
  -X POST "http://127.0.0.1:8789/admin/api/sources/demo-api-source/toggle" \
  -d '{"enabled":false}'
```

### 测试 source（抓取/解析 3 条样例）
```bash
curl -u admin:your_pass \
  -X POST "http://127.0.0.1:8789/admin/api/sources/demo-api-source/test"
```

## Unified Dry-run 示例
```bash
curl -u admin:your_pass \
  -X POST "http://127.0.0.1:8789/admin/api/dryrun?profile=enhanced&date=2026-02-16" | python3 -m json.tool
```

## 常见问题（FAQ）
1) QC fail 时怎么处理？
- 先看 dry-run 返回的 `qc_report.fail_reasons` 与 `qc_report.panel`（区域占比、重复率、必查信源缺口、event mix 等）。
- 再决定改动点：信源（/admin/sources）、采集过滤（/admin/content）、阈值/策略（/admin/qc）、渲染降级（/admin/output）。

2) 为什么 G 必须在末尾？
- A–F 是业务正文，G 是质量审计；系统强约束并有单测保证 “A–F 不含质量指标字段，G 置尾”。

3) 去重聚合如何选主来源？
- 由 `content_rules` 的 `source_priority` + `dedupe_cluster.primary_select` 决定；其他来源保留在 primary 的 `other_sources[]`，并在聚合 explain 中可审计。

## 错误结构
校验失败时返回结构化错误：
```json
{
  "ok": false,
  "error": {
    "code": "SOURCE_VALIDATION_FAILED",
    "details": [
      {"path": "$.url", "message": "url 非法或为空"}
    ]
  }
}
```

规则 schema 校验失败时，`draft.validation_errors` 中包含字段路径和可读错误信息。
