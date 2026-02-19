# New-project-1: 全球 IVD 晨报（规则控制台 + 常驻调度 + 云端兜底）

一个面向 IVD 行业前沿的自动化系统：按固定 A–G 结构生成《全球 IVD 晨报》，支持本机或 Docker 常驻运行，提供网页规则控制台（草稿校验、试跑预览、发布生效、版本回滚），并保留 GitHub Actions 作为云端兜底补发。

默认保持 `legacy` 行为不变；增强能力（`enhanced` 规则、常驻调度、质控/输出规则等）均可灰度启用。

---

## 1. 能做什么

- **晨报生成与发信**：生成晨报内容，通过 `send_mail_icloud.sh` 走 iCloud SMTP 发送。
- **网页规则控制台（/admin）**：编辑/校验/预览/发布/回滚；运行侧只读取“已发布版本”。
- **常驻在线模式（Docker Compose）**：
  - `admin-api`：规则控制台（FastAPI）
  - `scheduler-worker`：常驻调度（读取 `scheduler_rules`），并带单并发锁，避免重复跑
- **云端兜底补发（GitHub Actions: IVD Cloud Backup Mail）**：
  - 即使缺少 secrets、IMAP/SMTP 失败，也会生成 `reports/*.txt` 诊断并上传 artifact，便于定位
- **信源管理（sources registry）**：sources 独立为 registry，支持启用/禁用/test。
- **故事级聚合（story clustering）**：多源同事件只保留 1 条主条目，其余挂到 `other_sources`，并输出 explain。

---

## 2. 关键入口与目录

- 生成日报脚本：`scripts/generate_ivd_report.py`
- 发信脚本（SMTP）：`send_mail_icloud.sh`（读取同目录的 `.mail.env`）
- 管理台服务入口：`app/admin_server.py`（FastAPI）
- 调度 Worker：`app/workers/scheduler_worker.py`（APScheduler）
- 规则与 Schema：`rules/`、`rules/schemas/`
- 文档：
  - `docs/RULES_SYSTEM.md`（规则系统与边界）
  - `docs/RULES_CONSOLE.md`（控制台与 API）
  - `docs/ALWAYS_ON_RUNBOOK.md`（常驻运行手册）
  - `CLOUD_BACKUP_SETUP.md`（云端兜底配置）

---

## 3. 快速开始（推荐：Docker 常驻）

### 3.1 配置环境变量

1. Docker 环境变量：复制 `.docker.env.example` 为 `.docker.env` 并填写（至少包含管理台登录 `ADMIN_USER/ADMIN_PASS`）。
2. SMTP 发信：复制 `.mail.env.example` 为 `.mail.env` 并填写（不要提交到 git）。

`.mail.env` 示例：

```bash
SMTP_HOST=smtp.mail.me.com
SMTP_PORT=587
SMTP_USER=your_mail@me.com
SMTP_PASS=your_app_password
SMTP_FROM=your_mail@me.com
SMTP_FROM_NAME=全球IVD晨报
```

### 3.2 启动

建议先做一键预检：

```bash
./scripts/docker_preflight.sh
# 或
make docker-preflight
```

可选参数：

```bash
SKIP_PULL=1 ./scripts/docker_preflight.sh
```

说明：`SKIP_PULL=1` 会跳过公网拉取测试（适合离线环境）。

预检通过后再执行：

```bash
docker compose up -d --build
```

访问管理台：
- `http://127.0.0.1:8090/admin`

健康检查：
- `GET http://127.0.0.1:8090/healthz`

说明：
- `docker-compose.yml` 会把 `./app`、`./rules`、`./data`、`./artifacts`、`./logs` 挂载进容器，方便迭代。
- `scripts/` 和根目录脚本（如 `send_mail_icloud.sh`）在镜像内，改动后需要 `--build` 才会生效。

---

## 4. 控制台怎么用（运营 SOP）

### 4.1 发布规则（不会立刻发信）

通用流程（适用于 `/admin/email`、`/admin/content`、`/admin/qc`、`/admin/output`、`/admin/scheduler`）：

1. 修改左侧表单
2. 点击 `保存草稿并校验`
3. 校验通过后点击 `发布生效`
4. 如发现问题：进入 `/admin/versions` 对对应 ruleset 一键回滚

### 4.2 试跑预览（不发信）

各页面右侧的 `试跑预览(不发信)` 会执行 dry-run，常见输出包括：
- content 统计：候选/聚合后/入选数量
- qc 报告：pass/fail + 指标面板
- output 渲染：A–G 预览（`G` 必须置尾）
- email 预览：subject/recipients/preview

### 4.3 立刻执行发信（实跑）

进入 `/admin/scheduler`：
- `Trigger Now`：立即触发一次“采集→生成→发信”（真实发信）
- `Pause/Resume`：暂停/恢复自动调度

查看结果：
- `/admin` 运行状态页
- `artifacts/<run_id>/run_meta.json`

---

## 5. 本机运行（不使用 Docker）

启动管理台（本机）：

```bash
ADMIN_USER=admin ADMIN_PASS='change-me' python3 -m app.admin_server
```

或：

```bash
./scripts/run_admin.sh
```

访问：
- `http://127.0.0.1:8789/admin`

（可选）旧版规则控制台：

```bash
./scripts/run_rules_console.sh
```

---

## 6. GitHub Actions 云端兜底（IVD Cloud Backup Mail）

用途：当常驻运行/本机网络异常导致晨报未按时到达时，在云端进行补发。

特点（可靠兜底）：
- 任意失败路径都会产出 `reports/*.txt` 诊断文件并上传 artifact。
- `scripts/cloud_backup_send.py` 启动会做 env 自检，缺失项会写报告并以非 0 退出码失败（CI 可见）。

配置方式见：
- `CLOUD_BACKUP_SETUP.md`

本地自检（dry-run，不连接 IMAP/SMTP，只生成诊断报告）：

```bash
python3 scripts/cloud_backup_send.py --dry-run --date 2026-02-16
```

诊断输出：
- `reports/ivd_backup_YYYY-MM-DD.txt`

---

## 7. 安全与注意事项

- 不要把 `.mail.env`、`.docker.env`、任何 app 专用密码、token、PAT 提交到仓库。
- 管理台鉴权依赖 `ADMIN_TOKEN` 或 `ADMIN_USER/ADMIN_PASS`；对公网暴露前必须开启鉴权。
- sources/web 抓取受网络与站点结构影响；建议先在 `/admin/sources` 里 `test` 再启用。

---

## 8. 信源健康巡检（Sources Test Harness）

可对 `sources_registry.v1.yaml` 中启用信源做全量可重复测试，并导出 JSON + Markdown 报告：

```bash
python3 -m app.workers.cli sources:test \
  --enabled-only \
  --limit 3 \
  --workers 6 \
  --timeout-seconds 20 \
  --retries 2 \
  --json-out artifacts/sources_test.json \
  --md-out artifacts/sources_test.md
```

单源测试（与 `/admin/sources/{id}/test` 同逻辑）：

```bash
python3 -m app.workers.cli sources:test --source-id reuters-health-rss --limit 3
```

可回滚开关（默认都为 `true`）：

- `SOURCES_RSS_DISCOVERY_ENABLED`：RSS 自动发现
- `SOURCES_INDEX_DISCOVERY_ENABLED`：RSS 索引页子 feed 发现
- `SOURCES_HTML_FALLBACK_ENABLED`：HTML 通用列表抓取

示例（关闭某能力）：

```bash
SOURCES_HTML_FALLBACK_ENABLED=false python3 -m app.workers.cli sources:test --enabled-only
```

---

## 9. 数据库迁移（SQLite -> PostgreSQL）

已支持控制面数据库底座切换与灰度：

- `DATABASE_URL`：主库（可 PostgreSQL 或 SQLite）
- `DATABASE_URL_SECONDARY`：影子库（灰度对比/双写）
- `DB_WRITE_MODE=single|dual`
- `DB_READ_MODE=primary|shadow_compare`
- `DB_DUAL_STRICT=false|true`（默认 `false`，secondary 写失败仅告警）

一键脚本：

```bash
# 预检 + 迁移 + 校验 + 双库回放对比
DATABASE_URL='postgresql+psycopg://USER:PASS@HOST:5432/DB' \
DATABASE_URL_SECONDARY='sqlite:///data/rules.db' \
./scripts/db_cutover.sh go

# 输出 dual 模式 env
./scripts/db_cutover.sh enable-dual

# 输出回滚 SQLite env
./scripts/db_cutover.sh rollback
```

详见：`docs/DB_MIGRATION_PLAN.md` 与 `docs/DB_SCHEMA.md`。

## 9. License

MIT
