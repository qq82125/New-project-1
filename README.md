# Global IVD Morning Briefing Automation

一个面向 IVD 行业情报的自动化项目：按固定结构生成全球 IVD 晨报，并通过 iCloud SMTP 发送邮件，同时提供 GitHub Actions 云端兜底补发能力。

## Key Features

- 每日生成并发送《全球 IVD 晨报》（支持固定主题格式）。
- 本机 SMTP 发送脚本（iCloud SMTP，支持 UTF-8 正文）。
- GitHub Actions 云端兜底：检测当天是否已发送，未发送则自动补发。
- 自动化同步工具：提供本地 `git` 自动同步与 post-commit 自动推送能力。
- 配套部署文档，便于快速落地到个人仓库。

## Tech Stack

- `Bash`（自动化脚本）
- `Python 3`（云端兜底发送逻辑）
- `GitHub Actions`（定时任务与手动触发）
- `iCloud SMTP/IMAP`（邮件发送与“已发送”检测）
- `Git`（版本管理与同步）

## Getting Started

### 1. Clone 项目

```bash
git clone https://github.com/qq82125/New-project-1.git
cd New-project-1
```

### 2. 安装依赖

本项目核心脚本使用 Python 标准库，无强制第三方依赖。  
如需使用脚本化方式调用 GitHub Secrets 加密（高级用法），可安装：

```bash
pip install pynacl
```

### 3. 运行项目

1) 配置本机 SMTP 环境（示例 `.mail.env`）：

```bash
SMTP_HOST=smtp.mail.me.com
SMTP_PORT=587
SMTP_USER=your_mail@me.com
SMTP_PASS=your_app_password
SMTP_FROM=your_mail@me.com
SMTP_FROM_NAME=全球IVD晨报
```

2) 发送测试邮件：

```bash
./send_mail_icloud.sh qq82125@gmail.com "全球IVD晨报 - 2026-02-13（测试）" ./ivd_morning_2026-02-13.txt
```

3) 启用云端兜底（GitHub Actions）：
- 参考 `CLOUD_BACKUP_SETUP.md` 配置 Secrets。
- 在 Actions 中运行 `IVD Cloud Backup Mail` 做一次验证。

## Usage Example

### 命令行发送示例

```bash
./send_mail_icloud.sh \
  qq82125@gmail.com \
  "全球IVD晨报 - 2026-02-13" \
  ./ivd_morning_2026-02-13.txt
```

### 云端兜底逻辑（简述）

1. 每天 08:40（北京时间）触发 GitHub Actions。  
2. 先检查 iCloud「已发送」是否存在当日主题邮件。  
3. 若不存在，则自动补发到目标邮箱。  

## Rules Console

提供网页规则控制台（编辑/校验/预览/发布/回滚），不影响 `send_mail_icloud.sh` 与 GitHub Actions 兜底补发逻辑。

### 1) 启动 /admin 控制台（FastAPI，推荐）

本地启动（命令行一次性注入账号密码）：

```bash
ADMIN_USER=admin ADMIN_PASS=change-me python -m app.admin_server
```

或使用脚本（会自动读取同目录的 `.admin_api.env`，如存在）：

```bash
./scripts/run_admin.sh
```

打开页面：

- `http://127.0.0.1:8789/admin`（会跳转到 `/admin/email`）

### Docker Compose（常驻模式）

本项目提供 `docker-compose.yml`（两服务）：
- `admin-api`：规则控制台（FastAPI）
- `scheduler-worker`：常驻调度（读取 `scheduler_rules`）

启动：

```bash
docker compose up -d --build
```

健康检查：
- `admin-api`：`GET /healthz`
- `scheduler-worker`：每分钟写入 `/app/logs/scheduler_worker_heartbeat.json`（compose healthcheck 会检查更新时间）

访问：
- Docker 模式默认映射到 `http://127.0.0.1:8790/admin`（容器内是 8789；避免与本机 launchd 8789 冲突）

说明：
- 常驻模式启用后，GitHub Actions 仍保留作为兜底/可选停用（不删除）。
- 若要让容器内 digest 任务实际发信，需要提供 `TO_EMAIL` 与 SMTP 环境变量（`send_mail_icloud.sh` 所需）。

### 2) 在浏览器配置规则（Draft -> Publish）

通用流程（邮件规则/采集规则一致）：

1. 打开 `/admin/email` 或 `/admin/content`
2. 修改表单
3. 点击“保存草稿并校验”
4. 校验通过后点击“发布生效”

版本管理：

- 打开 `/admin/versions` 查看生效版本、对比差异、以及“一键回滚到上一版本”

### 3) dry-run 预览（不发信）

在页面上：

- `/admin/email` 右侧点击“试跑预览(不发信)”
- `/admin/content` 右侧点击“试跑预览(不发信)”查看候选条数与聚合簇

API 一键 dry-run（推荐）：

```bash
curl -u admin:change-me \
  -X POST "http://127.0.0.1:8789/admin/api/dryrun?profile=enhanced&date=2026-02-16" | python3 -m json.tool
```

### 4) 发布/回滚

- 发布：页面在 Draft 校验通过后，点击“发布生效”
- 回滚：进入 `/admin/versions` 点击“回滚邮件规则 / 回滚采集规则”

详细 API 与示例：见 `docs/RULES_CONSOLE.md`。

### 5) 旧版 Rules Console（8787）

历史版本的简易控制台仍保留，可用于对比/兼容验证：

```bash
export RULES_CONSOLE_USER=admin
export RULES_CONSOLE_PASS=change-me
python -m app.web.rules_console
```

打开 `http://127.0.0.1:8787`。  
该服务主要用于兼容验证；主运行侧规则读取以 DB active 版本为优先（失败回退到 `rules/legacy.*`）。

## Contributing

欢迎提交 Issue 和 Pull Request。建议先描述问题背景与预期行为，再提交最小可复现改动，便于快速评审和合并。

## License

This project is licensed under the MIT License.
