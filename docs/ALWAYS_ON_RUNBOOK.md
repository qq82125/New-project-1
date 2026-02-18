# Always-On Runbook（常驻模式运维手册）

本文档面向“常驻在线系统”模式：`admin-api` + `scheduler-worker` 以 Docker Compose 方式长期运行。

## 1. 启动与重建

在项目根目录执行：

```bash
cd "/Users/GY/Documents/New project 1"
docker compose up -d --build
```

访问控制台：
- `http://127.0.0.1:8090/admin`

说明：
- 8090 是宿主机端口，容器内 `admin-api` 监听 8789。
- 控制台鉴权沿用 `ADMIN_TOKEN` 或 `ADMIN_USER/ADMIN_PASS`（见 `docs/RULES_CONSOLE.md`）。

## 2. 在 /admin/scheduler 配置调度并验证生效

1) 打开：`/admin/scheduler`

2) 配置并发布（Draft -> Publish）：
- `enabled`: `true`
- `timezone`: 推荐 `Asia/Shanghai`（如你希望按北京时间出报）；也可用 `Asia/Singapore`
- `schedules`：
  - `cron`（出日报）：例如 `0 8 * * *` 表示每天 08:00（按 timezone）
  - `interval`（采集）：例如 `60` 表示每 60 分钟跑一次 collect
- `concurrency`：
  - `max_instances`: `1`
  - `coalesce`: `true`
  - `misfire_grace_seconds`: 例如 `600`

3) 验证是否生效（无需重启容器）：
- 页面右侧“下次运行时间”应出现每个 job 的 `next_run_time`
- 也可调用 API：`GET /admin/api/scheduler/status`

备注：
- Publish/Rollback 会触发 worker reload（通过 `data/scheduler_reload.signal`），通常 5 秒内生效。

## 3. 在 /admin/sources 设置 interval/rate_limit 并观察 last_fetched_at

1) 打开：`/admin/sources`

2) 找到某个 source（或编辑/新增）：
- `fetch.interval_minutes`：该信源最小抓取间隔（分钟）
- `rate_limit.rps/burst`：采集侧限速参数（用于抓取端节流）

3) 保存后，等待下一次 `collect` job 运行：
- 列表“最近抓取”会更新：
  - `last_fetched_at`: 最近一次实际抓取时间（仅在 due 且抓取尝试发生时更新）
  - `last_fetch_status`: `成功/失败/跳过`
    - `跳过`：未到间隔，不会更新 `last_fetched_at`（只会更新状态字段）

## 4. 手动 Trigger Now

在 `/admin/scheduler` 页面：
- 点击 `Trigger Now: collect`：立刻跑一次 per-source collect（trigger=manual）
- 点击 `Trigger Now: digest`：立刻跑一次日报主链路（trigger=manual，且会发信，取决于 email_rules）

也可以用 API：
```bash
curl -u admin:your_pass \
  -H "Content-Type: application/json" \
  -X POST "http://127.0.0.1:8090/admin/api/scheduler/trigger" \
  -d '{"profile":"enhanced","purpose":"collect","schedule_id":"manual"}'
```

## 5. 查看 artifacts/run_meta.json 与最近失败原因

1) 运行产物目录：
- 日报链路：`artifacts/run-*/run_meta.json`（或 `artifacts/dryrun-*/run_meta.json`）
- collect 链路：`artifacts/collect-*/run_meta.json`

2) 常看字段：
- `run_id`
- `trigger`: `schedule` 或 `manual`
- `schedule_id`
- `profile`
- `rules_version`（若该 run 记录了四套规则版本）
- `status` / `error`（若有）

3) 最近失败原因定位：
- `docker compose logs --tail=200 scheduler-worker`
- `docker compose logs --tail=200 admin-api`
- 信源抓取失败详情通常在 sources 的 `last_fetch_error/last_fetch_http_status` 里体现（控制台列表会显示“异常/HTTP码” pill）。

## 6. 回滚 scheduler_rules / sources 改动

### 6.1 回滚 scheduler_rules（推荐）
方式 1：控制台
- 打开 `/admin/versions`，点击“回滚调度规则”

方式 2：API
```bash
curl -u admin:your_pass \
  -H "Content-Type: application/json" \
  -X POST "http://127.0.0.1:8090/admin/api/scheduler_rules/rollback" \
  -d '{"profile":"enhanced"}'
```

回滚后 worker 会自动 reload，`/admin/scheduler` 的 next_run_time 会随之更新。

### 6.2 回滚 sources（目前是“DB 即事实”）
Sources 当前以 SQLite 为准，不走版本表；推荐做法：
- 小改动：在 `/admin/sources` 直接改回原值或停用（enabled=false）
- 大批量改动：改动前备份 `data/rules.db`（或使用 sqlite 备份），需要回滚时恢复该文件，然后 `docker compose up -d` 让进程重新读取。

