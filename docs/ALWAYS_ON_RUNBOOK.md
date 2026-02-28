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
  - `cron`（digest 出日报）：例如 `30 8 * * *` 表示每天 08:30（按 timezone）
  - `interval`（collect 采集）：例如 `60` 表示每 60 分钟跑一次 collect（只抓取并落资产）
- `concurrency`：
  - `max_instances`: `1`
  - `coalesce`: `true`
  - `misfire_grace_seconds`: 例如 `600`

3) 验证是否生效（无需重启容器）：
- 页面右侧“下次运行时间”应出现每个 job 的 `next_run_time`
- 也可调用 API：`GET /admin/api/scheduler/status`

备注：
- Publish/Rollback 会触发 worker reload（通过 `data/scheduler_reload.signal`），通常 5 秒内生效。
- collect 与 digest 已分离：collect 高频、digest 低频。collect 不会发信，digest 才会聚合并发信。

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
- 点击 `Trigger Now: digest`：立刻跑一次 digest（从 collect 资产窗口读取、聚合渲染并发信）

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
- collect 资产池：`artifacts/collect/items-YYYYMMDD.jsonl`（按天滚动）

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
- 如果“collect 正常但 digest 无输出”，优先检查：
  - `rules/scheduler_rules/*.yaml` 中 `collect_window_hours` 是否过小
  - `collect_asset_dir` 目录是否正确（默认 `artifacts/collect`）
  - `artifacts/collect/items-YYYYMMDD.jsonl` 是否有最近窗口内条目

## 6. collect / digest CLI 快速自检（推荐容器内执行）

```bash
docker compose exec -T scheduler-worker sh -lc 'python -m app.workers.cli collect-now --profile enhanced --limit-sources 20'
docker compose exec -T scheduler-worker sh -lc 'python -m app.workers.cli collect-now --profile enhanced --force true --fetch-limit 50'
docker compose exec -T scheduler-worker sh -lc 'python -m app.workers.cli collect-clean --keep-days 30'
docker compose exec -T scheduler-worker sh -lc 'python -m app.workers.cli digest-now --profile enhanced --send false --use-collect-assets true'
docker compose exec -T scheduler-worker sh -lc 'python -m app.workers.cli analysis-clean --keep-days 30'
docker compose exec -T scheduler-worker sh -lc 'python -m app.workers.cli analysis-recompute --model primary --prompt-version v2 --sample 20'
```

说明：
- `collect-now`：真实抓取信源条目并写入 `artifacts/collect/*.jsonl`
- `--limit-sources N`：仅跑前 N 个启用信源（联调更快）
- `--force true`：忽略最小抓取间隔（用于手工验收）
- `--fetch-limit 50`：每个信源最多拉取条目数
- `collect-clean`：按保留天数清理历史 collect 资产文件
- `analysis-clean`：按保留天数清理分析缓存 `artifacts/analysis/*.jsonl`
- `analysis-recompute`：对缓存样本按新模型/新 prompt_version 重算并输出对比报告
- `digest-now`：从 collect 资产读窗口（默认 24h）生成日报；`--send false` 不发信，仅验证渲染链路
- 可选参数：`--collect-window-hours 48 --collect-asset-dir artifacts/collect`
- 若必须在宿主机本地执行 `digest-now --send true`，先运行 `python -m app.workers.cli env-check`。当检查不通过时，CLI 会默认阻断本地发信并提示改用容器命令（可用 `--allow-local-send true` 强制绕过）。

### 6.0 一键命令（Makefile）

```bash
make collect-container
make digest-container
make digest-send-container
make fallback-drill-container
```

- `fallback-drill-container`：演练主发送失败后的兜底通道（`MAIL_SEND_FORCE_FAIL=1`），用于验证 failover 流程。

## 6.1 云端兜底联调（Failover Drill）

目标：确认 `fallback_triggered=true` 且 `fallback_ok=true`。

```bash
docker compose up -d --build
docker compose exec -T scheduler-worker sh -lc 'MAIL_SEND_FORCE_FAIL=1 python -m app.workers.cli digest-now --profile enhanced --send true'
```

验收字段（命令输出 JSON）：
- `fallback_triggered: true`
- `fallback_ok: true`
- `send_failure_nonfatal: true`

## 6.1 发布前闸门（防止模板被误改）

每次发布规则前，固定执行：

```bash
python3 scripts/prepublish_guard.py --full --strict
```

闸门包含：
- `rules:validate --profile enhanced`
- `tests/test_h_template_lock_pr23_2.py`（A-H 模板锁）
- `acceptance-run --mode full`

任一失败都不要发布，先修复再重新跑。

Analysis cache 说明（digest）：
- 默认启用：`content_rules.defaults.analysis_cache.enable_analysis_cache=true`
- digest 优先读 cache（命中不重复生成），miss 才生成并写回
- 失败降级：不会中断出报；G 段会显示 `degraded_count` 与原因 TopN

常见排障（collect）：
- 权限问题：`artifacts/collect` 无写权限会导致 `run_meta.json.errors` 出现 `append_failed`
- 磁盘问题：磁盘满时 `assets_written_count` 异常下降且 `errors` 增加
- 去重过高：连续两次运行 `deduped_count` 明显增加属于正常，表示同 URL 未重复刷盘

## 7. 回滚 scheduler_rules / sources 改动

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
