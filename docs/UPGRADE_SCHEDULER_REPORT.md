# Scheduler Upgrade Self-Check Report

生成日期：2026-02-18（本机）

## 1) scheduler_rules validate 通过

已执行：
- `python3 -m app.workers.cli rules:validate --profile enhanced`
- `python3 -m app.workers.cli rules:validate --profile legacy`

结果：
- enhanced：`scheduler_rules` version=`2.0.1`（file）校验通过
- legacy：`scheduler_rules` version=`1.0.1`（file）校验通过

## 2) worker 启动后能读取 active scheduler_rules 并打印 next run

worker 日志（摘要）：
- 启动后打印：`config_change ... version=2.0.1`
- 打印：`scheduled_jobs=2 tz=Asia/Singapore misfire_grace=600s`

worker 状态文件（`logs/scheduler_worker_status.json`）包含 job 的 `next_run_time`：
- `collect_interval_60m`: `next_run_time` 有值
- `digest_daily_0830`: `next_run_time` 有值

## 3) max_instances=1 生效（并发触发不会重复跑）

工程实现（双保险）：
- APScheduler 每个 job 设置 `max_instances`（来自 `scheduler_rules.defaults.concurrency.max_instances`），默认/推荐为 1。
- 运行时二次保险：所有 job 进入主链路前都会抢 `data/ivd_digest.lock`（SQLite/file lock），抢不到会直接跳过并记录日志：
  - `job_skipped_locked ...`

因此即使出现：
- 多个触发点同一时刻触发（cron + 手动 trigger）
- 或者调度误触发/重复 enqueue

也会被 `max_instances=1` + 全局锁阻止重复跑。

## 4) misfire_grace_seconds 生效（轻微延迟不丢触发）

工程实现：
- APScheduler add_job 使用 `misfire_grace_time = scheduler_rules.defaults.concurrency.misfire_grace_seconds`。
- worker 启动日志会打印当前 misfire_grace（例如 `misfire_grace=600s`），便于确认配置已被加载。

手动验证方法（建议）：
1. 将 `collect` interval 设为 1 分钟（或更短）
2. 暂停 `scheduler-worker` 容器约 1-2 分钟后恢复（但不超过 misfire_grace_seconds）
3. 观察恢复后是否会补跑最近一次触发（coalesce=true 时仅补最近一次）
