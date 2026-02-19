# DB Migration Plan (SQLite -> PostgreSQL)

本文档描述控制面数据库从 SQLite 迁移到 PostgreSQL 的标准流程，包含断点续跑、校验、灰度、切换与回滚。

## 10分钟快速验收版（推荐先跑）

目标：快速确认“已切 PG 主库 + single/primary + 关键唯一约束生效”。

1. 执行迁移并重启：

```bash
PYTHONPATH=. alembic upgrade head
docker compose up -d --build
```

2. 验证运行模式：

```bash
docker compose exec -T admin-api /bin/sh -lc 'echo DATABASE_URL=$DATABASE_URL; echo DATABASE_URL_SECONDARY=$DATABASE_URL_SECONDARY; echo DB_WRITE_MODE=$DB_WRITE_MODE; echo DB_READ_MODE=$DB_READ_MODE'
docker compose exec -T admin-api /bin/sh -lc 'curl -fsS http://127.0.0.1:8789/healthz'
```

通过标准：
- `DATABASE_URL` 指向 PG
- `DATABASE_URL_SECONDARY` 为空
- `DB_WRITE_MODE=single`
- `DB_READ_MODE=primary`
- `healthz.db_backend=postgresql`

3. 验证 DB 唯一约束：

```bash
docker compose exec -T db psql -U ivd -d ivd -c "SELECT conname, conrelid::regclass::text AS table_name FROM pg_constraint WHERE conname IN ('uq_send_attempts_send_key','uq_dedupe_keys_dedupe_key','uq_email_rules_profile_version','uq_content_rules_profile_version','uq_qc_rules_profile_version','uq_output_rules_profile_version','uq_scheduler_rules_profile_version') ORDER BY conname;"
docker compose exec -T db psql -U ivd -d ivd -c "SELECT indexname,indexdef FROM pg_indexes WHERE tablename='run_executions' AND indexname='uq_run_executions_run_key';"
```

通过标准：
- `send_key`、`dedupe_key`、`profile+version` 约束存在
- `run_key` 唯一索引存在

4. 失败即回滚（1 分钟）：

```bash
export DATABASE_URL='sqlite:///data/rules.db'
unset DATABASE_URL_SECONDARY
export DB_WRITE_MODE='single'
export DB_READ_MODE='primary'
docker compose up -d --build
```

## 关键开关

- `DATABASE_URL`：主库连接串（切到 PG 后指向 PG）
- `DATABASE_URL_SECONDARY`：影子库连接串（通常保留 SQLite）
- `DB_WRITE_MODE`：`single | dual`
- `DB_READ_MODE`：`primary | shadow_compare`

## Phase 0：PG 建库 + Alembic

1. 准备 PG 数据库和账号。
2. 执行迁移：

```bash
export DATABASE_URL='postgresql+psycopg://USER:PASS@HOST:5432/DB'
alembic upgrade head
```

## Phase 1：全量迁移（可断点续跑）

```bash
python3 -m app.workers.cli db:migrate \
  --from sqlite:///data/rules.db \
  --to "$DATABASE_URL" \
  --batch-size 1000 \
  --checkpoint data/db_migrate_checkpoint.json
```

说明：
- 按表 + 主键分批迁移。
- checkpoint 文件记录每表 `last_id`，中断后可续跑。
- 迁移前会做 unique 冲突预检，冲突会直接返回并停止。

## Phase 2：影子读对照（3-7 天）

```bash
export DATABASE_URL="$DATABASE_URL"
export DATABASE_URL_SECONDARY='sqlite:///data/rules.db'
export DB_WRITE_MODE='single'
export DB_READ_MODE='shadow_compare'
```

观察项：
- `db_compare_log` 增长趋势
- `python3 -m app.workers.cli db:status`

## Phase 3：双写灰度（3-7 天）

```bash
export DB_WRITE_MODE='dual'
export DB_READ_MODE='shadow_compare'
```

观察项：
- `dual_write_failures` 是否增长
- `python3 -m app.workers.cli db:dual-replay --limit 500` 是否可回放清空

## Phase 4：Cutover（主库切换 PG）

```bash
export DATABASE_URL='postgresql+psycopg://USER:PASS@HOST:5432/DB'
export DATABASE_URL_SECONDARY='sqlite:///data/rules.db'
export DB_WRITE_MODE='single'
export DB_READ_MODE='primary'
```

建议：SQLite 保留只读 2-4 周后再归档。

## 回滚（1 分钟内）

```bash
export DATABASE_URL='sqlite:///data/rules.db'
unset DATABASE_URL_SECONDARY
export DB_WRITE_MODE='single'
export DB_READ_MODE='primary'
```

然后重启 `admin-api` 与 `scheduler-worker`。

## 校验命令

```bash
python3 -m app.workers.cli db:verify \
  --from sqlite:///data/rules.db \
  --to "$DATABASE_URL" \
  --tables email_rules_versions,content_rules_versions,qc_rules_versions,output_rules_versions,scheduler_rules_versions,rules_drafts,sources \
  --sample 0.05
```

校验包含：
- count 对齐
- 抽样 hash 对齐（版本表至少对比 `config_json/profile/version/is_active`）
- unique 冲突预检

## 命令清单（本地）

```bash
# Schema
export DATABASE_URL='postgresql+psycopg://USER:PASS@HOST:5432/DB'
alembic upgrade head

# Migrate
python3 -m app.workers.cli db:migrate \
  --from sqlite:///data/rules.db \
  --to "$DATABASE_URL" \
  --batch-size 1000 \
  --checkpoint data/db_migrate_checkpoint.json

# Verify
python3 -m app.workers.cli db:verify \
  --from sqlite:///data/rules.db \
  --to "$DATABASE_URL" \
  --tables email_rules_versions,content_rules_versions,qc_rules_versions,output_rules_versions,scheduler_rules_versions,rules_drafts,sources \
  --sample 0.05

# Status + Replay
python3 -m app.workers.cli db:status
python3 -m app.workers.cli db:dual-replay --limit 200
```

## 命令清单（容器）

```bash
docker compose exec -T admin-api alembic upgrade head

docker compose exec -T admin-api python3 -m app.workers.cli db:migrate \
  --from sqlite:///data/rules.db \
  --to 'postgresql+psycopg://USER:PASS@db:5432/DB' \
  --batch-size 1000 \
  --checkpoint data/db_migrate_checkpoint.json

docker compose exec -T admin-api python3 -m app.workers.cli db:verify \
  --from sqlite:///data/rules.db \
  --to 'postgresql+psycopg://USER:PASS@db:5432/DB' \
  --tables email_rules_versions,content_rules_versions,qc_rules_versions,output_rules_versions,scheduler_rules_versions,rules_drafts,sources \
  --sample 0.05

docker compose exec -T admin-api python3 -m app.workers.cli db:status
docker compose exec -T admin-api python3 -m app.workers.cli db:dual-replay --limit 200
```

## 生产切换检查清单（一步一验收）

以下清单用于“执行一步 -> 验收一步 -> 再进入下一步”，避免一次性切换风险。

### Step A：迁移前基线确认

执行：

```bash
docker compose exec -T admin-api /bin/sh -lc 'echo DATABASE_URL=$DATABASE_URL; echo DATABASE_URL_SECONDARY=$DATABASE_URL_SECONDARY; echo DB_WRITE_MODE=$DB_WRITE_MODE; echo DB_READ_MODE=$DB_READ_MODE'
docker compose exec -T admin-api /bin/sh -lc 'curl -fsS http://127.0.0.1:8789/healthz'
```

通过标准：
- `healthz.ok=true`
- 当前服务可正常响应（不要求已切 PG）

失败处理：
- 先修复服务启动问题（容器、环境变量、端口）后再继续。

### Step B：Schema 升级验收（Alembic）

执行：

```bash
docker compose exec -T admin-api alembic upgrade head
docker compose exec -T db psql -U ivd -d ivd -c "\dt"
```

通过标准：
- Alembic 无报错
- 关键表存在：`*_rules_versions`、`rules_drafts`、`sources`、`run_executions`、`source_fetch_events`、`report_artifacts`、`send_attempts`、`dedupe_keys`

失败处理：
- 修复迁移失败点后重跑 `alembic upgrade head`，不要手工改线上表结构。

### Step C：数据迁移与一致性验收

执行：

```bash
python3 -m app.workers.cli db:migrate \
  --from sqlite:///data/rules.db \
  --to "$DATABASE_URL" \
  --batch-size 1000 \
  --checkpoint data/db_migrate_checkpoint.json

python3 -m app.workers.cli db:verify \
  --from sqlite:///data/rules.db \
  --to "$DATABASE_URL" \
  --tables email_rules_versions,content_rules_versions,qc_rules_versions,output_rules_versions,scheduler_rules_versions,rules_drafts,sources \
  --sample 0.05
```

通过标准：
- `db:migrate` 成功完成
- `db:verify` 无关键不一致（count/hash/unique）

失败处理：
- 根据 `checkpoint` 续跑迁移；若出现冲突先修复冲突再重跑 verify。

### Step D：灰度期验收（可选）

执行（影子读）：

```bash
export DB_WRITE_MODE='single'
export DB_READ_MODE='shadow_compare'
docker compose up -d --build
python3 -m app.workers.cli db:status
```

执行（双写）：

```bash
export DB_WRITE_MODE='dual'
export DB_READ_MODE='shadow_compare'
docker compose up -d --build
python3 -m app.workers.cli db:status
python3 -m app.workers.cli db:dual-replay --limit 200
```

通过标准：
- `dual_write_failures` 不持续增长
- `db_compare_log` 差异可解释或趋近于 0

失败处理：
- 立即退回 `single + primary`，定位差异后再灰度。

### Step E：正式切主验收（目标状态）

执行：

```bash
export DATABASE_URL='postgresql+psycopg://USER:PASS@HOST:5432/DB'
unset DATABASE_URL_SECONDARY
export DB_WRITE_MODE='single'
export DB_READ_MODE='primary'
docker compose up -d --build

docker compose exec -T admin-api /bin/sh -lc 'echo DATABASE_URL=$DATABASE_URL; echo DATABASE_URL_SECONDARY=$DATABASE_URL_SECONDARY; echo DB_WRITE_MODE=$DB_WRITE_MODE; echo DB_READ_MODE=$DB_READ_MODE'
docker compose exec -T admin-api /bin/sh -lc 'curl -fsS http://127.0.0.1:8789/healthz'
```

通过标准：
- `db_backend=postgresql`
- `DB_WRITE_MODE=single`
- `DB_READ_MODE=primary`
- 业务读写正常（/admin 页面、dry-run、scheduler）

### Step F：唯一约束验收（DB 层）

执行：

```bash
docker compose exec -T db psql -U ivd -d ivd -c "SELECT conname, conrelid::regclass::text AS table_name FROM pg_constraint WHERE conname IN ('uq_send_attempts_send_key','uq_dedupe_keys_dedupe_key','uq_email_rules_profile_version','uq_content_rules_profile_version','uq_qc_rules_profile_version','uq_output_rules_profile_version','uq_scheduler_rules_profile_version') ORDER BY conname;"
docker compose exec -T db psql -U ivd -d ivd -c "SELECT indexname,indexdef FROM pg_indexes WHERE tablename='run_executions' AND indexname='uq_run_executions_run_key';"
```

通过标准：
- `send_key` / `dedupe_key` / `profile+version` 约束存在
- `run_key` 唯一索引存在

### Step G：回滚演练（必须可执行）

执行：

```bash
export DATABASE_URL='sqlite:///data/rules.db'
unset DATABASE_URL_SECONDARY
export DB_WRITE_MODE='single'
export DB_READ_MODE='primary'
docker compose up -d --build
docker compose exec -T admin-api /bin/sh -lc 'curl -fsS http://127.0.0.1:8789/healthz'
```

通过标准：
- 1 分钟内恢复可用
- 管理台与调度可继续运行
