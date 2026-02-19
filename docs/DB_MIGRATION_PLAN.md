# DB Migration Plan (SQLite -> PostgreSQL)

本文档描述如何把规则控制面从 SQLite 灰度迁移到 PostgreSQL，并可在 1 分钟内回滚。

## 目标

- 主库切换到 PostgreSQL（`DATABASE_URL`）
- 保留 SQLite 作为本地/离线模式
- 支持双写与影子读对比：
  - `DB_WRITE_MODE=single|dual`
  - `DB_READ_MODE=primary|shadow_compare`

## 阶段步骤

1. **准备 PG 并建库**
2. **执行 Alembic**：`alembic upgrade head`
3. **首轮迁移（可断点续跑）**：`python -m app.workers.cli db:migrate ...`
4. **一致性校验**：`python -m app.workers.cli db:verify ...`
5. **灰度双写**：
   - `DATABASE_URL=<PG>`
   - `DATABASE_URL_SECONDARY=sqlite:///data/rules.db`
   - `DB_WRITE_MODE=dual`
   - `DB_READ_MODE=shadow_compare`
6. **观察无差异后切主**：
   - `DB_WRITE_MODE=single`
   - `DB_READ_MODE=primary`

## 回滚预案（< 1 分钟）

若发现问题：

1. 切回环境变量：
   - `DATABASE_URL=sqlite:///data/rules.db`
   - `unset DATABASE_URL_SECONDARY`
   - `DB_WRITE_MODE=single`
   - `DB_READ_MODE=primary`
2. 重启服务（admin/scheduler）

## 命令样例

```bash
# 1) 迁移 schema
export DATABASE_URL=postgresql+psycopg://USER:PASS@HOST:5432/DB
python -m alembic upgrade head

# 2) 数据迁移（可断点）
python -m app.workers.cli db:migrate --target-url "$DATABASE_URL" --source-sqlite data/rules.db --batch-size 500 --resume true

# 3) 校验
python -m app.workers.cli db:verify --target-url "$DATABASE_URL" --source-sqlite data/rules.db

# 4) 双库读写回放对比
python -m app.workers.cli db:dual-replay --primary-url "$DATABASE_URL" --secondary-url sqlite:///data/rules.db
```

## 说明

- `db:migrate` 使用目标库 `_db_migrate_checkpoint` 记录进度，支持续跑。
- 本阶段只迁移控制面表：规则版本、drafts、sources。
- 业务规则选择与 `/admin` SOP 不变。

## 一键脚本

- `scripts/db_preflight.sh`：连通性 + Alembic 预检
- `scripts/db_cutover.sh`：迁移、校验、双库对比、输出灰度/回滚环境变量

示例：

```bash
DATABASE_URL='postgresql+psycopg://USER:PASS@HOST:5432/DB' \
DATABASE_URL_SECONDARY='sqlite:///data/rules.db' \
./scripts/db_cutover.sh go

./scripts/db_cutover.sh enable-dual
./scripts/db_cutover.sh finalize
./scripts/db_cutover.sh rollback
```

`DB_DUAL_STRICT` 默认 `false`：dual 模式下 secondary 写失败仅告警，不阻断 primary。
设为 `true` 可改为严格模式（secondary 失败即报错）。
