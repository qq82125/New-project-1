# Web UI (Next.js Dual Feed)

本项目新增独立前端 `web/`（Next.js 14 + TypeScript + Tailwind），仅调用现有后端 API，不迁移采集/调度/规则逻辑。

## 页面

- `/feed`：Story Feed（事件流，业务主视图）
- `/feed-items`：Item Feed（原始条目流，证据复核/运维）
- `/feed/[id]`：Story 详情页
- `/dashboard`：占位页

## 后端接口

由现有 `admin-api` 提供：

- `GET /api/feed`（stories）
- `GET /api/feed/{id}`（story + evidence）
- `GET /api/feed-items`（raw_items）
- `GET /api/feed-items/{id}`（raw item + mapped story）

查询参数（`/api/feed` 与 `/api/feed-items`）：

- `cursor`, `limit`
- `group`, `region`, `trust_tier`, `source_id`
- `q`（搜索）
- `start`, `end`
- `since`（轮询增量）

## 数据准备（首次）

先把 collect 资产写入 Postgres，再构建 stories：

```bash
python3 -m app.workers.cli raw-ingest --scan-artifacts-days 7
python3 -m app.workers.cli story-build --window-days 30
python3 -m app.workers.cli backfill-meta --execute --batch-size 2000
python3 -m app.workers.cli backfill-stories-meta --execute --batch-size 2000
```

说明：

- `backfill-meta` 会基于 `sources_registry` 补齐 raw_items 的 `group/region/trust_tier/priority`，并生成 `event_type`。
- `backfill-stories-meta` 会把 stories 的元信息从 primary raw item 继承到 story 层。
- `/feed` 与 `/feed-items` 的 Region/Event type 筛选下拉来自当前已加载数据的 distinct+count（前端动态统计）。

## 本地启动

1) 启动后端（已有方式）  
`python3 -m app.admin_server`

2) 启动前端

```bash
cd web
cp .env.example .env.local
npm install
npm run dev
```

访问：

- [http://localhost:3000/feed](http://localhost:3000/feed)
- [http://localhost:3000/feed-items](http://localhost:3000/feed-items)

## Docker Compose

已加入 `web` 服务：

```bash
docker compose up -d --build
```

访问：

- Web: [http://localhost:3000/feed](http://localhost:3000/feed)
- Web: [http://localhost:3000/feed-items](http://localhost:3000/feed-items)
- API: [http://localhost:8090/api/feed](http://localhost:8090/api/feed)

## 注意

- 双 feed 都默认展示 all-time（不传 start/end）。
- 分页使用 cursor，不使用 offset。
- 轮询默认 60 秒；若后端不支持增量则提示手动刷新。
- 当前无鉴权，仅用于本地开发环境。
