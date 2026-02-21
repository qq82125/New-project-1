# Track Contract (core / frontier)

本文定义 New-project-1 在 `enhanced` 侧的双维相关性分流契约。  
目标：在不破坏 `legacy` 行为前提下，把内容从“是否 IVD”升级为“`track` + `relevance_level`”。

## 1. 字段定义

- `track`: `core | frontier`
- `relevance_level`: `0..4`（整数）
- `relevance_why`: 可解释原因（关键词/分值/事件上下文）

运行时断言：

- `track` 必须属于 `{core, frontier}`
- `relevance_level` 必须在 `[0,4]`
- 若异常，运行时会归一化修正并把修正记录写入 `artifacts/*/track_contract.json`

## 2. A / F / G 分流规则

- A 段（今日要点）：优先承载 `core` 且 `relevance_level >= 3` 的高相关条目
- F 段（信息缺口）：承载 `frontier` 且 `relevance_level >= 2` 的雷达条目，受 `frontier_quota.max_items_per_day` 控制
- G 段（质量审计，必须末尾）：新增 core/frontier 覆盖统计与分流缺口解释

默认路由（当规则未配置时）：

```yaml
track_routing:
  A:
    track: core
    min_relevance_level: 3
  F:
    track: frontier
    min_relevance_level: 2
  G:
    include_track_coverage: true
```

推荐在 `content_rules` 中同步配置阈值与配额：

```yaml
defaults:
  relevance_thresholds:
    core_min_level_for_A: 3
    frontier_min_level_for_F: 2
  frontier_quota:
    max_items_per_day: 3
  anchors_pack:
    core: [ivd, diagnostic, assay, pcr, ngs, poct, lab automation]
    frontier: [single-cell, multi-omics, proteomics, microfluidic, simoa]
  negatives_pack: [earnings, layoff, phase 3 drug, therapy only]
```

## 3. 配额与覆盖目标（本期最小契约）

- 不强制改变 legacy 选稿结果
- enhanced 在 G 段输出：
  - `core/frontier覆盖：<core>/<frontier>`
  - `A候选`、`F候选`数量
  - `F配额占用：used/target`
  - 分流规则缺口（若路由为空或无命中）

强制要求（enhanced）：
- 当 A 段 `core` 高相关条目不足时，必须在 G 段输出缺口解释（允许继续出报，不中断）
- 当 F 段 `frontier` 配额未占满时，必须输出“前沿信号不足”原因统计（topN）

## 4. 缺口处理

当 A/F 路由规则缺失或无命中时：

- 不中断生成流程
- F 段第一条插入“分流规则缺口”解释
- G 段追加“分流规则缺口说明”
- 不允许 A/F/G 三段同时为空；至少保留缺口解释文本

## 5. 手工验收（本地）

1. 启动服务：`docker compose up -d --build`
2. 控制台试跑（enhanced）
3. 检查输出：
   - A/F/G 仍可生成
   - G 段出现 core/frontier 覆盖统计
4. 检查资产：
   - `artifacts/<run_id>/track_contract.json`
