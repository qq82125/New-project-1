# Relevance Contract (0~4)

本文定义 `relevance_level` 的判别锚点、负面词处理与 explain 要求。  
适用范围：`enhanced` 运行链路（legacy 保持兼容）。

## 1. 分级定义

- `4`: 强相关且高确定性（核心诊断锚点密集，监管/临床/产品证据充分）
- `3`: 高相关（明确 IVD/检测技术语义，具备业务价值）
- `2`: 中相关（有检测语义但信息密度一般，需二次核验）
- `1`: 弱相关（边缘命中，保留作补充观察）
- `0`: 低相关或噪音（可用于解释/审计，不应优先进入 A）

## 2. 必要判别锚点（示例）

- 诊断/检测锚点：`diagnostic`, `assay`, `test`, `IVD`, `PCR`, `NGS`, `POCT` …
- 平台锚点：`flow cytometry`, `mass spec`, `immunoassay`, `single molecule` …
- 监管锚点：`FDA`, `NMPA`, `PMDA`, `TGA`, `HSA`, `MFDS`, `guidance`, `recall` …

## 3. 负面词处理

负面词不会直接丢弃条目，但会拉低相关分，典型如：

- 财报类：`earnings`, `revenue`, `sales`
- 纯药物研发类：`phase`, `drug`, `therapy`, `vaccine`

若存在强诊断锚点，仍可保留并进入 explain。

## 4. explain 要求

每条 item 至少应可追溯：

- `track`
- `relevance_level`
- `relevance_why`
- `relevance_explain`（结构化）：
  - `anchors_hit: []`
  - `negatives_hit: []`
  - `rules_applied: []`
  - `final_reason: string`

审计输出要求：

- G 段包含 core/frontier 覆盖统计
- 资产中保留 `track_contract.json`（含样例与修正记录）

## 5. 输出约束

- A~F 禁止出现质量审计字段（质量指标集中在 G）
- G 必须置尾
- 当分流规则为空或无命中，必须有“缺口解释”而不是静默失败
