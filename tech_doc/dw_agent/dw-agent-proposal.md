# 数仓 AI Agent 技术方案

> 以 Hive 元数据为核心知识源，构建覆盖自动化治理报告、智能问数 SQL 生成、对话式知识问答的全栈 AI Agent 平台。

---

## 目录

1. [方案概述](#一方案概述)
2. [系统架构](#二系统架构)
3. [核心模块详解](#三核心模块详解)
   - 3.1 [元数据采集与知识库构建](#31-元数据采集与知识库构建)
   - 3.2 [自动化数据治理报告](#32-自动化数据治理报告)
   - 3.3 [智能问数 Text-to-SQL](#33-智能问数-text-to-sql)
   - 3.4 [对话式知识问答](#34-对话式知识问答)
4. [技术选型](#四技术选型)
5. [实施路线图](#五实施路线图)
6. [风险评估](#六风险评估)
7. [资源与成本估算](#七资源与成本估算)

---

## 一、方案概述

### 背景与目标

数仓团队积累了大量表结构、血缘依赖、调度配置等元数据，但这些知识分散在 Hive Metastore、Airflow、文档系统中，难以统一利用。本方案旨在：

- 将数仓元数据系统性地向量化，形成可被 AI 检索的专属知识库
- 在知识库之上部署四类 AI Agent 工作流，将重复性人工作业转化为自动化能力
- 提供面向不同角色（工程师、分析师、业务人员）的智能交互入口

### 核心能力一览

| 能力模块 | 面向角色 | 核心价值 |
|----------|----------|----------|
| 元数据知识库 | 全员基础设施 | 所有 Agent 能力的底座，数仓知识统一入口 |
| 自动化治理报告 | 数据工程师 | 替代人工整理数据，报告自动生成并推送 |
| 智能问数 Text-to-SQL | 数据分析师 | 自然语言生成 HiveSQL，降低查数门槛 |
| 对话式知识问答 | 业务人员 | 用中文提问，即时获取表/指标说明 |
| 数据质量监控 | 数据工程师 | 自动检测异常、归因，推送告警摘要 |
| 血缘影响分析 | 数据工程师 | 上游变更时自动生成下游影响范围报告 |

---

## 二、系统架构

采用六层分层架构，各层职责清晰，支持独立扩展与替换。

```
┌────────────────────────────────────────────────────────────┐
│  👤 用户层                                                   │
│  数据工程师（治理报告/告警）  分析师（问数）  业务（问答）  外部系统（API）  │
└──────────────────────────┬─────────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────────┐
│  📱 应用层                                                   │
│  自动化治理报告        智能问数 Text-to-SQL     对话式知识问答   │
│  Airflow DAG→Report    Chat UI / API           企微机器人     │
└──────────────────────────┬─────────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────────┐
│  🤖 Agent 编排层                                             │
│  LangChain Agent    Prompt Templates    RAG Pipeline         │
│  工具调用/规划       域适配提示词          检索→增强→生成        │
│  Memory Manager（多轮对话记忆）                               │
└──────────────────────────┬─────────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────────┐
│  🧠 知识层                                                   │
│  向量知识库              结构化元数据库         大语言模型        │
│  Milvus · 元数据Embedding  PostgreSQL·表/字段/血缘  GPT-4o/Qwen  │
└──────────────────────────┬─────────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────────┐
│  ⚙️ 采集与处理层                                             │
│  元数据 ETL           文本向量化        调度日志 Parser          │
│  PyHive / JDBC        Embedding 服务    Airflow / 自定义        │
│  增量同步（Kafka / Cron）                                     │
└──────────────────────────┬─────────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────────┐
│  🗄️ 数据源层                                                 │
│  Hive Metastore   HDFS        Airflow      业务文档    数据质量  │
│  表结构/分区       存储统计     调度元信息    指标定义/Wiki  GE     │
└────────────────────────────────────────────────────────────┘
```

---

## 三、核心模块详解

### 3.1 元数据采集与知识库构建

> **核心思路**：从 Hive Metastore 提取结构化元数据，转化为自然语言描述文档后向量化存储，形成数仓专属知识图谱。这是所有 Agent 能力的基础底座。

#### Step 1：连接 Hive Metastore，全量抽取元数据

通过 PyHive / Thrift API 连接 HMS，按库/表/字段逐层抽取：

```python
from pyhive import hive

conn = hive.connect(host='hive-metastore', port=10000)
cursor = conn.cursor()

# 获取所有表的字段 + 注释
cursor.execute("""
  SELECT t.TBL_NAME, c.COLUMN_NAME, c.TYPE_NAME, c.COMMENT
  FROM TBLS t JOIN COLUMNS_V2 c ON t.CD_ID = c.CD_ID
  WHERE t.DB_ID = (SELECT DB_ID FROM DBS WHERE NAME = %s)
""", (db_name,))
rows = cursor.fetchall()
```

**抽取的核心元数据字段：**

| 类别 | 字段 |
|------|------|
| 表基础信息 | db_name、table_name、owner、create_time、table_type、storage_format |
| 字段信息 | column_name、data_type、comment、nullable、is_partition |
| 统计信息 | row_count、file_size、last_ddl_time、num_files、num_partitions |
| 血缘关系 | upstream_tables、downstream_tables、lineage_depth |
| 调度信息 | job_name、cron_expr、avg_duration、sla_threshold |
| 质量信息 | null_rate、max_val、min_val、last_check_time |

#### Step 2：元数据文档化（结构化 → 自然语言）

将原始元数据转换为标准化"表描述文档"：

```python
TEMPLATE = """
# 表：{db}.{table}

**业务描述**：{desc}
**所属分层**：{layer} | **负责人**：{owner}
**分区字段**：{partitions} | **更新频率**：{cron}

## 字段说明
{fields_md}

## 上游依赖
{upstream}

## 典型查询示例
{sql_examples}
"""
```

对于注释缺失的表，调用 LLM 结合表名 + 字段名 + 示例数据自动补全注释。

#### Step 3：向量化存储（三层知识库）

| 层级 | 内容 | 用途 | Top-K |
|------|------|------|-------|
| L1 全局层 | 库级别摘要 | "哪个库有 XX 数据" | 3 |
| L2 表级层 | 表描述 + 字段摘要 | SQL 生成时选表 | 5~10 |
| L3 字段层 | 字段含义、枚举值 | 列名精确补全 | 15 |

**检索策略**：语义向量搜索 + BM25 关键词检索 → RRF 融合排序 → Cross-Encoder 重排序 → Redis 热点缓存（TTL 1小时）

#### Step 4：增量同步

监听 HMS DDL 事件，通过定时对比捕获变更（新增/修改/删除），触发对应向量块的更新，延迟 ≤ 15 分钟。

---

### 3.2 自动化数据治理报告

> **核心思路**：Agent 作为"自动化报告员"嵌入 Airflow 流水线，调度周期结束后自动拉取指标、对比历史、生成报告并推送，无需人工介入。

#### 触发方式

- **事件触发**：Airflow DAG `on_success_callback` 在调度周期完成后自动触发
- **定时触发**：每日 10:00 生成前一天汇总报告

#### Agent 工具调用设计

```python
from langchain.tools import tool

@tool
def get_sla_stats(date: str, db_filter: list[str]) -> dict:
    """获取指定日期各库SLA达成率、平均完成时间、失败作业列表"""
    return query_airflow_logs(date, db_filter)

@tool
def get_quality_metrics(tables: list[str]) -> dict:
    """获取表数据质量指标：空值率、行数变化、异常分布"""
    return query_quality_db(tables)

@tool
def get_resource_stats(date: str) -> dict:
    """获取资源消耗 Top10、排队峰值、Gap 冗余分析"""
    return query_resource_logs(date)

# Agent 自主决定调用哪些工具、以什么顺序
agent = create_react_agent(llm, tools=[get_sla_stats, get_quality_metrics, get_resource_stats])
```

#### 报告覆盖维度

- **SLA 完成情况**：各库各层完成率、超时作业、P90 耗时趋势
- **资源消耗**：内存/核时 Top 10，与上期对比变化量
- **调度 Gap**：冗余等待作业识别，调整建议
- **数据质量**：空值异常、行数波动、分区断档检测
- **血缘风险**：高下游依赖作业延迟风险预警
- **优化 ROI**：与历史基准期对比改进量化

#### 推送渠道

- 企业微信 / 钉钉 Webhook（摘要卡片）
- 邮件（完整 HTML 报告）
- 对象存储（OSS/S3，历史报告查阅）

#### 质量保障

- 所有数字来源于工具调用，LLM 禁止凭空生成数字（防幻觉）
- 报告数字与原始数据做一致性校验，不通过则降级为人工
- 报告含"异议反馈"链接，修正结果用于后续优化

---

### 3.3 智能问数 Text-to-SQL

> **核心思路**：通过 RAG 检索相关表结构注入 Prompt，结合 Schema Linking 将问句实体映射到实际字段，生成符合 HiveSQL 规范的查询语句，并在沙箱执行验证。

#### 完整处理流程

```
用户输入自然语言
       ↓
[NLU] 意图识别 + 实体抽取 + 时间归一化
       ↓
[Schema Linking] 向量检索候选表 → 字段精确匹配 → 构建精简 Schema 上下文
       ↓
[SQL 生成] 注入 HiveSQL 规范 + Schema 上下文 → LLM 生成 SQL
       ↓
[语法校验] EXPLAIN 检查 + Schema 合法性验证
       ↓
[沙箱执行] 只读账号 + LIMIT 上限 + 超时 60s
       ↓
执行成功 → LLM 自然语言解读结果
执行失败 → 错误反馈给 Agent 自我修正（最多 3 次）
```

#### Schema Linking 示例

```python
user_query = "查询上周 dws_pa 库中每个渠道的新增用户数"

# 1. 向量检索候选表（top_k=8）
candidates = vector_store.search(user_query, collection="table_level", top_k=8)

# 2. 字段级精确匹配，精简 Schema
schema_ctx = build_schema_context(candidates, user_query)
# → 仅保留: dws_pa.user_daily_new(channel, new_user_cnt, dt)

# 3. 注入 Prompt 生成 SQL（带 HiveSQL 规范约束）
sql = llm.invoke(SQL_PROMPT.format(schema=schema_ctx, query=user_query))
```

#### 准确率提升策略

| 策略 | 说明 |
|------|------|
| Few-shot 示例 | Prompt 中注入 3~5 个领域内高质量 SQL 示例 |
| Self-consistency | 生成 3 个候选 SQL，投票选最稳定的 |
| Schema 精减 | 只保留相关字段，减少 LLM 干扰 |
| SQL 微调 | 收集人工纠错样本进行 LoRA 微调（迭代优化） |
| Fallback 机制 | 置信度低时，展示 SQL 草稿请用户确认后再执行 |
| 业务词典 | 维护"GMV→order_amount、DAU→active_user_cnt"映射表 |

#### 安全控制

- 使用专用 Hive 只读账号，配置独立 YARN 队列
- SQL 白名单：只允许 SELECT，拦截 DDL/DML/LOAD
- 强制 `LIMIT 1000` + 执行超时 60s
- PII 字段（手机、邮箱）自动脱敏后返回
- 所有 SQL 执行记录入审计日志，可追溯到用户

---

### 3.4 对话式知识问答

> **核心思路**：面向业务人员的低门槛知识问答入口，用户用中文提问，Agent 从知识库检索并组织自然语言答案，所有答案注明来源可溯。

#### 处理链路

```
用户提问
    ↓
[问题分类路由]
    ├── 元数据查询型  → 检索向量知识库
    ├── 数据值查询型  → 触发 SQL 查询链路
    └── 流程说明型   → 检索文档 + 血缘知识库
    ↓
[多源混合检索]
向量知识库 + 结构化数据库（精确查表）+ 历史问答对缓存
    ↓
[答案生成]
标注引用来源（哪张表的注释、哪条血缘记录）
    ↓
[反馈收集]
👍 有帮助 / 👎 有误 → 负反馈触发人工审核 → 合格样本加入训练集
```

#### 典型问答场景

| 用户提问 | Agent 返回 |
|----------|------------|
| "dws_pa.order_daily 是什么表？" | 业务描述、字段列表、更新频率、负责人 |
| "channel_type 字段有哪些取值？" | 枚举值说明及统计分布（Top 5） |
| "dws_pa 和 ods_pa 有什么区别？" | 分层定位对比、字段粒度差异说明 |
| "order_amount 昨天比前天少了30%，为什么？" | 检查上游数据新鲜度、分区完整性、质量异常 |
| "这张表被哪些下游作业依赖？" | 遍历血缘图，返回直接/间接下游列表 |

#### 接入渠道

- **企业微信机器人**：@机器人 直接提问，适合日常快速查询
- **独立 Chat 页面**：集成到内部数据门户，支持多轮对话历史
- **Jupyter 插件**：在 Notebook 中 `%ask "问题"`，返回表结构和 SQL
- **REST API**：外部系统通过 API 集成，支持 SSE 流式响应

---

## 四、技术选型

### 推荐技术栈

| 类别 | 推荐方案 | 备选方案 | 选型理由 |
|------|----------|----------|----------|
| **大语言模型** | Qwen2.5-72B-Instruct | GPT-4o / DeepSeek-V3 | 中文最优，支持私有化，SQL 任务表现突出 |
| **Embedding 模型** | BGE-M3 (BAAI) | text-embedding-3-small | 多语言多粒度，支持 dense+sparse 混合检索 |
| **向量数据库** | Milvus 2.x | Chroma / PGVector | 亿级向量支持，混合检索能力强 |
| **Agent 框架** | LangChain + LangGraph | LlamaIndex | 工具调用生态完整，支持多 Agent 状态机 |
| **后端 API** | FastAPI + Python 3.11 | — | 异步高性能，SSE 流式响应，与 LangChain 无缝集成 |
| **前端界面** | Next.js 14 + shadcn/ui | Streamlit（内部工具） | SSR 友好，流式输出支持好 |
| **工作流编排** | Apache Airflow 2.x | Prefect / Dagster | 沿用现有调度系统，零迁移成本 |
| **缓存** | Redis | — | 热点问答缓存、异步任务队列 |
| **元数据抽取** | PyHive + SQLAlchemy | JDBC | 纯 Python 生态，与 Pandas 集成方便 |
| **监控追踪** | LangSmith | Prometheus + Grafana | Agent 调用链路可视化，便于调试优化 |
| **血缘图计算** | NetworkX | Apache Atlas | 轻量级，满足中等规模血缘分析 |

### LLM 选型说明

```
私有化部署（推荐）：
  Qwen2.5-72B-Instruct
  ├── 部署方式：vLLM on 4×A100 GPU
  ├── 中文 SQL 生成能力：★★★★★
  ├── 数据安全：完全内网，无数据出境风险
  └── 成本：一次性 GPU 投入，无 token 费用

云 API（快速起步）：
  DeepSeek-V3  →  SQL/代码能力极强，价格最低（￥1/M tokens）
  GPT-4o       →  综合能力最强，但需数据出境审批
```

---

## 五、实施路线图

### 总体节奏：4 阶段 × 4 周 = 16 周

#### 第一阶段（第 1~4 周）：基础设施 & 知识库

**目标**：完成知识库底座，验证检索召回准确率 ≥ 80%

- [ ] 搭建 Milvus + PostgreSQL 基础设施（容器化）
- [ ] 完成 Hive 元数据全量采集脚本
- [ ] 实现表/字段描述文档自动生成（含 LLM 注释补全）
- [ ] 完成三层向量知识库的写入与索引
- [ ] 搭建 FastAPI 基础服务框架 + 认证机制
- [ ] 验证检索召回准确率，输出 Benchmark 报告

**交付物**：可查询的元数据知识库 + 检索 API

---

#### 第二阶段（第 5~8 周）：对话问答 & Text-to-SQL MVP

**目标**：上线基础问答能力，单表 SQL 准确率 ≥ 70%

- [ ] 实现基础 Q&A 问答链（RAG Pipeline）
- [ ] 上线企业微信机器人问答入口
- [ ] Text-to-SQL 支持单表查询（含分区过滤）
- [ ] SQL 沙箱执行验证 + 自我修正机制
- [ ] 开始系统性收集用户反馈样本
- [ ] 建立准确率监控看板

**交付物**：企微问答机器人 + 单表 SQL 生成能力

---

#### 第三阶段（第 9~12 周）：自动化报告 & 多表 SQL

**目标**：自动化报告上线，多表 JOIN SQL 准确率 ≥ 60%

- [ ] 集成 Airflow，实现三类报告自动生成（SLA / 质量 / Gap）
- [ ] 报告推送企微 + 邮件，历史归档到 OSS
- [ ] Text-to-SQL 支持多表 JOIN 查询
- [ ] 增量元数据同步机制上线（延迟 ≤ 15min）
- [ ] 数据质量监控 Agent 上线（空值/行数异常告警）

**交付物**：自动化日报系统 + 多表 SQL 生成

---

#### 第四阶段（第 13~16 周）：智能化增强 & 生产稳定化

**目标**：全面上线，生产级稳定运行

- [ ] 基于积累的反馈样本进行 LoRA 微调，提升 SQL 准确率
- [ ] 血缘影响分析 Agent 上线
- [ ] 独立 Chat Web 页面上线（集成到数据门户）
- [ ] 完善监控告警、降级机制（LLM 超时自动降级）
- [ ] 全面性能压测（并发 20 QPS）
- [ ] 文档与运维手册沉淀

**交付物**：完整平台上线 + 运维文档

---

## 六、风险评估

| 风险点 | 等级 | 风险描述 | 应对策略 |
|--------|------|----------|----------|
| **元数据注释质量差** | 🔴 高 | 大量表/字段无中文注释，影响 RAG 检索质量和 LLM 理解准确率 | ① LLM 自动补全注释；② 注释质量评分，低分表人工优先补录；③ 随注释改善迭代提升 |
| **LLM Hallucination** | 🔴 高 | SQL 生成或问答中 LLM 捏造不存在的表名、字段名 | ① Schema Linking 约束 LLM 不得创造新字段；② 所有数字来源工具调用；③ SQL 执行前合法性校验；④ 置信度低时展示原始上下文 |
| **API 成本失控** | 🟡 中 | 高频问答导致 LLM 调用量超预期 | ① 热点答案 Redis 缓存（TTL 1h）；② Prompt 精简控制 token；③ 用户每日配额限制；④ 简单问题路由至小模型降本 |
| **数据安全合规** | 🟡 中 | 元数据通过 LLM API 出境；SQL 返回敏感数据 | ① 优先私有化部署 LLM；② 云 API 场景下表名/字段名脱敏替换；③ SQL 结果 PII 字段自动遮盖；④ 通过数据安全团队合规审查 |
| **Hive 资源占用** | 🟡 中 | Text-to-SQL 大量查询影响生产集群稳定性 | ① 专用只读账号 + 独立 YARN 队列；② 强制 LIMIT 上限 + 超时 60s；③ 相同 SQL 结果 24h 缓存；④ 非工作时间预计算热点查询 |
| **团队 AI 接受度** | 🟢 低 | 用户对 AI 生成内容不信任或使用率低 | ① 答案来源透明可溯；② 设定合理预期（辅助工具而非替代品）；③ 从工程师内测开始推广；④ 公示准确率改进看板 |

---

## 七、资源与成本估算

### 团队配置

| 角色 | 人数 | 主要职责 |
|------|------|----------|
| 后端工程师 | 2 人 | Agent 框架开发、FastAPI 服务、RAG Pipeline |
| 数据工程师 | 1 人 | 元数据采集、Airflow 集成、SQL 规范适配 |
| 前端工程师 | 1 人 | Chat 界面、报告页面、数据门户集成 |
| **合计** | **4 人** | **全职投入约 4 个月** |

### 基础设施需求

| 资源 | 规格 | 用途 |
|------|------|------|
| GPU 服务器 | 4×A100 (80G) | Qwen2.5-72B 模型推理，支持 ~20 QPS |
| 应用服务器 | 16 核 / 64G RAM | FastAPI + Milvus + PostgreSQL + Redis |
| 存储 | 500G SSD | 向量索引 + 元数据库 + 报告归档 |

> **注**：若选用云 API（DeepSeek/GPT-4o），可跳过 GPU 服务器投入，以月度 API 费用替代。按 1000 次/天问答量，DeepSeek-V3 月费用约 ¥500~2000（视平均 token 量）。

### 预期收益量化

| 指标 | 当前 | 目标（6个月后） |
|------|------|----------------|
| 治理报告生成时间 | ~4 小时/周（人工） | ~10 分钟/次（自动） |
| 分析师查数等待时间 | ~2 小时（等工程师写 SQL） | ~5 分钟（自助问数） |
| 数据问题平均定位时间 | ~1 小时 | ~10 分钟（AI 辅助归因） |
| 元数据文档覆盖率 | ~30%（有注释的表） | ~90%（AI 补全） |

---

## 附录：参考资料

- [LangChain 官方文档](https://python.langchain.com/)
- [LlamaIndex 官方文档](https://docs.llamaindex.ai/)
- [Milvus 向量数据库](https://milvus.io/)
- [BGE-M3 Embedding 模型](https://huggingface.co/BAAI/bge-m3)
- [Qwen2.5 模型](https://huggingface.co/Qwen)
- [vLLM 推理框架](https://docs.vllm.ai/)
- [Text-to-SQL Survey (2024)](https://arxiv.org/abs/2408.05109)

---

*数仓 AI Agent 技术方案 · 版本 v1.0 · 2026-03-30*
