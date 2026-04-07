# 伪中间层迁移重构方案

**作业名称**：`etl-hive-dws_ads_pa-ads_anu_operations_pobai_necklace_di`  
**迁移方向**：`dws_ads_pa` → `dws_pa`（数仓公共层）  
**文档状态**：草案 · 待评审  
**最后更新**：2026-03-30  

---

## 一、问题诊断

### 1.1 伪中间层认定依据

| 判定维度 | 现状 | 问题 |
|----------|------|------|
| **归属库** | `dws_ads_pa`（分析师 ads 层） | 应属数仓公共层，不应由分析师维护 |
| **下游依赖数** | **13 个**下游作业直接依赖 | 已成事实上的"中间层"，影响范围远超 ads 层定义 |
| **负责人** | `u101356`（单人维护） | 无数仓团队兜底，运维风险高 |
| **上游来源** | 9 个来自 `dws_pa` 公共层，5 个来自 `dws_ads_pa` ads 层 | 数据来源跨层，逻辑天然属于中间加工层 |
| **业务定位** | 破白用户全量宽表（30 日滚动窗口） | 典型的公共明细宽表，非一次性分析 |

**结论：该表事实上承担了数仓公共宽表职责，需从 `dws_ads_pa` ads 层迁移至 `dws_pa` 数仓公共层统一管控。**

---

## 二、现状分析

### 2.1 作业基本信息

| 属性 | 值 |
|------|-----|
| 作业 ID | 449695 |
| 作业名称 | etl-hive-dws_ads_pa-ads_anu_operations_pobai_necklace_di |
| 中文描述 | 项链破白 |
| 输出表 | `dws_ads_pa.ads_anu_operations_pobai_necklace_di` |
| 调度时间 | `0 0 6 ? * *`（每日 06:00 触发） |
| 负责人 | u101356 |
| 通知人 | u003102 / ecs-liu.kaibao / u400619 |
| 创建时间 | 2024-03-18 |
| 重试次数 | 25 次（异常偏高，说明历史稳定性较差） |

### 2.2 运行性能指标（基于 2026-03-27~29 三日数据）

| 指标 | 值 | 说明 |
|------|-----|------|
| 调度次数 | 3 次（全部成功） | 近期稳定运行 |
| 平均触发→完成时长 | **约 71~72 分钟** | 06:00 触发，约 07:10~08:14 完成 |
| 单次 MR Application 数 | **13 个**（最多一次 15 个） | 多段 SQL 导致 MR 任务数多 |
| 3 日累计内存消耗 | **137.2 GB·h** | 资源占用较重 |
| 3 日累计核时 | **19.4 vcore·h** | — |
| 使用队列 | `olap` | — |

### 2.3 上游血缘（14 个依赖）

```
etl-hive-dws_ads_pa-ads_anu_operations_pobai_necklace_di
├── 来自 dws_pa（公共层，9个）
│   ├── etl-hive-dws_pa-ads_mkt_dim_room_da              （房间维度）
│   ├── etl-hive-dws_pa-ads_pt_anu_next_days_user_di     （破白后行为）
│   ├── etl-hive-dws_pa-ads_pt_log_pobai_xinren_pchat_user_to_user_di （新人榜私聊）
│   ├── etl-hive-dws_pa-ads_pt_trans_next_days_user_di   （破白后充值消费）
│   ├── etl-hive-dws_pa-dim_user_channel_df              （用户渠道维度）
│   ├── etl-hive-dws_pa-dim_user_df                      （用户基础维度）
│   ├── etl-hive-dws_pa-dwd_log_onmic_room_di            （上麦日志）
│   ├── etl-hive-dws_pa-dwd_trans_consume_journal_di     （消费流水）
│   ├── etl-hive-dws_pa-tms_trans_charge_user_da         （首充用户）
│   └── etl-hive-dws_pa-ads_vip_level_next_days_user_df  （VIP等级轨迹）
│
└── 来自 dws_ads_pa（ads层，5个）
    ├── etl-hive-dws_ads_pa-ads_anu_operations_charge_cheat_di  （作弊用户）
    ├── etl-hive-dws_ads_pa-ads_op_user_to_user_consume_days_df （GS消费金额）
    ├── etl-hive-dws_ads_pa-ads_op_user_label_l_df              （L标签）
    ├── etl-hive-dws_ads_pa-ads_anu_pobai_necklace_txk_di       （破白贴靠）
    └── dws_ads_ch_o.ads_channel_pay_valid_100_va_user_df        （价值用户，外部库）
```

> **关键发现**：上游 9/14 来自数仓公共层（dws_pa），说明该表的数据底座已是公共层，**迁移 ETL 逻辑阻力较小**。

### 2.4 下游血缘（13 个依赖，全在 dws_ads_pa，全为同一负责人）

| # | 下游作业名 | 调度时间 | 中文描述 |
|---|-----------|----------|----------|
| 1 | `ads_service_user_anchor_di` | 06:30 | PA-承接主播相关天增量指标 |
| 2 | `ads_service_user_room_di` | 06:30 | PA-承接厅相关天增量指标 |
| 3 | `tp-hive-ck-dws_ads_pa-ads_anu_operations_pobai_necklace_di` | 06:30 | CK 同步（ClickHouse 实时层） |
| 4 | `dts_anu_pt_pobai_private_di` | 04:30 | PA-gsuid私聊及破白日数据 |
| 5 | `ads_op_o_auto_accost_from_user_di` | 07:40 | 用户视角GS自动搭讪数据监控 |
| 6 | `ads_active_gcc_user_room_info_di` | 07:10 | PA-大区房间uid用户活跃及消费数据 |
| 7 | `ads_anu_itr_active_user_info_di` | 07:00 | PA-活跃uid行为日数据ab |
| 8 | `ads_op_o_auto_accost_from_user_mid_di` | 06:10 | 自动搭讪中间表 |
| 9 | `ads_pt_new_uid_grow_up_di` | 06:10 | PA-大区-用户uid注册X日消费和充值成长 |
| 10 | `ads_op_o_regs_cj_ff_jt_di` | 06:10 | PT-大区-新注册uid-付费标签-日-中间表 |
| 11 | `ads_mkt_user_room_label_pobai_di` | 08:10 | PA-房间uid+room_id集市宽表 |
| 12 | `ads_op_o_onmic_gs_pobai_di` | 08:10 | 破白成长 |
| 13 | `dts_anu_pa_ff_private_di` | 06:30 | PA-uid-活跃用户每日私聊付费明细 |

> ⚠️ **注意**：下游 #3 为 ClickHouse 同步任务，迁移后 CK 同步任务的源表路径**必须同步变更**，否则 ClickHouse 数据将断流。

---

## 三、下游任务使用模式分析

> 通过逐一阅读 13 个下游作业的 SQL，提取其对 `ads_anu_operations_pobai_necklace_di` 的引用方式，识别公共逻辑，为迁移后的兼容设计提供依据。

### 3.1 直接引用 vs 调度依赖

首先区分"直接读取该表数据"和"仅因调度时序依赖"：

| 作业名 | 是否直接引用该表 | 引用方式 |
|--------|----------------|----------|
| `ads_service_user_anchor_di` | ✅ 直接引用 | `FROM dws_ads_pa.ads_anu_operations_pobai_necklace_di` |
| `ads_service_user_room_di` | ✅ 直接引用 | `FROM dws_ads_pa.ads_anu_operations_pobai_necklace_di` |
| `tp-hive-ck-...(CK同步)` | ✅ 直接引用 | 全量同步至 ClickHouse |
| `dts_anu_pt_pobai_private_di` | ⚠️ 仅调度依赖 | SQL 中未见直接引用，依赖关系可能为时序保障 |
| `ads_op_o_auto_accost_from_user_di` | ✅ 直接引用 | `FROM dws_ads_pa.ads_anu_operations_pobai_necklace_di` |
| `ads_active_gcc_user_room_info_di` | ✅ 直接引用 | `FROM dws_ads_pa.ads_anu_operations_pobai_necklace_di` |
| `ads_anu_itr_active_user_info_di` | ⚠️ 仅调度依赖 | SQL 未见直接引用，可能间接依赖 |
| `ads_op_o_auto_accost_from_user_mid_di` | ✅ 直接引用 | `FROM dws_ads_pa.ads_anu_operations_pobai_necklace_di` |
| `ads_pt_new_uid_grow_up_di` | ✅ 直接引用 | `FROM dws_ads_pa.ads_anu_operations_pobai_necklace_di` |
| `ads_op_o_regs_cj_ff_jt_di` | ✅ 直接引用 | `FROM dws_ads_pa.ads_anu_operations_pobai_necklace_di` |
| `ads_mkt_user_room_label_pobai_di` | ✅ 直接引用 | `FROM dws_ads_pa.ads_anu_operations_pobai_necklace_di` |
| `ads_op_o_onmic_gs_pobai_di` | ✅ 直接引用 | `FROM dws_ads_pa.ads_anu_operations_pobai_necklace_di` |
| `dts_anu_pa_ff_private_di` | ⚠️ 仅调度依赖 | SQL 未见直接引用，依赖为时序保障 |

**结论**：10 个作业直接读取该表数据，3 个为调度时序依赖（迁移时只需更新这 3 个作业的依赖 ID，无需修改 SQL）。

---

### 3.2 公共过滤逻辑（每个引用该表的作业都有）

**所有直接引用的作业均使用以下过滤条件，无一例外：**

```sql
-- ① 小号过滤（剔除异常账号）：
WHERE (is_small IS NULL OR is_small = 4)

-- ② 分区过滤（必须指定 dt，以下几种形式）：
WHERE dt = '${etl_date}'                                              -- 单日（5个作业）
WHERE dt IN ('${etl_date}', '${etl_date_sub_1}', date_sub('${etl_date}',6))  -- 7日窗口（2个作业）
WHERE dt >= date_sub('${etl_date}', 29) AND dt <= '${etl_date}'       -- 30日滚动（3个作业）
WHERE dt >= date_sub('${etl_date}', 89) AND dt <= '${etl_date}'       -- 90日窗口（1个作业）
```

**迁移建议**：新表迁移后保留相同字段名和含义，下游无需修改过滤逻辑；仅需将表路径从 `dws_ads_pa.ads_anu_operations_pobai_necklace_di` 替换为 `dws_pa.dws_anu_pobai_necklace_di`。

---

### 3.3 高频字段使用清单

以下字段被 **5 个及以上** 直接引用作业使用，为核心字段，迁移时**字段名不得变更**：

| 字段名 | 使用作业数 | 使用场景 |
|--------|-----------|---------|
| `uid` | 10 | 所有作业，破白用户主键 |
| `to_uid` | 8 | GS（主播）uid，用于关联主播维度或按GS聚合 |
| `dt` | 10 | 分区过滤，所有作业 |
| `is_small` | 10 | 小号过滤条件，所有作业 |
| `pobai_type` | 6 | 过滤项链破白（`= 2`），区分项链/头像框 |
| `region_name` | 7 | 大区维度，用于分组或下游聚合 |
| `consume_scene` | 5 | 破白消费场景，用于分类分析 |
| `reg_pobai_mar` | 5 | 破白距注册天数，用于新用户漏斗分析 |
| `pobai_time` | 5 | 破白时间戳，用于时序判断（与私聊时间对比） |
| `room_id` | 5 | 破白房间，用于关联房间维度或去重 |
| `broker_id` / `broker_name` | 4 | GS 公会信息 |

---

### 3.4 ⚠️ 关键：字段命名不一致问题（影响迁移兼容性）

下游任务引用的部分字段名与源表 SQL 中的字段名**存在差异**，需在迁移前核实实际存储的列名：

| 下游引用的字段名 | 源表 SQL 中的计算列 | 差异说明 |
|----------------|-------------------|---------|
| `charge_money` | `t8.charge_amt_7d` | 7日充值，下游用 `charge_money`，源表用 `charge_amt_7d` |
| `charge_money_1d` | `t8.charge_amt_1d` | 当日充值，命名不一致 |
| `charge_money_2d` | `t8.charge_amt_2d` | 2日充值，命名不一致 |
| `charge_money_14d` | `t8.charge_amt_14d AS charge_money_14d` | ✅ 有显式别名，一致 |
| `consume_money` | `t8.all_amt_7d` | 7日消费，命名完全不同 |
| `consume_money_1d` | `t8.all_amt_1d` | 当日消费，命名不同 |
| `consume_money_2d` | `t8.all_amt_2d` | 2日消费，命名不同 |
| `consume_money_14d` | `t8.all_amt_14d AS consume_money_14d` | ✅ 有显式别名，一致 |
| `consume_bus_money_7d` | `t8.consume_bus_amt_7d AS consume_bus_money_7d` | ✅ 有显式别名，一致 |
| `new_amt_30d` | 未在源表 SQL 中找到 | ⚠️ 字段来源不明，可能已废弃或来自旧版本 |
| `consume_scene2` | 未在源表 SQL 中找到 | ⚠️ 东南亚专用字段，源表 SQL 中未见定义 |

**处理建议**：
- 迁移前**必须执行** `DESCRIBE dws_ads_pa.ads_anu_operations_pobai_necklace_di` 获取实际列名清单
- 重构 SQL 中对 `charge_money` / `consume_money` 系列字段**使用显式 AS 别名**，保持与下游一致
- `new_amt_30d` 和 `consume_scene2` 若实际存在但源 SQL 中未定义，说明存在**历史遗留字段**，需在新表中保留或说明废弃

---

### 3.5 公共使用模式提取

通过分析 10 个直接引用作业，归纳出以下三类公共使用模式，可在迁移说明文档和下游改造指引中直接复用：

#### 模式 A：单日破白去重（取首次）

> **使用方**：`ads_active_gcc_user_room_info_di`、`ads_mkt_user_room_label_pobai_di`

```sql
-- 取同一用户在同一房间的首次破白记录
SELECT uid, room_id, to_uid, ...
FROM (
    SELECT uid, room_id, to_uid,
           ROW_NUMBER() OVER (PARTITION BY uid, room_id ORDER BY pobai_time) AS rk
    FROM dws_pa.dws_anu_pobai_necklace_di   -- 迁移后新表名
    WHERE dt = '${hivevar:etl_date}'
      AND (is_small IS NULL OR is_small = 4)
      AND pobai_type = 2
) t
WHERE rk = 1
```

#### 模式 B：按 GS 汇总破白次数

> **使用方**：`ads_service_user_anchor_di`、`ads_service_user_room_di`

```sql
-- 统计每个用户对每个 GS（或每个房间）的破白次数
SELECT
    uid,
    CAST(to_uid AS BIGINT) AS anchor_uid,   -- to_uid 关联 GS
    COUNT(1)               AS pobai_consume_cnt
FROM dws_pa.dws_anu_pobai_necklace_di
WHERE dt = '${hivevar:etl_date}'
  AND (is_small IS NULL OR is_small = 4)
GROUP BY uid, to_uid
```

#### 模式 C：多日窗口破白事件 + GS 私聊时序匹配

> **使用方**：`ads_op_o_auto_accost_from_user_di`、`ads_op_o_auto_accost_from_user_mid_di`

```sql
-- 在 N 日窗口内取破白事件，与 GS 私聊时间做时序对比（破白是否发生在私聊之后）
WITH pobai AS (
    SELECT
        to_uid,
        pobai_time,
        uid,
        charge_money      AS charge_money_7d,    -- 注意：实际存储字段名待确认
        charge_money_1d,
        region_name,
        pobai_type,
        consume_scene,
        reg_pobai_mar,
        consume_bus_money_7d,
        dt
    FROM dws_pa.dws_anu_pobai_necklace_di
    WHERE dt IN ('${hivevar:etl_date}', date_sub('${hivevar:etl_date}', 6))
      AND (is_small IS NULL OR is_small = 4)
)
-- 下游与 ads_itr_pchat_auto_user_acep_da 做时序 JOIN：
-- CASE WHEN pobai_time >= gs_first_pchat_time THEN ... END
```

#### 模式 D：长窗口（30/90日）历史破白标记

> **使用方**：`ads_pt_new_uid_grow_up_di`、`ads_op_o_regs_cj_ff_jt_di`、`ads_op_o_onmic_gs_pobai_di`

```sql
-- 取用户历史破白记录（用于判断新用户是否已完成破白，或统计破白后 N 日成长）
SELECT
    uid,
    reg_pobai_mar,
    pobai_time,
    consume_scene,
    room_id,
    to_uid,
    dt
FROM dws_pa.dws_anu_pobai_necklace_di
WHERE dt >= date_sub('${hivevar:etl_date}', 29)   -- 30日窗口，按需调整
  AND dt <= '${hivevar:etl_date}'
  AND pobai_type = 2
  AND (is_small IS NULL OR is_small = 4)
```

---

### 3.6 下游改造工作量评估

| 改造内容 | 涉及作业数 | 工作量 | 备注 |
|---------|-----------|--------|------|
| 替换表名路径 | 10 | 低 | 全局替换字符串，约 30 分钟 |
| 核实 `charge_money` 系列字段名 | 6 | 中 | 需先确认实际列名，再决定是否需改字段名 |
| 核实 `consume_money` 系列字段名 | 5 | 中 | 同上 |
| 查找 `new_amt_30d` / `consume_scene2` 来源 | 2 | 高 | 需回溯历史版本或与负责人确认 |
| 调度依赖 ID 替换 | 13 | 低 | 在调度系统中修改 relied_jobs |
| CK 同步任务源表变更 | 1 | 低~中 | 需核实 CK 同步配置方式 |

---

## 四、现有 SQL 存在的质量问题

迁移前需同步修复以下 SQL 缺陷：

### 3.1 字段重复定义（必须修复）

**问题 1：`consume_scene` 字段定义了两次**

```sql
-- 第一次（约第 52 行）
CASE WHEN property = 'business' THEN '商业房' ... END AS consume_scene

-- 第二次（约第 130 行，新增注释标注 2023-01-11）
CASE WHEN property = 'business' THEN '商业房' 
     WHEN types = 'chat-gift'  THEN '私聊'    -- 枚举值更详细
     ... END AS consume_scene   --- 新增 xmm   2023-01-11
```

**影响**：下游读取 `consume_scene` 字段时，Hive 以**最后一个同名字段**为准，第一个定义实际无效，但存在歧义，可维护性差。

**问题 2：`is_jz` 字段定义了两次**

```sql
,case when t16.uid is not null then '是' else '否' end  is_jz   -- t16: 14日1000价值用户
,case when t17.uid is not null then '是' else '否' end  is_jz   -- t17: KA用户（注册30日充值≥1万）
```

**影响**：两个完全不同的业务含义被挂在同一字段名下，下游必然只能取到其中一个，且含义混乱。

### 3.2 注释不规范（建议修复）

- 多处字段无中文注释（如 `broker_id`、`room_id` 等）
- 已注释掉的 `t7` 代码块（约 80 行）建议清理，避免干扰阅读
- 部分注释中含有开发时间戳（如 `2022-09-23`），应改为字段语义注释

### 3.3 引用了 ads 层表（需评估是否搬到公共层）

| 上游 ads 层表 | 库 | 建议 |
|--------------|-----|------|
| `ads_anu_operations_charge_cheat_di` | dws_ads_pa | 核实是否有公共层等价表；若无，迁移后继续跨层引用并做说明 |
| `ads_op_user_to_user_consume_days_df` | dws_ads_pa | 同上 |
| `ads_op_user_label_l_df` | dws_ads_pa | 同上 |
| `ads_channel_pay_valid_100_va_user_df` | dws_ads_ch_o | 跨业务线引用，需确认数据共享协议 |

---

## 五、重构方案

### 4.1 目标状态

| 属性 | 迁移前 | 迁移后 |
|------|--------|--------|
| 归属库 | `dws_ads_pa` | `dws_pa` |
| 表名 | `dws_ads_pa.ads_anu_operations_pobai_necklace_di` | `dws_pa.dws_anu_pobai_necklace_di` |
| 作业名 | `etl-hive-dws_ads_pa-ads_anu_operations_pobai_necklace_di` | `etl-hive-dws_pa-dws_anu_pobai_necklace_di` |
| 负责人 | u101356（分析师） | 数仓工程师（待指定，建议 ecs-liu.kaibao 或数仓组） |
| 调度时间 | 06:00 | 保持 06:00（可视下游 SLA 调整） |
| 分层标识 | 无（ads层） | `dws` 层 |

> **命名简化说明**：去掉原名中的 `anu_operations_` 冗余前缀，以 `dws_` 分层标识替代，表名更清晰。

### 4.2 建表 DDL

```sql
-- ============================================================
-- 作者: 数仓团队（原作者: u101356）
-- 日期: 2026-03-30
-- 用途: 项链破白用户行为宽表（公共层），记录用户通过项链礼物完成破白
--       后 N 日内的消费、充值、VIP 成长等核心行为指标，滚动保留 30 日
-- 来源迁移: dws_ads_pa.ads_anu_operations_pobai_necklace_di
-- ============================================================
CREATE TABLE IF NOT EXISTS dws_pa.dws_anu_pobai_necklace_di (
    -- 破白事件核心维度
    region_name          STRING    COMMENT '大区名称',
    uid                  STRING    COMMENT '破白用户 UID',
    reg_pobai_mar        INT       COMMENT '注册到破白的天数差',
    pobai_time           STRING    COMMENT '破白发生时间（精确到秒）',
    reg_date             STRING    COMMENT '用户注册日期',
    consume_scene        STRING    COMMENT '破白消费场景：商业房/家族房/个人房/私聊/1v1/付费照片/付费视频/其他',
    room_channel         STRING    COMMENT '破白房间所属渠道名称',
    room_id              STRING    COMMENT '破白发生房间 ID，无房间时为"无"',

    -- 接待主播信息
    reception_uid        STRING    COMMENT '破白时在场接待主播 UID',
    reception_name       STRING    COMMENT '接待主播昵称',
    to_uid               STRING    COMMENT '收到破白礼物的 GS（主播）UID',
    to_uid_nickname      STRING    COMMENT 'GS 昵称',
    gender_to_uid        STRING    COMMENT 'GS 性别：男/女/其他/未知',

    -- GS 公会信息
    broker_id            STRING    COMMENT 'GS 所属公会 ID',
    broker_name          STRING    COMMENT 'GS 所属公会名称',

    -- 用户画像
    city_name            STRING    COMMENT '破白用户所在城市',
    country_name         STRING    COMMENT '破白用户所在国家',
    uid_gender           STRING    COMMENT '破白用户性别（0默认/1男/2女）',
    uid_broker_id        STRING    COMMENT '破白用户所属公会 ID',
    uid_broker_name      STRING    COMMENT '破白用户所属公会名称',

    -- 破白事件属性
    gift_id              STRING    COMMENT '触发破白的礼物 ID（项链礼物）',
    pobai_type           INT       COMMENT '破白类型：2=项链破白',
    is_small             INT       COMMENT '是否作弊用户（异常充值标记，>0 为疑似作弊）',

    -- VIP 成长轨迹（破白后 N 日等级快照）
    vip_level            INT       COMMENT '破白前日 VIP 等级（-1 表示无数据）',
    vip_level_2d         INT       COMMENT '破白后第 2 日 VIP 等级',
    vip_level_7          INT       COMMENT '破白后第 7 日 VIP 等级',
    vip_level_14d        INT       COMMENT '破白后第 14 日 VIP 等级',

    -- L 标签
    label_l              STRING    COMMENT '破白当日 L 标签等级',
    label_l_7d           STRING    COMMENT '破白后 7 日 L 标签等级',
    label_l_14d          STRING    COMMENT '破白后 14 日 L 标签等级',

    -- 消费指标（对 GS 的打赏，伴伴币）
    all_amt_1d           DOUBLE    COMMENT '破白当日消费金额（伴伴币）',
    all_amt_2d           DOUBLE    COMMENT '破白后 2 日累计消费金额',
    all_amt_7d           DOUBLE    COMMENT '破白后 7 日累计消费金额',
    all_amt_14d          DOUBLE    COMMENT '破白后 14 日累计消费金额（含GS）',
    all_amt_30d          DOUBLE    COMMENT '破白后 30 日累计消费金额',
    consume_money_14d    DOUBLE    COMMENT '破白后 14 日累计消费总额（不含代充和订单）',
    consume_bus_money_7d  DOUBLE   COMMENT '破白后 7 日商业房消费金额',
    consume_bus_money_14d DOUBLE   COMMENT '破白后 14 日商业房消费金额',
    consume_days_7d      INT       COMMENT '破白后 7 日内消费天数',
    consume_days_14d     INT       COMMENT '破白后 14 日内消费天数',

    -- 充值指标（伴伴币）
    charge_amt_1d        DOUBLE    COMMENT '破白当日充值金额',
    charge_amt_2d        DOUBLE    COMMENT '破白后 2 日累计充值',
    charge_amt_7d        DOUBLE    COMMENT '破白后 7 日累计充值',
    charge_amt_14d       DOUBLE    COMMENT '破白后 14 日累计充值',
    charge_amt_30d       DOUBLE    COMMENT '破白后 30 日累计充值',
    charge_money_14d     DOUBLE    COMMENT '破白后 14 日累计充值（含代充）',
    charge_cnt_7d        INT       COMMENT '破白后 7 日内充值次数',
    charge_cnt_14d       INT       COMMENT '破白后 14 日内充值次数',
    rep_charge_money_7d  DOUBLE    COMMENT '7 日内复购金额（7日充值 - 首日充值）',
    rep_charge_money_14d DOUBLE    COMMENT '14 日内复购金额（14日充值 - 首日充值）',

    -- uid→GS 的消费金额（分维度）
    gs_amt_1d            DOUBLE    COMMENT '破白当日 uid 给该 GS 的消费（伴伴币）',
    gs_amt_2d            DOUBLE    COMMENT '破白后 2 日 uid 给该 GS 的消费',
    gs_amt_3d            DOUBLE    COMMENT '破白后 3 日 uid 给该 GS 的消费',
    gs_amt_7d            DOUBLE    COMMENT '破白后 7 日 uid 给该 GS 的消费',
    gs_amt_8d            DOUBLE    COMMENT '破白后 8 日 uid 给该 GS 的消费',
    gs_amt_14d           DOUBLE    COMMENT '破白后 14 日 uid 给该 GS 的消费',
    gs_amt_15d           DOUBLE    COMMENT '破白后 15 日 uid 给该 GS 的消费',
    gs_amt_16d           DOUBLE    COMMENT '破白后 16 日 uid 给该 GS 的消费',
    gs_amt_30d           DOUBLE    COMMENT '破白后 30 日 uid 给该 GS 的消费',
    gs_amt_31d           DOUBLE    COMMENT '破白后 31 日 uid 给该 GS 的消费',

    -- 进房停留时长
    bus_room_stay_dur_7d  DOUBLE   COMMENT '破白后 7 日商业房停留时长（分钟）',
    bus_room_stay_dur_14d DOUBLE   COMMENT '破白后 14 日商业房停留时长（分钟）',

    -- 渠道信息（破白用户）
    channel_id           STRING    COMMENT '破白用户渠道 ID',
    channel_name         STRING    COMMENT '破白用户渠道名称',
    parent_channel_id    STRING    COMMENT '破白用户父渠道 ID',
    parent_channel_name  STRING    COMMENT '破白用户父渠道名称',

    -- 渠道信息（GS/主播）
    gs_channel_id        STRING    COMMENT 'GS 渠道 ID',
    gs_channel_name      STRING    COMMENT 'GS 渠道名称',
    gs_parent_channel_id STRING    COMMENT 'GS 父渠道 ID',
    gs_parent_channel_name STRING  COMMENT 'GS 父渠道名称',

    -- 行为标签
    days_to_first_charge INT       COMMENT '破白距离首充的天数（负数表示破白前已充值）',
    is_xinren_pchat_3d   INT       COMMENT '破白前 3 天内是否通过新人榜私聊过该 GS（0/1）',
    is_jz_value          STRING    COMMENT '是否14日1000价值用户（是/否）',
    is_ka_user           STRING    COMMENT '是否KA用户（注册30日充值≥1万币，是/否）'
)
COMMENT '破白用户行为宽表（项链礼物触发），记录破白事件发生后 30 日内用户的行为、消费、充值、成长轨迹，每日全量覆写当日破白数据，窗口保留 30 日'
PARTITIONED BY (dt STRING COMMENT '数据日期，格式 YYYYMMDD，即破白发生日期')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
```

### 4.3 重构后 ETL SQL

```sql
-- ============================================================
-- @author  : 数仓团队（原作者 u101356）
-- @date    : 2026-03-30
-- @purpose : 项链破白用户宽表，迁移自 dws_ads_pa.ads_anu_operations_pobai_necklace_di
-- @AUDIT_ID: DW-MIGRATION-2026-001
-- ============================================================

-- MR 内存配置（与原始保持一致）
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts=-Xmx3686m;
SET mapreduce.reduce.memory.mb=8192;
SET mapreduce.reduce.java.opts=-Xmx3686m;
SET hive.exec.parallel=TRUE;
SET hive.vectorized.execution.enabled=TRUE;

WITH

-- ── t1: 触发破白的项链礼物事件（主驱动，近 30 日） ──
t1 AS (
    SELECT
        uid,
        from_unixtime(pobai_ts, 'yyyy-MM-dd') AS pobai_date,
        from_unixtime(pobai_ts)               AS pobai_time,
        room_id,
        dst_uid                               AS to_uid,
        type                                  AS types,
        gift_id,
        dt,
        2                                     AS pobai_type
    FROM dws_oversea.dwd_user_pobai_necklace_di
    WHERE dt >= date_sub('${hivevar:etl_date}', 29)
),

-- ── t2: 用户基础维度（昵称、注册日期、大区、城市等） ──
t2 AS (
    SELECT
        uid,
        nickname,
        from_unixtime(create_time, 'yyyy-MM-dd') AS reg_date,
        region_name,
        city_name,
        country_name,
        gender,
        broker_id,
        broker_name,
        dt
    FROM dws_oversea.dim_user_df
    WHERE dt >= date_sub('${hivevar:etl_date}', 29)
),

-- ── t3: 房间维度（渠道、属性） ──
t3 AS (
    SELECT
        room_id,
        settlement_channel_name,
        property,
        dt
    FROM dws_oversea.ads_mkt_dim_room_da
    WHERE dt >= date_sub('${hivevar:etl_date}', 29)
      AND room_id > 0
),

-- ── t4: 接待主播上麦记录（匹配破白时在场的 GS） ──
t4 AS (
    SELECT
        room_id,
        uid,
        start_time,
        end_time,
        dt
    FROM dws_oversea.dwd_log_onmic_room_di
    WHERE role = 'reception'
      AND duration < 86400
      AND dt >= date_sub('${hivevar:etl_date}', 29)
),

-- ── t5: GS 当前公会归属（取当日快照） ──
t5 AS (
    SELECT
        uid,
        broker_id,
        broker_name
    FROM dws_oversea.dim_user_df
    WHERE dt = '${hivevar:etl_date}'
      AND broker_id > 0
),

-- ── t8: 破白后 N 日消费/充值聚合指标 ──
t8 AS (
    SELECT
        uid,
        consume_amt_1d / 100  AS all_amt_1d,
        consume_amt_2d / 100  AS all_amt_2d,
        consume_amt_7d / 100  AS all_amt_7d,
        consume_amt_14d / 100 AS all_amt_14d,
        consume_amt_30d / 100 AS all_amt_30d,
        consume_bus_amt_7d / 100  AS consume_bus_amt_7d,
        consume_bus_amt_14d / 100 AS consume_bus_amt_14d,
        consume_days_7d,
        consume_days_14d,
        charge_amt_1d / 100   AS charge_amt_1d,
        charge_amt_2d / 100   AS charge_amt_2d,
        charge_amt_7d / 100   AS charge_amt_7d,
        charge_amt_14d / 100  AS charge_amt_14d,
        charge_amt_30d / 100  AS charge_amt_30d,
        charge_cnt_7d,
        charge_cnt_14d,
        dt
    FROM dws_oversea.ads_pt_trans_next_days_user_di
    WHERE dt >= date_sub('${hivevar:etl_date}', 29)
),

-- ── t9: 作弊用户标记 ──
t9 AS (
    SELECT
        uid,
        is_check AS is_cheat,
        dt
    FROM dws_ads_pa.ads_anu_operations_charge_cheat_di
    WHERE dt >= date_sub('${hivevar:etl_date}', 29)
      AND is_check > 0
),

-- ── t10: L 标签（用户价值分层标签） ──
t10 AS (
    SELECT
        uid,
        label_l,
        label_l_7d,
        label_l_14d,
        dt
    FROM dws_ads_pa.ads_op_user_label_l_df
    WHERE dt >= date_sub('${hivevar:etl_date}', 29)
),

-- ── t11: 首充用户信息 ──
t11 AS (
    SELECT
        uid,
        from_unixtime(first_charge_time_da_001, 'yyyy-MM-dd') AS first_charge_time,
        CASE WHEN first_charge_scene_da_001 = 'agent-package' THEN 1 ELSE 0 END AS is_pay_agent,
        dt
    FROM dws_oversea.tms_trans_charge_user_da
    WHERE dt = '${hivevar:etl_date}'
      AND first_charge_scene_da_001 != 'gs-agent-package'
      AND from_unixtime(first_charge_time_da_001, 'yyyy-MM-dd') >= date_sub('${hivevar:etl_date}', 29)
),

-- ── t12: 用户渠道归属（取前一日快照） ──
t12 AS (
    SELECT *
    FROM dws_oversea.dim_user_channel_df
    WHERE dt = date_sub(CURRENT_DATE(), 1)
),

-- ── t13: uid 对特定 GS 的 N 日消费金额（用户→GS 交互维度） ──
t13 AS (
    SELECT
        uid,
        to_uid                AS acep_uid,
        all_amt_1d,
        all_amt_2d,
        all_amt_3d,
        all_amt_7d,
        all_amt_8d,
        all_amt_14d,
        all_amt_15d,
        all_amt_16d,
        all_amt_30d,
        all_amt_31d,
        dt
    FROM dws_ads_pa.ads_op_user_to_user_consume_days_df
    WHERE dt >= date_sub('${hivevar:etl_date}', 29)
),

-- ── t14: 破白用户是否在破白前 3 日通过新人榜私聊过该 GS ──
t14 AS (
    SELECT
        uid,
        to_uid,
        IF(send_msg_cnt_3d > 0, 1, 0) AS is_xinren_pchat_3d,
        dt
    FROM dws_oversea.ads_pt_log_pobai_xinren_pchat_user_to_user_di
    WHERE dt >= date_sub('${hivevar:etl_date}', 29)
),

-- ── t15: 破白后 N 日商业房停留时长 ──
t15 AS (
    SELECT
        uid,
        room_stay_dur_7d / 60  AS room_stay_dur_7d,
        room_stay_dur_14d / 60 AS room_stay_dur_14d,
        dt
    FROM dws_oversea.ads_pt_anu_next_days_user_di
    WHERE dt >= date_sub('${hivevar:etl_date}', 29)
),

-- ── t16: 14 日 1000 币价值用户标记（jz 价值用户） ──
t16 AS (
    SELECT uid
    FROM dws_ads_ch_o.ads_channel_pay_valid_100_va_user_df
    WHERE dt = IF('${hivevar:etl_date}' < '2023-07-26', '2023-07-26', '${hivevar:etl_date}')
      AND is_valid = 1
      AND user_app_id = 5
      AND 14day_1000_value = 1
),

-- ── t17: KA 用户（注册 30 日内充值 ≥ 1 万币） ──
t17 AS (
    SELECT
        uid,
        dt
    FROM dws_ads_pa.ads_ka_charge_ht_df
    WHERE dt > date_sub('${hivevar:etl_date}', 30)
      AND dt <= '${hivevar:etl_date}'
),

-- ── p11: 破白用户 VIP 等级 N 日快照 ──
p11 AS (
    SELECT
        uid,
        vip_level_1d  AS vip_level_0d,
        vip_level     AS vip_level_1d,
        vip_level_f2d AS vip_level_2d,
        vip_level_f3d AS vip_level_3d,
        vip_level_f7d AS vip_level_7d,
        vip_level_f14d AS vip_level_14d,
        dt
    FROM dws_oversea.ads_vip_level_next_days_user_df
    WHERE dt >= date_sub('${hivevar:etl_date}', 29)
)

INSERT OVERWRITE TABLE dws_pa.dws_anu_pobai_necklace_di PARTITION (dt)
SELECT
    -- 基础维度
    t2.region_name,
    t1.uid,
    datediff(t1.pobai_date, t2.reg_date)  AS reg_pobai_mar,
    t1.pobai_time,
    t2.reg_date,

    -- 破白消费场景（合并为统一定义，覆盖所有场景）
    CASE
        WHEN t3.property = 'business' THEN '商业房'
        WHEN t3.property = 'fleet'    THEN '家族房'
        WHEN t3.property = 'vip'      THEN '个人房'
        WHEN COALESCE(t3.property, '') <> '' THEN '其他房间'
        WHEN t1.types = 'chat-gift'   THEN '私聊'
        WHEN t1.types = '1'           THEN '1v1私聊'
        WHEN t1.types = '8'           THEN '1v1首次付费私聊'
        WHEN t1.types = '5'           THEN '订单场景'
        WHEN t1.types = '10'          THEN '付费照片'
        WHEN t1.types = '11'          THEN '付费视频'
        ELSE '其他'
    END                                    AS consume_scene,

    t3.settlement_channel_name             AS room_channel,
    COALESCE(t1.room_id, '无')            AS room_id,
    COALESCE(t4.uid, '无')               AS reception_uid,
    COALESCE(k5.nickname, '无')          AS reception_name,
    t1.to_uid,
    k7.nickname                           AS to_uid_nickname,
    COALESCE(t5.broker_id, '无')         AS broker_id,
    COALESCE(t5.broker_name, '无')       AS broker_name,
    t9.is_cheat                           AS is_small,

    -- VIP 成长快照
    COALESCE(p11.vip_level_0d,  -1)      AS vip_level,
    COALESCE(p11.vip_level_2d,  -1)      AS vip_level_2d,
    COALESCE(p11.vip_level_7d,  -1)      AS vip_level_7,
    COALESCE(p11.vip_level_14d, -1)      AS vip_level_14d,

    -- L 标签
    t10.label_l,
    t10.label_l_7d,
    t10.label_l_14d,

    -- 消费指标
    t8.all_amt_1d,
    t8.all_amt_2d,
    t8.all_amt_7d,
    t8.all_amt_14d,
    t8.all_amt_30d,
    t8.consume_bus_amt_14d                AS consume_money_14d,
    t8.consume_bus_amt_7d                 AS consume_bus_money_7d,
    t8.consume_bus_amt_14d                AS consume_bus_money_14d,
    t8.consume_days_7d,
    t8.consume_days_14d,

    -- 充值指标
    t8.charge_amt_1d,
    t8.charge_amt_2d,
    t8.charge_amt_7d,
    t8.charge_amt_14d,
    t8.charge_amt_30d,
    t8.charge_amt_14d                     AS charge_money_14d,
    t8.charge_cnt_7d,
    t8.charge_cnt_14d,
    COALESCE(t8.charge_amt_7d,  0) - COALESCE(t8.charge_amt_1d, 0)  AS rep_charge_money_7d,
    COALESCE(t8.charge_amt_14d, 0) - COALESCE(t8.charge_amt_1d, 0)  AS rep_charge_money_14d,

    -- uid→GS 消费（对特定 GS 的打赏）
    t13.all_amt_1d                        AS gs_amt_1d,
    t13.all_amt_2d                        AS gs_amt_2d,
    t13.all_amt_3d                        AS gs_amt_3d,
    t13.all_amt_7d                        AS gs_amt_7d,
    t13.all_amt_8d                        AS gs_amt_8d,
    t13.all_amt_14d                       AS gs_amt_14d,
    t13.all_amt_15d                       AS gs_amt_15d,
    t13.all_amt_16d                       AS gs_amt_16d,
    t13.all_amt_30d                       AS gs_amt_30d,
    t13.all_amt_31d                       AS gs_amt_31d,

    -- 进房停留
    t15.room_stay_dur_7d                  AS bus_room_stay_dur_7d,
    t15.room_stay_dur_14d                 AS bus_room_stay_dur_14d,

    -- 渠道信息（破白用户侧）
    t12_uid.channel_id,
    t12_uid.channel_name,
    t12_uid.parent_channel_id,
    t12_uid.parent_channel_name,

    -- 渠道信息（GS 侧）
    t12_gs.channel_id                     AS gs_channel_id,
    t12_gs.channel_name                   AS gs_channel_name,
    t12_gs.parent_channel_id              AS gs_parent_channel_id,
    t12_gs.parent_channel_name            AS gs_parent_channel_name,

    -- 其他行为标签
    t1.gift_id,
    t1.pobai_type,
    datediff(t1.pobai_date, t11.first_charge_time)  AS days_to_first_charge,
    COALESCE(t14.is_xinren_pchat_3d, 0)             AS is_xinren_pchat_3d,
    CASE WHEN t16.uid IS NOT NULL THEN '是' ELSE '否' END AS is_jz_value,
    CASE WHEN t17.uid IS NOT NULL THEN '是' ELSE '否' END AS is_ka_user,

    -- 用户画像
    t2.city_name,
    t2.country_name,
    CASE WHEN k7.gender = 1 THEN '男' WHEN k7.gender = 2 THEN '女'
         WHEN k7.gender = 3 THEN '其他' ELSE '未知' END AS gender_to_uid,
    t2.broker_id                          AS uid_broker_id,
    t2.broker_name                        AS uid_broker_name,

    t1.dt

FROM t1
LEFT JOIN t2          ON t1.uid     = t2.uid     AND t1.dt = t2.dt
LEFT JOIN t3          ON t1.room_id = t3.room_id AND t1.dt = t3.dt
LEFT JOIN t4          ON t1.room_id = t4.room_id AND t1.dt = t4.dt
                      AND t1.pobai_time >= t4.start_time
                      AND t1.pobai_time <= t4.end_time
                      AND t1.to_uid    = t4.uid
LEFT JOIN t2 AS k5    ON t4.uid     = k5.uid     AND t4.dt = k5.dt
LEFT JOIN t5          ON t1.to_uid  = t5.uid
LEFT JOIN t2 AS k7    ON t1.to_uid  = k7.uid     AND t1.dt = k7.dt
LEFT JOIN t8          ON t1.uid     = t8.uid     AND t1.dt = t8.dt
LEFT JOIN t9          ON t1.uid     = t9.uid     AND t1.dt = t9.dt
LEFT JOIN p11         ON t1.uid     = p11.uid    AND t1.dt = p11.dt
LEFT JOIN t10         ON t1.uid     = t10.uid    AND t1.dt = t10.dt
LEFT JOIN t11         ON t1.uid     = t11.uid    AND t1.dt = t11.dt
LEFT JOIN t12 AS t12_uid ON t1.uid    = t12_uid.uid
LEFT JOIN t12 AS t12_gs  ON t1.to_uid = t12_gs.uid
LEFT JOIN t13         ON t1.uid     = t13.uid    AND t1.dt = t13.dt AND t1.to_uid = t13.acep_uid
LEFT JOIN t14         ON t1.to_uid  = t14.uid    AND t1.dt = t14.dt AND t1.uid    = t14.to_uid
LEFT JOIN t15         ON t1.uid     = t15.uid    AND t1.dt = t15.dt
LEFT JOIN t16         ON t1.uid     = t16.uid
LEFT JOIN t17         ON t1.uid     = t17.uid    AND t1.dt = t17.dt
;

ANALYZE TABLE dws_pa.dws_anu_pobai_necklace_di PARTITION (dt = '${hivevar:etl_date}') COMPUTE STATISTICS;
ANALYZE TABLE dws_pa.dws_anu_pobai_necklace_di PARTITION (dt = '${hivevar:etl_date}') COMPUTE STATISTICS FOR COLUMNS;
```

---

## 六、迁移执行步骤

### 5.1 迁移时间线

```
第 1~2 天（准备）
    ├── 执行建表 DDL，在 dws_pa 创建新表
    ├── 在调度系统中新建作业 etl-hive-dws_pa-dws_anu_pobai_necklace_di
    └── 配置好依赖关系（与旧作业相同的上游依赖）

第 3~9 天（双跑验证，7 天）
    ├── 新旧两个作业同时运行
    ├── 每日自动对比新旧表的行数、关键字段分布（见 5.2 验证 SQL）
    └── 连续 5 天数据 100% 一致（行数误差 <0.1%）后可进入切换

第 10 天（切换）
    ├── 通知 13 个下游作业负责人（u101356），统一修改依赖表路径
    ├── 将 13 个下游作业的 relied_jobs 从 449695 改为新作业 ID
    ├── 下游 CK 同步任务（ID=449697）同步变更源表为 dws_pa.dws_anu_pobai_necklace_di
    └── 当日观察所有下游作业是否正常完成

第 11~17 天（观察期）
    ├── 持续监控下游 13 个作业运行状态
    ├── 如有异常立即回滚（切回旧依赖）
    └── 确认无异常后进入下线阶段

第 18 天（下线旧作业）
    ├── 停用旧作业 etl-hive-dws_ads_pa-ads_anu_operations_pobai_necklace_di
    ├── 旧表 dws_ads_pa.ads_anu_operations_pobai_necklace_di 保留 30 天后归档
    └── 更新数据门户/文档中的表引用
```

### 5.2 双跑验证 SQL

```sql
-- 每日执行，验证新旧表数据一致性
WITH old_stats AS (
    SELECT
        COUNT(*)                          AS row_cnt,
        COUNT(DISTINCT uid)               AS uid_cnt,
        SUM(all_amt_7d)                   AS total_consume_7d,
        SUM(charge_amt_7d)                AS total_charge_7d,
        AVG(reg_pobai_mar)                AS avg_reg_pobai_mar
    FROM dws_ads_pa.ads_anu_operations_pobai_necklace_di
    WHERE dt = '${hivevar:etl_date}'
),
new_stats AS (
    SELECT
        COUNT(*)                          AS row_cnt,
        COUNT(DISTINCT uid)               AS uid_cnt,
        SUM(all_amt_7d)                   AS total_consume_7d,
        SUM(charge_amt_7d)                AS total_charge_7d,
        AVG(reg_pobai_mar)                AS avg_reg_pobai_mar
    FROM dws_pa.dws_anu_pobai_necklace_di
    WHERE dt = '${hivevar:etl_date}'
)
SELECT
    o.row_cnt              AS old_row_cnt,
    n.row_cnt              AS new_row_cnt,
    ABS(o.row_cnt - n.row_cnt) / NULLIF(o.row_cnt, 0) AS row_diff_pct,
    o.uid_cnt              AS old_uid_cnt,
    n.uid_cnt              AS new_uid_cnt,
    ROUND(o.total_consume_7d, 2) AS old_consume,
    ROUND(n.total_consume_7d, 2) AS new_consume,
    ROUND(o.total_charge_7d,  2) AS old_charge,
    ROUND(n.total_charge_7d,  2) AS new_charge
FROM old_stats o
CROSS JOIN new_stats n;
```

**通过标准**：`row_diff_pct < 0.001`（行数误差 < 0.1%），关键指标相对误差 < 0.1%。

### 5.3 下游作业修改清单

迁移切换日，需通知 **u101356** 修改以下 13 个作业的上游依赖（将旧作业 ID `449695` 替换为新作业 ID）：

| 作业 ID | 作业名称 | 修改内容 |
|---------|---------|----------|
| 483562 | ads_service_user_anchor_di | relied_jobs 中替换 449695 |
| 483564 | ads_service_user_room_di | relied_jobs 中替换 449695 |
| **449697** | **tp-hive-ck-dws_ads_pa-ads_anu_operations_pobai_necklace_di** | **⚠️ 源表路径必须同步变更** |
| 449779 | dts_anu_pt_pobai_private_di | relied_jobs 中替换 449695 |
| 449786 | ads_op_o_auto_accost_from_user_di | relied_jobs 中替换 449695 |
| 449793 | ads_active_gcc_user_room_info_di | relied_jobs 中替换 449695 |
| 449848 | ads_anu_itr_active_user_info_di | relied_jobs 中替换 449695 |
| 450151 | ads_op_o_auto_accost_from_user_mid_di | relied_jobs 中替换 449695 |
| 450267 | ads_pt_new_uid_grow_up_di | relied_jobs 中替换 449695 |
| 450310 | ads_op_o_regs_cj_ff_jt_di | relied_jobs 中替换 449695 |
| 450350 | ads_mkt_user_room_label_pobai_di | relied_jobs 中替换 449695 |
| 450353 | ads_op_o_onmic_gs_pobai_di | relied_jobs 中替换 449695 |
| 453855 | dts_anu_pa_ff_private_di | relied_jobs 中替换 449695 |

---

## 七、风险评估与应对

| 风险 | 等级 | 应对措施 |
|------|------|----------|
| CK 同步任务断流 | 🔴 高 | 切换前单独验证 CK 同步（ID=449697）的源表配置，确认能读到新表数据后再切换 |
| 下游 SQL 中硬编码旧表名 | 🔴 高 | 排查 13 个下游作业的 SQL 代码，确认是否直接引用 `dws_ads_pa.ads_anu_operations_pobai_necklace_di` 并替换 |
| 字段名变更导致下游报错 | 🟡 中 | 重构后表中 `is_jz` 拆分为 `is_jz_value` 和 `is_ka_user`，需逐一确认下游是否有引用该字段 |
| 跨 ads 层引用（t9/t10/t13） | 🟡 中 | 迁移后仍引用 `dws_ads_pa` 中的 3 张表，在表 COMMENT 中注明跨层依赖原因，评估是否后续公共化 |
| 双跑期资源翻倍 | 🟢 低 | 双跑周期控制在 7 天内，可接受；新旧作业使用相同 YARN 队列 `olap` |

---

## 八、长期治理建议

1. **表命名规范**：新表采用 `dws_pa.dws_[业务线]_[主题]_[粒度]_[周期]` 格式，去掉 `anu_operations_` 等冗余前缀
2. **跨层依赖治理**：t9（charge_cheat）/ t10（label_l）/ t13（user_to_user_consume）三张 ads 层来源表，建议评估是否下沉到数仓公共层，彻底解耦
3. **下游作业管控**：迁移后的 `dws_pa.dws_anu_pobai_necklace_di` 设置下游依赖数告警（当前 13 个，超过 15 个时触发评审）
4. **SLA 配置**：新作业在调度系统中补充 SLA 超时告警（建议 120 分钟），旧作业 exec_timeout_minutes=0（无告警）是问题根源之一
5. **责任人移交**：正式移交给数仓工程师后，将 u101356 保留为"业务对接人"角色，确保业务需求变更有对接窗口

---

*文档生成基于 20260330 调度报告数据（etl_date = 2026-03-29）*
