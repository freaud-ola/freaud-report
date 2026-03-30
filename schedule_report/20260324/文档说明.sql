-- 作业信息表结构
CREATE EXTERNAL TABLE `dws_meta.dts_sch_job_df`(
  `id` bigint COMMENT '自增id, 业务 PRIMARY KEY(id)', 
  `project_id` bigint COMMENT '工程id', 
  `name` string COMMENT '任务名称, 业务 UNIQUE KEY(name)', 
  `description` string COMMENT '任务描述', 
  `cron` string COMMENT '调度表达式', 
  `owner` string COMMENT '任务负责人', 
  `job_type` string COMMENT '任务类型，etl,tp,op', 
  `job_args` string COMMENT 'job执行参数', 
  `jvm_args` string COMMENT 'job的jvm参数', 
  `mask` bigint COMMENT '可见状态', 
  `retry_time` bigint COMMENT '重试次数', 
  `retry_interval_time` bigint COMMENT '重试间隔时间，秒', 
  `notice_receiver_ids` string COMMENT '告警接收者', 
  `exec_timeout_minutes` bigint COMMENT '任务执行超时告警，分钟阈值', 
  `fast_fail_minutes` bigint COMMENT '超过触发时间指定时间后未成功，直接失败，0代表不超时', 
  `priority` bigint COMMENT '优先级', 
  `version_id` bigint COMMENT '代码版本id', 
  `relied_jobs` string COMMENT '依赖上游的 job id', 
  `oss_rely_files` string COMMENT 'oss依赖文件，json格式', 
  `self_rely` int COMMENT '是否开启自依赖 0-关闭，1-开启', 
  `enabled` int COMMENT '是否启用，0-否，1-是', 
  `deleted` int COMMENT '是否删除，0-否，1-是', 
  `create_time` string COMMENT '创建时间', 
  `update_time` string COMMENT '更新时间', 
  `database_name` string COMMENT '数据库')
COMMENT '任务信息表,bdp_schedule.sch_job'
PARTITIONED BY ( 
  `dt` string COMMENT '日期，格式为yyyy-MM-dd')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'oss://sg-emr-data/dw/dws_meta/dts_sch_job_df'
TBLPROPERTIES (
  'last_modified_by'='ecs-luo.xiangzhou', 
  'last_modified_time'='1756800251', 
  'orc.compress'='SNAPPY', 
  'spark.sql.cached'='false', 
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.partCol.0'='dt', 
  'transient_lastDdlTime'='1756800251')

-- 查询 sql
SELECT  * from  dws_meta.dts_sch_job_df where dt='2026-03-23'  AND enabled=1 and deleted=0 

--------------------------------------------------------------------------------------
-- 调度历史日志表结构
CREATE EXTERNAL TABLE `dws_meta.dwd_sch_history_job_di`(
  `history_id` bigint COMMENT 'history id', 
  `job_id` bigint COMMENT 'job id', 
  `job_name` string COMMENT '任务名', 
  `owner_role` string COMMENT '责任人角色：数仓/BP/其他', 
  `job_type` string COMMENT 'job类型（etl/tp/jar）', 
  `bus_line` string COMMENT '业务线：bban/pt/Veeka/ps/海外增长/皮队友/凶手/lolfi/国内增长...', 
  `dws_level` string COMMENT '表所属层级(针对数仓而言)：ods/dts/dwd/dim/tms/ads', 
  `job_desc` string COMMENT '任务描述', 
  `fire_time` string COMMENT '触发时间', 
  `start_time` string COMMENT 'history开始时间', 
  `end_time` string COMMENT 'history结束时间', 
  `status` string COMMENT 'history状态', 
  `reason_code` string COMMENT '原因码', 
  `executor` string COMMENT '执行人', 
  `dispatcher` string COMMENT '调度者', 
  `execute_type` string COMMENT '任务类型', 
  `retry_num` bigint COMMENT '重试次数', 
  `create_time` string COMMENT 'history创建时间', 
  `update_time` string COMMENT 'history更新时间')
COMMENT '执行历史明细(job粒度)'
PARTITIONED BY ( 
  `dt` string COMMENT '日期(binlog产生日期)，格式为yyyy-MM-dd')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'oss://sg-emr-data/dw/dws_meta/dwd_sch_history_job_di'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'orc.compress'='SNAPPY', 
  'spark.sql.cached'='false', 
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.partCol.0'='dt', 
  'transient_lastDdlTime'='1675826704')

-- 查询 sql  从 2026-02-01 至 2026-03-05 成功执行的自动任务
SELECT  * from  dws_meta.dwd_sch_history_job_di where dt>='2026-03-22' and dt<='2026-03-23' and status = 'SUCCESSFUL' and execute_type='AUTO'
--------------------------------------------------------------------------------------
-- 资源使用情况表结构
CREATE EXTERNAL TABLE `dws_meta.dwd_sch_history_application_di`(
  `history_id` bigint COMMENT 'history id', 
  `job_id` bigint COMMENT 'job id', 
  `job_name` string COMMENT '任务名', 
  `job_desc` string COMMENT '任务描述', 
  `fire_time` string COMMENT '触发时间', 
  `start_time` string COMMENT 'history开始时间', 
  `end_time` string COMMENT 'history结束时间', 
  `status` string COMMENT 'history状态', 
  `reason_code` string COMMENT '原因码', 
  `executor` string COMMENT '执行人', 
  `dispatcher` string COMMENT '调度者', 
  `execute_type` string COMMENT '任务类型', 
  `retry_num` bigint COMMENT '重试次数', 
  `create_time` string COMMENT 'history创建时间', 
  `update_time` string COMMENT 'history更新时间', 
  `application_id` string COMMENT 'application id', 
  `username` string COMMENT '提交任务用户', 
  `name` string COMMENT '任务名称', 
  `queue` string COMMENT '任务队列', 
  `state` string COMMENT '任务状态', 
  `final_status` string COMMENT '任务最终状态', 
  `application_type` string COMMENT '任务类型', 
  `priority` bigint COMMENT '任务优先级', 
  `started_time` string COMMENT '任务开始时间', 
  `finished_time` string COMMENT '任务结束时间', 
  `elapsed_time` bigint COMMENT '任务执行耗时，秒', 
  `memory_seconds` bigint COMMENT '任务使用的总内存，MB', 
  `vcore_seconds` bigint COMMENT '任务使用的总虚拟核数')
COMMENT '执行历史明细'
PARTITIONED BY ( 
  `dt` string COMMENT '日期(binlog产生日期)，格式为yyyy-MM-dd')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'oss://sg-emr-data/dw/dws_meta/dwd_sch_history_application_di'
TBLPROPERTIES (
  'orc.compress'='SNAPPY', 
  'spark.sql.cached'='false', 
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.partCol.0'='dt', 
  'transient_lastDdlTime'='1653044913')

-- 查询 sql 从 2026-03-01 至 2026-03-10 成功执行的hive application情况
select * from dws_meta.dwd_sch_history_application_di where dt>='2026-03-22' and dt<='2026-03-23' and status='SUCCESSFUL' 
--------------------------------------------------------------------------------------
-- etl 任务 sql
SELECT     
            dt
           ,m.id  -- job_id
           ,m.name -- job_name
           ,n.code  -- 作业sql代码，etl 任务对应的是hivesql，其他类型任务可以忽略代码
  from(
    SELECT id,name,version_id
     from dws_meta.dts_sch_job_df
     WHERE
        dt = '2026-03-23' AND job_type='ETL' AND enabled=1 and deleted=0
     ) m JOIN  dws_meta.dts_sch_code_df n on n.dt='2026-03-23' and n.job_id=m.id and m.version_id=n.version_id


--------------------------------------------------------------------------------------

上面查询结果我已经导入到 excel 中，在本目录中可以看到。分别对应的是：作业信息、调度历史日志、hive调度资源使用日志、etl任务sql代码。
任务名 job_name中包含库名和表名，表明是分层的，ods/dts/dwd/dim/tms/ads 。
要求你根据这4个元数据表，分析调度作业依赖关系，按照库名和分层进行分类。判断出每个库每个层级的作业从1点至10点的每个小时完成进度。结合统计学方法给出分析结论。
作业名限制只要 etl ,tp 开头的任务，任务名规范是：
- etl 任务名规范：etl-hive-库名-表名
- tp 任务名规范：tp-hive-ck_库名-表名
库名如果带有 ads，那么分层全部按照 ads 层归类。

要求：
1、先给出分析的大纲，包括但不限于，统计当前作业的现状，作业数、类型分布情况、依赖情况、调度时间分布等等。
2、有了大纲以后再根据已有的四个数据去计算出各个细分项。
3、有了细分项以后根据数据情况给出分析报告结果，给出各项优化措施以及实施的步骤。报告尽可能覆盖全面，分析结果尽可能客观，避免主观臆断。
4、报告输出为单页的 HTML 页面，页面内容包括大纲、细分项、分析报告结果，尽可能多的采用现代化图表和数据可视化技术，确保报告的易读性和美观性。
5、报告输出到当前目录中。



1、调度配置 Gap 优化建议这个数据 >= 30min全部导出为excel文件给我。
2、伪中间层作业明细 全部导出为excel文件给我。
3、高耗时作业 SQL 优化分析 全部导出为excel文件给我。
上面的文件全部放到 export_data 目录中