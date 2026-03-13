#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调度作业全面瓶颈分析报告生成脚本 v3
数据来源：作业信息、调度历史日志、hive调度资源使用日志、etl任务sql代码
"""

import pandas as pd
import numpy as np
import json
import os
import re
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
META_DIR   = os.path.join(BASE_DIR, 'meta_data')
EXPORT_DIR = os.path.join(BASE_DIR, 'export_data')


# ─────────────────────────────────────────────
# 1. 数据加载
# ─────────────────────────────────────────────
def load_data():
    print("[1/6] 加载数据...")
    df_meta    = pd.read_excel(os.path.join(META_DIR, '作业信息.xlsx'))
    df_history = pd.read_excel(os.path.join(META_DIR, '调度历史日志.xlsx'))
    df_res     = pd.read_excel(os.path.join(META_DIR, 'hive调度资源使用日志.xlsx'))
    df_sql     = pd.read_excel(os.path.join(META_DIR, 'etl 任务 sql 代码.xlsx'))
    for df in [df_meta, df_history, df_res, df_sql]:
        df.columns = df.columns.str.strip()
    print(f"  作业信息: {len(df_meta)} 行  历史日志: {len(df_history)} 行  资源日志: {len(df_res)} 行  SQL代码: {len(df_sql)} 行")
    return df_meta, df_history, df_res, df_sql


# ─────────────────────────────────────────────
# 2. 作业名解析 → db_name / layer
# ─────────────────────────────────────────────
def parse_job_name(job_name):
    """返回 (db_name, table_name, layer) 或 None"""
    if not isinstance(job_name, str):
        return None
    jn = job_name.strip()
    if not (jn.startswith('etl') or jn.startswith('tp')):
        return None
    parts = jn.split('-')
    if len(parts) < 4:
        return None
    table_name = parts[-1]
    db_name    = parts[-2]
    # 库名含 ads → 统一归 ads 层
    if 'ads' in db_name:
        layer = 'ads'
    else:
        first = table_name.split('_')[0] if table_name else ''
        valid = {'ods', 'dts', 'dwd', 'dim', 'tms', 'ads'}
        layer = first if first in valid else first  # 保留原始前缀
    return (db_name, table_name, layer)


def enrich(df, col='job_name'):
    parsed = df[col].apply(parse_job_name)
    mask   = parsed.notna()
    df     = df[mask].copy()
    df['db_name']    = [x[0] for x in parsed[mask]]
    df['table_name'] = [x[1] for x in parsed[mask]]
    df['layer']      = [x[2] for x in parsed[mask]]
    # 排除 _rt 结尾的库
    df = df[~df['db_name'].str.endswith('_rt', na=False)].copy()
    return df


# ─────────────────────────────────────────────
# 3. 基础统计
# ─────────────────────────────────────────────
def basic_stats(df_meta, df_history):
    print("[2/6] 基础统计...")

    # --- 作业信息维度 ---
    meta = df_meta[df_meta['name'].apply(lambda x: isinstance(x, str) and (x.startswith('etl') or x.startswith('tp')))].copy()
    total_jobs = len(meta)
    type_dist  = meta['job_type'].value_counts().to_dict()

    # 解析 db/layer
    parsed = meta['name'].apply(parse_job_name)
    meta = meta[parsed.notna()].copy()
    meta['db_name'] = [x[0] for x in parsed[parsed.notna()]]
    meta['layer']   = [x[2] for x in parsed[parsed.notna()]]
    meta['job_type_clean'] = meta['job_type'].str.upper()
    # 排除 _rt 结尾的库
    meta = meta[~meta['db_name'].str.endswith('_rt', na=False)].copy()

    db_dist    = meta['db_name'].value_counts().head(20).to_dict()
    layer_dist = meta['layer'].value_counts().to_dict()

    # 依赖情况
    has_dep = meta['relied_jobs'].apply(lambda x: isinstance(x, str) and x not in ('[]', '', 'nan') and len(str(x)) > 2)
    dep_rate = round(has_dep.sum() / len(meta) * 100, 1)

    # cron 触发时间分布
    def parse_cron_hour(cron):
        try:
            parts = str(cron).split()
            if len(parts) >= 3:
                h = int(parts[2])
                if 0 <= h <= 23:
                    return h
        except:
            pass
        return None
    meta['cron_hour'] = meta['cron'].apply(parse_cron_hour)
    cron_hour_dist = meta['cron_hour'].dropna().astype(int).value_counts().sort_index().to_dict()

    # 重试配置
    retry_gt0 = (meta['retry_time'] > 0).sum()
    self_rely  = (meta['self_rely'] == 1).sum()

    # --- 历史日志维度 ---
    hist = df_history.copy()
    hist['end_time']   = pd.to_datetime(hist['end_time'],   errors='coerce')
    hist['start_time'] = pd.to_datetime(hist['start_time'], errors='coerce')
    hist['fire_time']  = pd.to_datetime(hist['fire_time'],  errors='coerce')

    # 成功率
    status_dist = hist['status'].value_counts().to_dict()

    # 执行时长（分钟）
    hist['duration_min'] = (hist['end_time'] - hist['start_time']).dt.total_seconds() / 60
    # 排队等待（fire→start，分钟）
    hist['queue_min'] = (hist['start_time'] - hist['fire_time']).dt.total_seconds() / 60

    # 过滤 AUTO + SUCCESSFUL
    succ = hist[(hist['status'] == 'SUCCESSFUL') & (hist['execute_type'] == 'AUTO')].copy()
    succ = enrich(succ, 'job_name')
    succ['date'] = succ['end_time'].dt.date.astype(str)

    return {
        'total_jobs': total_jobs,
        'type_dist': type_dist,
        'db_dist': db_dist,
        'layer_dist': layer_dist,
        'dep_rate': dep_rate,
        'cron_hour_dist': cron_hour_dist,
        'retry_gt0': int(retry_gt0),
        'self_rely': int(self_rely),
        'status_dist': status_dist,
        'meta': meta,
        'succ': succ,
        'hist': hist,
    }


# ─────────────────────────────────────────────
# 4. 小时完成率（1~10 点）
# ─────────────────────────────────────────────
def hourly_progress(succ):
    print("[3/6] 计算小时完成率...")
    hours = list(range(1, 11))
    results = []
    for (date, db, layer), grp in succ.groupby(['date', 'db_name', 'layer']):
        total = len(grp)
        if total == 0:
            continue
        base = pd.Timestamp(date)
        row = {'date': date, 'db_name': db, 'layer': layer, 'total': total}
        for h in hours:
            cut = base + pd.Timedelta(hours=h)
            row[f'h{h}'] = round(len(grp[grp['end_time'] <= cut]) / total * 100, 2)
        results.append(row)
    df_r = pd.DataFrame(results)
    stats = df_r.groupby(['db_name', 'layer'])[[f'h{h}' for h in hours]].mean().reset_index()
    avg_cnt = df_r.groupby(['db_name', 'layer'])['total'].mean().reset_index(name='avg_jobs')
    stats = stats.merge(avg_cnt, on=['db_name', 'layer'])
    for h in hours:
        stats[f'h{h}'] = stats[f'h{h}'].round(2)
    stats['avg_jobs'] = stats['avg_jobs'].round(1)
    return stats


# ─────────────────────────────────────────────
# 5. 资源分析
# ─────────────────────────────────────────────
def resource_analysis(df_res):
    print("[4/6] 资源分析...")
    df = df_res.copy()
    df = enrich(df, 'job_name')
    df['elapsed_min'] = pd.to_numeric(df['elapsed_time'], errors='coerce') / 60
    df['mem_gb_h']    = pd.to_numeric(df['memory_seconds'], errors='coerce') / 3600 / 1024  # GB·h
    df['vcore_h']     = pd.to_numeric(df['vcore_seconds'], errors='coerce') / 3600

    # 队列分布
    queue_dist = df['queue'].value_counts().to_dict()

    # 按作业汇总耗时
    job_dur = (df.groupby('job_name')
               .agg(avg_elapsed_min=('elapsed_min','mean'),
                    p90_elapsed_min=('elapsed_min', lambda x: np.nanpercentile(x.dropna(), 90) if len(x.dropna()) > 0 else np.nan),
                    total_mem_gb_h=('mem_gb_h','sum'),
                    total_vcore_h=('vcore_h','sum'),
                    run_cnt=('job_name','count'))
               .reset_index()
               .dropna(subset=['avg_elapsed_min']))
    job_dur['avg_elapsed_min'] = job_dur['avg_elapsed_min'].round(1)
    job_dur['p90_elapsed_min'] = job_dur['p90_elapsed_min'].round(1)
    job_dur['total_mem_gb_h']  = job_dur['total_mem_gb_h'].round(1)
    job_dur['total_vcore_h']   = job_dur['total_vcore_h'].round(1)

    # 添加 db/layer
    parsed = job_dur['job_name'].apply(parse_job_name)
    job_dur['db_name'] = [x[0] if x else 'unknown' for x in parsed]
    job_dur['layer']   = [x[2] if x else 'unknown' for x in parsed]

    top_dur  = job_dur.nlargest(20, 'avg_elapsed_min')[['job_name','db_name','layer','avg_elapsed_min','p90_elapsed_min','run_cnt']].to_dict('records')
    top_mem  = job_dur.nlargest(20, 'total_mem_gb_h')[['job_name','db_name','layer','total_mem_gb_h','total_vcore_h','run_cnt']].to_dict('records')

    # 按库汇总资源消耗
    db_res = (df.groupby('db_name')
              .agg(total_mem_gb_h=('mem_gb_h','sum'), total_vcore_h=('vcore_h','sum'))
              .reset_index()
              .sort_values('total_mem_gb_h', ascending=False)
              .head(20))
    db_res['total_mem_gb_h'] = db_res['total_mem_gb_h'].round(0)
    db_res['total_vcore_h']  = db_res['total_vcore_h'].round(0)

    # 按小时统计并发作业数（fire_time 小时）
    df['fire_hour'] = pd.to_datetime(df['fire_time'], errors='coerce').dt.hour
    hourly_concurrent = df.groupby('fire_hour').size().to_dict()

    return {
        'queue_dist': queue_dist,
        'top_dur': top_dur,
        'top_mem': top_mem,
        'db_res': db_res.to_dict('records'),
        'hourly_concurrent': {int(k): int(v) for k, v in hourly_concurrent.items() if pd.notna(k)},
    }


# ─────────────────────────────────────────────
# 6. 调度配置 Gap 分析（上游完成 → 本作业启动）
# ─────────────────────────────────────────────
def gap_analysis(df_meta, succ):
    print("[5/6] Gap 优化分析...")

    # 构建 job_id → name/cron/owner 映射
    meta_idx = df_meta.set_index('id')
    id2name  = meta_idx['name'].to_dict()
    id2cron  = meta_idx['cron'].to_dict()
    id2owner = meta_idx['owner'].to_dict()

    # 解析 relied_jobs
    def parse_deps(v):
        if pd.isna(v) or str(v).strip() in ('[]', '', 'nan'):
            return []
        try:
            return [int(x) for x in re.findall(r'\d+', str(v))]
        except:
            return []

    # 每日每作业首次执行记录（只保留AUTO SUCCESSFUL）
    daily = (succ.sort_values('start_time')
                 .groupby(['date', 'job_id'])
                 .first()
                 .reset_index())
    lookup = {}
    for _, row in daily.iterrows():
        lookup[(row['date'], row['job_id'])] = {'start': row['start_time'], 'end': row['end_time']}

    def time_to_mins(dt):
        return dt.hour * 60 + dt.minute

    def mins_to_time(m):
        h = int(m // 60) % 24
        mi = int(m % 60)
        return f"{h:02d}:{mi:02d}"

    suggestions = []
    for _, row in df_meta.iterrows():
        jid   = row['id']
        deps  = parse_deps(row['relied_jobs'])
        if not deps:
            continue
        jname = row['name']
        if not isinstance(jname, str) or not (jname.startswith('etl') or jname.startswith('tp')):
            continue

        runs = daily[daily['job_id'] == jid]
        if runs.empty:
            continue

        gaps, parent_ends = [], []
        for _, run in runs.iterrows():
            date = run['date']
            actual_start = run['start_time']
            max_p_end = None
            for pid in deps:
                if (date, pid) in lookup:
                    pe = lookup[(date, pid)]['end']
                    max_p_end = pe if max_p_end is None else max(max_p_end, pe)
            if max_p_end is not None:
                gap = (actual_start - max_p_end).total_seconds() / 60
                gaps.append(gap)
                parent_ends.append(max_p_end)

        if not gaps:
            continue
        min_gap = min(gaps)
        avg_gap = sum(gaps) / len(gaps)
        if min_gap <= 60:
            continue

        safe_end_min = max(time_to_mins(t) for t in parent_ends)
        sug_min = safe_end_min + 15

        cron = row.get('cron', '')
        cfg_start, cfg_mins = 'Unknown', 9999
        try:
            cp = str(cron).split()
            if len(cp) >= 3:
                cfg_mins   = int(cp[2]) * 60 + int(cp[1])
                cfg_start  = f"{int(cp[2]):02d}:{int(cp[1]):02d}"
        except:
            pass

        if sug_min >= cfg_mins:
            continue

        parsed = parse_job_name(jname)
        suggestions.append({
            '作业名': jname,
            '负责人': id2owner.get(jid, ''),
            '库名': parsed[0] if parsed else '',
            '分层': parsed[2] if parsed else '',
            '当前配置': cfg_start,
            '建议时间': mins_to_time(sug_min),
            '平均空闲(分)': round(avg_gap, 1),
            '最小空闲(分)': round(min_gap, 1),
        })

    df_sug = pd.DataFrame(suggestions).sort_values('最小空闲(分)', ascending=False)
    return df_sug


# ─────────────────────────────────────────────
# 7. Fire→Start 排队等待分析
# ─────────────────────────────────────────────
def queue_wait_analysis(hist):
    hist2 = hist.copy()
    hist2['fire_hour'] = hist2['fire_time'].dt.hour
    hist2['queue_min_clean'] = hist2['queue_min'].clip(lower=0)
    g = hist2.groupby('fire_hour')['queue_min_clean'].agg(['mean','median',
        lambda x: np.nanpercentile(x.dropna(), 90) if len(x.dropna()) > 0 else np.nan]).reset_index()
    g.columns = ['hour','avg_queue','median_queue','p90_queue']
    g = g.dropna(subset=['hour']).copy()
    g['hour'] = g['hour'].astype(int)
    return g.to_dict('records')


# ─────────────────────────────────────────────
# 7.0  ads 库"中间层"作业分析（ads 库但被下游大量依赖）
# ─────────────────────────────────────────────
def ads_pivot_analysis(df_meta, df_res):
    """
    找出 db_name 含 'ads' 但被其他作业大量依赖的"伪中间层"作业。
    返回:
      pivot_list  - list[dict] 完整列表（downstream_cnt >= 1，含资源）
      by_db       - list[dict] 按 db_name 聚合（pivot 数量 + 总下游数）
      risk_matrix - list[dict] 用于散点图：每个 pivot 作业的 (avg_dur, downstream_cnt)
    """
    import re as _re

    df = df_meta.copy()

    def parse_db(name):
        if not isinstance(name, str): return None
        if not (name.startswith('etl') or name.startswith('tp')): return None
        parts = name.split('-')
        return parts[-2] if len(parts) >= 4 else None

    df['db_name'] = df['name'].apply(parse_db)
    df = df[df['db_name'].notna() & ~df['db_name'].str.endswith('_rt', na=False)]

    def parse_deps(v):
        if pd.isna(v) or str(v).strip() in ('[]', '', 'nan'): return []
        try: return [int(x) for x in _re.findall(r'\d+', str(v))]
        except: return []

    # 反向统计：每个 job_id 有多少下游作业依赖它
    downstream_count = {}
    downstream_names = {}  # job_id -> list of downstream job names
    id2name = df.set_index('id')['name'].to_dict()

    for _, row in df.iterrows():
        deps = parse_deps(row['relied_jobs'])
        for pid in deps:
            downstream_count[pid] = downstream_count.get(pid, 0) + 1
            if pid not in downstream_names:
                downstream_names[pid] = []
            if len(downstream_names[pid]) < 8:
                downstream_names[pid].append(row['name'])

    df['downstream_cnt']   = df['id'].map(downstream_count).fillna(0).astype(int)
    df['downstream_names'] = df['id'].map(downstream_names).apply(lambda x: x if isinstance(x, list) else [])

    # 资源信息
    df_r = df_res.copy()
    df_r['elapsed_min'] = pd.to_numeric(df_r['elapsed_time'], errors='coerce') / 60
    df_r['mem_gb_h']    = pd.to_numeric(df_r['memory_seconds'], errors='coerce') / 3600 / 1024
    res_agg = df_r.groupby('job_name').agg(
        avg_dur=('elapsed_min', 'mean'),
        total_mem=('mem_gb_h', 'sum'),
    ).reset_index().rename(columns={'job_name': 'name'})

    df = df.merge(res_agg, on='name', how='left')
    df['avg_dur']   = df['avg_dur'].fillna(0).round(1)
    df['total_mem'] = df['total_mem'].fillna(0).round(1)

    # 解析 cron 为可读时间
    def cron_to_time(cron):
        try:
            p = str(cron).split()
            if len(p) >= 3:
                return '%02d:%02d' % (int(p[2]), int(p[1]))
        except:
            pass
        return '--:--'
    df['cron_time'] = df['cron'].apply(cron_to_time)

    # 风险等级
    def risk(cnt):
        if cnt >= 20: return '高危'
        if cnt >= 10: return '中危'
        if cnt >= 3:  return '低危'
        return '观察'
    df['risk'] = df['downstream_cnt'].apply(risk)

    # 筛选 ads 库且有下游依赖
    ads_pivot = df[
        df['db_name'].str.contains('ads', na=False) &
        (df['downstream_cnt'] >= 1)
    ].sort_values('downstream_cnt', ascending=False)

    # 完整列表（取 top200 供展示）
    pivot_list = ads_pivot[
        ['name', 'db_name', 'owner', 'cron_time', 'downstream_cnt', 'avg_dur', 'total_mem', 'risk', 'downstream_names']
    ].to_dict('records')

    # 按库聚合
    by_db = (ads_pivot[ads_pivot['downstream_cnt'] >= 3]
             .groupby('db_name')
             .agg(pivot_cnt=('name','count'), total_downstream=('downstream_cnt','sum'))
             .reset_index()
             .sort_values('total_downstream', ascending=False)
             .head(20)
             .to_dict('records'))

    # 散点图数据（downstream_cnt >= 3）
    risk_matrix = ads_pivot[ads_pivot['downstream_cnt'] >= 3].head(80)[[
        'name', 'db_name', 'avg_dur', 'downstream_cnt', 'risk'
    ]].to_dict('records')

    # 汇总数字
    summary = {
        'total_pivot':     int(len(ads_pivot[ads_pivot['downstream_cnt'] >= 1])),
        'high_risk':       int(len(ads_pivot[ads_pivot['downstream_cnt'] >= 20])),
        'mid_risk':        int(len(ads_pivot[(ads_pivot['downstream_cnt'] >= 10) & (ads_pivot['downstream_cnt'] < 20)])),
        'low_risk':        int(len(ads_pivot[(ads_pivot['downstream_cnt'] >= 3) & (ads_pivot['downstream_cnt'] < 10)])),
        'max_downstream':  int(ads_pivot['downstream_cnt'].max()),
        'top1_name':       str(ads_pivot.iloc[0]['name']) if len(ads_pivot) > 0 else '',
    }

    return pivot_list, by_db, risk_matrix, summary


# ─────────────────────────────────────────────
# 7.5  SQL 优化分析（高耗时作业，排除 dws_common）
# 耗时来源：df_history（dwd_sch_history_job_di），end_time - start_time
# 资源来源：df_res（dwd_sch_history_application_di），memory_seconds
# ─────────────────────────────────────────────
def sql_optimization_analysis(df_history, df_res, df_sql):
    """返回 list[dict]，每个元素对应一个作业的分析结果，包含原始SQL片段和优化建议。"""
    import re as _re

    def parse_db(name):
        if not isinstance(name, str): return None
        if not (name.startswith('etl') or name.startswith('tp')): return None
        parts = name.split('-')
        return parts[-2] if len(parts) >= 4 else None

    # ── 耗时：来自调度历史日志（job 级别，end_time - start_time）──
    df_h = df_history.copy()
    df_h['end_time']   = pd.to_datetime(df_h['end_time'],   errors='coerce')
    df_h['start_time'] = pd.to_datetime(df_h['start_time'], errors='coerce')
    df_h['duration_min'] = (df_h['end_time'] - df_h['start_time']).dt.total_seconds() / 60
    # 只保留 AUTO SUCCESSFUL，过滤异常耗时（<=0）
    df_h = df_h[(df_h['status'] == 'SUCCESSFUL') & (df_h['execute_type'] == 'AUTO')].copy()
    df_h = df_h[df_h['duration_min'] > 0]
    df_h['db_name'] = df_h['job_name'].apply(parse_db)
    df_h = df_h[df_h['db_name'].notna()]
    df_h = df_h[~df_h['db_name'].str.endswith('_rt', na=False)]
    df_h = df_h[df_h['db_name'] != 'dws_common']

    job_dur = df_h.groupby('job_name').agg(
        avg_dur=('duration_min', 'mean'),
        p90_dur=('duration_min', lambda x: np.nanpercentile(x.dropna(), 90) if len(x.dropna()) > 0 else np.nan),
        runs=('job_name', 'count'),
        db_name=('db_name', 'first'),
    ).reset_index()

    # ── 资源：来自资源日志（application 级别，memory_seconds）──
    df_r = df_res.copy()
    df_r['mem_gb_h'] = pd.to_numeric(df_r['memory_seconds'], errors='coerce') / 3600 / 1024
    res_agg = df_r.groupby('job_name').agg(total_mem=('mem_gb_h', 'sum')).reset_index()

    job_agg = job_dur.merge(res_agg, on='job_name', how='left')
    job_agg['total_mem'] = job_agg['total_mem'].fillna(0).round(1)

    # 只取有 ETL SQL 的作业
    sql_names = set(df_sql['name'].tolist())
    job_agg = job_agg[job_agg['job_name'].isin(sql_names)]

    top10 = job_agg.sort_values('avg_dur', ascending=False).reset_index(drop=True)

    sql_map = df_sql.set_index('name')['code'].to_dict()

    # ── 每个作业的诊断规则 ──
    def diagnose(name, code):
        issues = []
        suggestions = []
        opt_sql = None

        code_upper = code.upper() if isinstance(code, str) else ''
        code_str   = code if isinstance(code, str) else ''

        # 1. HBase 外部表扫描
        if 'HBASE' in code_upper or 'external_dim_' in code_str.lower():
            issues.append('从 HBase 外部表全量扫描（无谓并发、无列过滤），单次执行动辄数百分钟')
            suggestions.append('将 HBase 扫描替换为增量同步：每日只同步 update_time > yesterday 的 rowkey，写入临时表后 MERGE 进分区表，避免全表扫描')
            suggestions.append('对 HBase 外部表开启 hbase.mapreduce.inputtable.snapshotname 快照读，减少实时 Region 竞争')
            opt_sql = (
                "-- 优化示例：仅扫描昨日有变更的 rowkey（需在 HBase 侧按 ts 列建二级索引或 Phoenix 视图）\n"
                "INSERT OVERWRITE TABLE {tbl} PARTITION(dt='${{etl_date}}')\n"
                "SELECT user_id, props_left\n"
                "FROM   {tbl}_external_snapshot\n"
                "WHERE  last_update_ts >= unix_timestamp(date_sub('${{etl_date}}',1))*1000\n"
                "  AND  last_update_ts <  unix_timestamp('${{etl_date}}')*1000;\n\n"
                "-- 若无法增量，改用 HBase Snapshot + BulkLoad 方式，可将扫描时间缩短 60%+"
            ).format(tbl=name.split('etl-hive-')[-1].replace('-', '.', 1))

        # 2. 无界分区扫描（dt >= 历史固定日期，不含上界）
        if _re.search(r"dt\s*>=\s*'20\d{2}-\d{2}-\d{2}'", code_str) and \
           not _re.search(r"dt\s*<=|dt\s*=\s*'\$\{etl_date\}'", code_str):
            issues.append("WHERE 条件含 dt >= '历史固定日期' 但无上界，每次执行均全量扫描历史所有分区，数据量随时间线性增长")
            suggestions.append("将开放式 dt 范围收紧为 dt = '${etl_date}'（增量模式），或改为 dt BETWEEN date_sub('${etl_date}', N) AND '${etl_date}'")
            opt_sql = (
                "-- 原始（危险）：无上界扫描\n"
                "-- WHERE dt >= '2024-07-01'\n\n"
                "-- 优化：精确到当日分区（增量写入）\n"
                "WHERE dt = '${{etl_date}}'\n"
                "  AND is_active = 1\n\n"
                "-- 如需累积结果，改为先写当日增量再与历史 UNION ALL，避免读取全量"
            )

        # 3. 固定 reduce 任务数过少
        if _re.search(r'mapred\.reduce\.tasks\s*=\s*[1-9]\d?\b', code_str):
            m = _re.search(r'mapred\.reduce\.tasks\s*=\s*(\d+)', code_str)
            n = int(m.group(1)) if m else 0
            if n <= 50:
                issues.append('reduce 任务数固定为 %d，对大数据量聚合严重不足，导致单 reduce 处理数据过多、OOM 或执行缓慢' % n)
                suggestions.append('删除硬编码的 mapred.reduce.tasks，改用动态配置：SET hive.exec.reducers.bytes.per.reducer=256MB 让 Hive 自动决定并行度')
                opt_sql = (
                    "-- 删除以下固定配置：\n"
                    "-- SET mapred.reduce.tasks = %d;\n\n"
                    "-- 改为动态 Reducer 数量（推荐）：\n"
                    "SET hive.exec.reducers.bytes.per.reducer = 268435456;  -- 256MB/reducer\n"
                    "SET hive.exec.reducers.max = 1009;                     -- 上限防止过多小文件\n"
                    "-- Hive 将根据数据量自动计算合理的 reducer 数，避免人工猜测"
                ) % n

        # 4. CTE 被多次引用自连接（retention 类模式）
        if code_str.count('tbl15') >= 3 or (code_str.count('LEFT JOIN') >= 2 and 'retain' in name.lower()):
            issues.append('同一 CTE（tbl15）被 3 次引用做自连接（d0/d1/d7 留存），Hive 会重复执行 3 次相同的大表扫描，计算量是最优方案的 3 倍')
            suggestions.append('改用窗口函数 LEAD/LAG 或条件聚合在单次扫描内计算多日留存，消除重复大表扫描')
            opt_sql = (
                "-- 原始：CTE tbl15 被 LEFT JOIN 3 次，扫描 3 遍大表\n\n"
                "-- 优化：单次扫描，用条件聚合计算留存\n"
                "WITH base AS (\n"
                "    SELECT anchor_uid, user_uid, dt\n"
                "    FROM   tbl15  -- 只扫描一次\n"
                "),\n"
                "retention AS (\n"
                "    SELECT\n"
                "        a.anchor_uid,\n"
                "        a.user_uid                                        AS d0_users,\n"
                "        MAX(CASE WHEN b.dt = date_add(a.dt,1) THEN 1 END) AS d1_flag,\n"
                "        MAX(CASE WHEN b.dt = date_add(a.dt,6) THEN 1 END) AS d7_flag,\n"
                "        a.dt\n"
                "    FROM  base a\n"
                "    LEFT JOIN base b\n"
                "        ON  b.anchor_uid = a.anchor_uid\n"
                "        AND b.user_uid   = a.user_uid\n"
                "        AND b.dt IN (date_add(a.dt,1), date_add(a.dt,6))  -- 一次 JOIN 覆盖 d1+d7\n"
                "    GROUP BY a.anchor_uid, a.user_uid, a.dt\n"
                ")\n"
                "INSERT OVERWRITE TABLE ... PARTITION(dt)\n"
                "SELECT anchor_uid,\n"
                "       d0_users,\n"
                "       CASE WHEN d1_flag=1 THEN user_uid END AS d1_retained,\n"
                "       CASE WHEN d7_flag=1 THEN user_uid END AS d7_retained,\n"
                "       dt\n"
                "FROM   retention;"
            )

        # 5. 动态分区+日期计算下推到 SELECT（滑动窗口聚合）
        if 'next_day' in code_str.lower() and 'floor(datediff' in code_str.lower():
            issues.append('在 SELECT 阶段对每行执行 FLOOR(datediff)/next_day 等复杂日期计算作为动态分区键，触发大量小分区写入，造成文件碎片和 Metastore 压力')
            suggestions.append('预先计算分区键，用 GROUP BY 先聚合再写入；或将滑动窗口改为按 dt=当天逐日写入，在查询层用 date_trunc 聚合')
            opt_sql = (
                "-- 原始：在 SELECT 中动态计算周分区键（每行都触发 datediff + next_day）\n"
                "-- ,next_day(date_sub(dt,DATEDIFF(dt,reg_date)%7),'mo') dt\n\n"
                "-- 优化：先用 CTE 计算好分区键，然后 GROUP BY 聚合后写入\n"
                "WITH enriched AS (\n"
                "    SELECT *,\n"
                "           next_day(date_sub(dt, DATEDIFF(dt,reg_date)%7),'mo') AS week_start_dt\n"
                "    FROM   source_table\n"
                "),\n"
                "aggregated AS (\n"
                "    SELECT uid, week_start_dt,\n"
                "           SUM(all_ml_money)      AS all_ml_money,\n"
                "           SUM(all_consume_money) AS all_consume_money\n"
                "           -- ...其他聚合列\n"
                "    FROM   enriched\n"
                "    GROUP BY uid, week_start_dt\n"
                ")\n"
                "INSERT OVERWRITE TABLE target PARTITION(dt)\n"
                "SELECT *, week_start_dt AS dt FROM aggregated;\n"
                "-- 分区数量从 O(行数) 降到 O(uid×周数)，减少 HDFS 小文件"
            )

        # 6. 范围时间 JOIN（进房时间段与开播时间段 BETWEEN）
        if ('unix_timestamp' in code_str.lower() and 'between' in code_str.upper()
                and 'inout_room' in code_str.lower()):
            issues.append('进厅日志与开播时段进行时间范围 JOIN（start_time <= event_time <= end_time），Hive 对不等式 JOIN 无法使用 HashJoin，退化为 Cross Join + 过滤，数据量爆炸')
            suggestions.append('改用广播主播时段表（较小），在 Map 端做 UDF 或用 Spark SQL 的 rangeJoin；或将连续时间段离散化为分钟级粒度后做等值 JOIN')
            opt_sql = (
                "-- 原始：时间范围不等式 JOIN（Hive 退化为 Cross Join）\n"
                "-- ON a.room_id = c.room_id AND\n"
                "--    UNIX_TIMESTAMP(a.start_time,...) >= c.start_time AND\n"
                "--    UNIX_TIMESTAMP(a.end_time,...) <= c.end_time\n\n"
                "-- 优化方案1：将主播开播时段按分钟展开成等值 JOIN 键\n"
                "WITH anchor_minutes AS (\n"
                "    SELECT room_id, uid AS anchor_uid,\n"
                "           minute_ts,  -- 用 posexplode 按分钟展开\n"
                "           dt\n"
                "    FROM   tbl1\n"
                "    LATERAL VIEW posexplode(\n"
                "        split(space(CAST((end_time-start_time)/60 AS INT)),' ')\n"
                "    ) t AS pos, val\n"
                "    WHERE (start_time + pos*60) < end_time\n"
                ")\n"
                "SELECT CAST(a.uid AS BIGINT) AS user_uid,\n"
                "       a.room_id,\n"
                "       m.anchor_uid,\n"
                "       a.dt\n"
                "FROM   dws_oversea.dwd_log_inout_room_di a\n"
                "-- 等值 JOIN（分钟对齐后可走 HashJoin）\n"
                "JOIN   anchor_minutes m\n"
                "    ON a.room_id = m.room_id\n"
                "    AND floor(UNIX_TIMESTAMP(a.start_time,...)/60) = m.minute_ts\n"
                "    AND a.uid <> m.anchor_uid\n"
                "WHERE  a.dt BETWEEN date_sub('${{etl_date}}',6) AND '${{etl_date}}'\n"
                "  AND  a.duration > 0;"
            )

        # 7. ODS binlog 全量落盘（无列裁剪）
        if 'ods_log_binary' in name.lower() and 'extra_tags_map' in code_str:
            issues.append('从 Kafka 小时流表（_hi）落盘到日分区表，字段 extra_tags_map[key] 访问 Map 类型列代价高，且所有字段全量写出，无列裁剪')
            suggestions.append('对 Map 列使用 lateral view explode 预先展开常用 key，减少运行时 Map 解析；对不常查询的字段做冷热分离，热字段单独列，冷字段保留 map')
            suggestions.append('确认 ods_log_binary_oversea_hi 的小时分区已经用 ORC + ZSTD 压缩，若仍是 Text/Parquet 则压缩率差导致 IO 成本高')
            opt_sql = (
                "-- 原始：直接访问 map 列（每行都要解析整个 map）\n"
                "-- cast(extra_tags_map['kafka_timestamp'] AS BIGINT) AS kafka_timestamp,\n\n"
                "-- 优化：将高频访问的 map key 提前物化为普通列（在 _hi 层）\n"
                "-- 若 _hi 层已有物化列则直接引用，否则在本任务加 hint：\n"
                "SET hive.vectorized.execution.enabled=true;\n"
                "SET hive.vectorized.execution.reduce.enabled=true;\n"
                "SET orc.stripe.size=67108864;  -- 减小 ORC stripe，加快随机读\n\n"
                "INSERT OVERWRITE TABLE dws_veeka.ods_log_binary_oversea_di\n"
                "    PARTITION(dt, db, tb)\n"
                "SELECT\n"
                "    /*+ VECTORIZED */          -- 启用向量化执行\n"
                "    CAST(kafka_timestamp AS BIGINT),  -- 已从 _hi 物化\n"
                "    kafka_topic,\n"
                "    CAST(kafka_partition AS INT),\n"
                "    CAST(kafka_offset AS BIGINT),\n"
                "    record_id, source, record_type, source_timestamp,\n"
                "    extra_tags_map, fields_array, before_image_array, after_image_array,\n"
                "    dt, database_name AS db, table_name AS tb\n"
                "FROM dws_veeka.ods_log_binary_oversea_hi\n"
                "WHERE dt = '${{etl_date}}';"
            )

        # 8. 多表 LEFT JOIN 维表未 MAPJOIN
        if code_str.count('LEFT JOIN') >= 4 and '/*+ mapjoin' not in code_str.lower():
            issues.append('存在 %d 个 LEFT JOIN 但未使用 MAPJOIN Hint，Hive 对小维表仍走 ReduceSide Join，增加网络 Shuffle 开销' % code_str.count('LEFT JOIN'))
            suggestions.append('对所有小维表（< 128MB）添加 /*+ MAPJOIN(b,c,d,e,f) */ Hint，强制 Map 端广播，消除 Shuffle')
            opt_sql = (
                "-- 原始：无 MAPJOIN hint，全走 Reduce Side Join\n"
                "-- SELECT ... FROM (...) a LEFT JOIN (...) b ON ...\n\n"
                "-- 优化：对小维表显式指定 MAPJOIN\n"
                "SET hive.auto.convert.join.noconditionaltask.size = 268435456;  -- 256MB\n\n"
                "INSERT OVERWRITE TABLE dws_oversea.ads_mkt_dim_room_da PARTITION(dt='${{etl_date}}')\n"
                "SELECT /*+ MAPJOIN(b, c, d, e, f) */  -- 广播所有小维表\n"
                "    a.room_id, a.property, ...\n"
                "    coalesce(b.name,'unknow')          AS settlement_channel_name,\n"
                "    coalesce(c.has_broker_state, 0)    AS has_broker_state,\n"
                "    coalesce(d.broker_name,'unknow')   AS broker_name,\n"
                "    coalesce(e.factory_name,'unknow')  AS room_factory_name,\n"
                "    coalesce(f.show,'unknow'),\n"
                "    coalesce(f.label,'unknow')\n"
                "FROM (SELECT ... FROM dws_oversea.dim_chatroom_da WHERE dt='${{etl_date}}') a\n"
                "LEFT JOIN (SELECT type,name FROM dws_oversea.dts_anu_settlement_channel_df WHERE dt='${{etl_date}}') b ON ...\n"
                "LEFT JOIN (SELECT bid,rid,dateline,1 AS has_broker_state FROM dws_oversea.dts_anu_broker_chatroom_df WHERE dt='${{etl_date}}' AND bid>0) c ON ...\n"
                "LEFT JOIN (SELECT broker_id,broker_name,... FROM dws_oversea.dts_anu_broker_df WHERE dt='${{etl_date}}') d ON ...\n"
                "LEFT JOIN (SELECT factory_type,factory_name FROM dws_oversea.dts_anu_chatroom_module_factory_df WHERE dt='${{etl_date}}') e ON ...\n"
                "LEFT JOIN (SELECT id,show,label FROM dws_oversea.dts_anu_chatroom_module_tag_df WHERE dt='${{etl_date}}') f ON ...;"
            )

        return issues, suggestions, opt_sql

    results = []
    for _, row in top10.iterrows():
        name = row['job_name']
        code = sql_map.get(name, '')
        issues, suggestions, opt_sql = diagnose(name, code)
        # 截取 SQL 片段（去掉 SET/注释，保留核心逻辑，最多 800 字符）
        core_lines = [l for l in code.split('\n')
                      if l.strip() and not l.strip().startswith('--')
                      and not l.strip().upper().startswith('SET ')
                      and not l.strip().upper().startswith('/*')]
        snippet = '\n'.join(core_lines[:40])[:900]

        results.append({
            'job_name':   name,
            'db_name':    row['db_name'],
            'avg_dur':    round(float(row['avg_dur']), 1),
            'p90_dur':    round(float(row['p90_dur']), 1) if pd.notna(row['p90_dur']) else 0,
            'total_mem':  round(float(row['total_mem']), 1),
            'runs':       int(row['runs']),
            'issues':     issues,
            'suggestions': suggestions,
            'opt_sql':    opt_sql or '-- 通用建议：开启向量化执行 + 动态 reducer 数\nSET hive.vectorized.execution.enabled=true;\nSET hive.exec.reducers.bytes.per.reducer=268435456;',
            'snippet':    snippet,
        })

    return results


# ─────────────────────────────────────────────
# 8. 生成 HTML  (使用占位符替换，避免 f-string 与 JS 语法冲突)
# ─────────────────────────────────────────────
def _tpl():
    """返回 HTML 模板字符串，用 __KEY__ 标记所有动态插值位置。"""
    return r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>调度作业全面瓶颈分析报告</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<link href="https://cdn.datatables.net/1.13.8/css/dataTables.bootstrap5.min.css" rel="stylesheet">
<style>
:root{--c1:#1a73e8;--c2:#34a853;--c3:#fbbc04;--c4:#ea4335;--c5:#9c27b0;--bg:#f0f4f9;--card:#fff;--radius:12px;--nav-h:72px;}
html{scroll-padding-top:var(--nav-h);}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);font-family:'PingFang SC','Helvetica Neue',Arial,sans-serif;font-size:14px;color:#333;}
.navbar{background:linear-gradient(135deg,#1a73e8,#0d47a1);padding:16px 32px;color:#fff;position:sticky;top:0;z-index:999;display:flex;align-items:center;gap:16px;box-shadow:0 2px 8px rgba(0,0,0,.2);}
.navbar h1{font-size:1.4rem;font-weight:700;margin:0;}
.navbar .sub{font-size:.8rem;opacity:.8;margin-top:2px;}
.nav-pills a{color:#fff;text-decoration:none;padding:6px 14px;border-radius:20px;font-size:.85rem;transition:.2s;}
.nav-pills a:hover{background:rgba(255,255,255,.2);}
.content{max-width:1600px;margin:24px auto;padding:0 20px;}
.section{background:var(--card);border-radius:var(--radius);box-shadow:0 2px 12px rgba(0,0,0,.08);margin-bottom:28px;overflow:hidden;scroll-margin-top:var(--nav-h);}
.section-header{background:linear-gradient(90deg,#1a73e8,#1557b0);color:#fff;padding:16px 24px;display:flex;align-items:center;gap:10px;}
.section-header h2{font-size:1.1rem;font-weight:600;margin:0;}
.badge-num{background:rgba(255,255,255,.3);border-radius:12px;padding:2px 10px;font-size:.8rem;}
.section-body{padding:20px 24px;}
.kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:16px;margin-bottom:4px;}
.kpi-card{background:linear-gradient(135deg,#e8f0fe,#d2e3fc);border-radius:10px;padding:16px;text-align:center;}
.kpi-card.warn{background:linear-gradient(135deg,#fff8e1,#ffecb3);}
.kpi-card.danger{background:linear-gradient(135deg,#fce8e6,#f5c6c3);}
.kpi-card.ok{background:linear-gradient(135deg,#e6f4ea,#c8e6c9);}
.kpi-val{font-size:2rem;font-weight:700;color:var(--c1);}
.kpi-card.warn .kpi-val{color:#f57c00;}
.kpi-card.danger .kpi-val{color:var(--c4);}
.kpi-card.ok .kpi-val{color:var(--c2);}
.kpi-label{font-size:.78rem;color:#666;margin-top:4px;}
.chart-row{display:grid;gap:16px;}
.chart-row.cols-2{grid-template-columns:1fr 1fr;}
.chart-row.cols-3{grid-template-columns:1fr 1fr 1fr;}
.chart-box{background:#f8f9fa;border-radius:8px;padding:12px;}
.chart-title{font-size:.85rem;font-weight:600;color:#444;margin-bottom:8px;}
.chart{width:100%;}
.h300{height:300px;}.h350{height:350px;}.h400{height:400px;}.h450{height:450px;}.h500{height:500px;}
.ctrl-row{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px;}
.ctrl-row label{font-weight:600;font-size:.85rem;}
select.form-select{width:auto;font-size:.85rem;}
table.dataTable thead th{background:#1a73e8;color:#fff;font-size:.82rem;}
table.dataTable tbody td{font-size:.8rem;}
.badge-layer{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72rem;font-weight:600;}
.l-ods{background:#e3f2fd;color:#1565c0;}.l-dts{background:#f3e5f5;color:#6a1b9a;}
.l-dwd{background:#e8f5e9;color:#2e7d32;}.l-dim{background:#fff3e0;color:#e65100;}
.l-tms{background:#fce4ec;color:#880e4f;}.l-ads{background:#ffebee;color:#c62828;}
.l-other{background:#f5f5f5;color:#555;}
.sla-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:8px;margin-top:8px;}
.sla-card{border-radius:8px;padding:10px 14px;font-size:.8rem;display:flex;justify-content:space-between;align-items:center;}
.sla-fail{background:#fff0f0;border-left:4px solid #e53935;}
.sla-ok{background:#f0fff4;border-left:4px solid #43a047;}
.outline-list{counter-reset:section;list-style:none;padding:0;}
.outline-list li{counter-increment:section;padding:8px 0;border-bottom:1px solid #f0f0f0;display:flex;align-items:center;gap:8px;}
.outline-list li::before{content:counter(section);background:#1a73e8;color:#fff;border-radius:50%;width:24px;height:24px;display:inline-flex;align-items:center;justify-content:center;font-size:.75rem;font-weight:700;flex-shrink:0;}
.insight-block{background:#e8f0fe;border-left:4px solid #1a73e8;border-radius:0 8px 8px 0;padding:12px 16px;margin:10px 0;font-size:.85rem;line-height:1.8;}
.insight-block.warn{background:#fff8e1;border-color:#fb8c00;}
.opt-tag{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72rem;font-weight:600;background:#fff3cd;color:#856404;margin-right:4px;}
.footer{text-align:center;color:#999;font-size:.75rem;padding:20px;}
</style>
</head>
<body>
<div class="navbar">
  <div>
    <h1>&#128202; 调度作业全面瓶颈分析报告</h1>
    <div class="sub">数据范围：2026-03-01 ~ 2026-03-10 &nbsp;|&nbsp; 生成时间：__GEN_TIME__</div>
  </div>
  <nav class="nav-pills ms-auto d-flex gap-1 flex-wrap">
    <a href="#sec-outline">大纲</a><a href="#sec-overview">作业概览</a>
    <a href="#sec-progress">完成率</a><a href="#sec-resource">资源分析</a>
    <a href="#sec-queue">排队分析</a>    <a href="#sec-opt">优化建议</a>
    <a href="#sec-ads-pivot">ads伪中间层</a>
    <a href="#sec-sql">SQL优化</a>
    <a href="#sec-conclusion">结论</a>
  </nav>
</div>
<div class="content">

<!-- 大纲 -->
<div class="section" id="sec-outline">
  <div class="section-header"><h2>&#128203; 一、分析大纲</h2></div>
  <div class="section-body">
    <p style="color:#555;margin-bottom:14px">本报告基于作业信息、调度历史日志、Hive 资源使用日志、ETL SQL 代码四份数据，从以下维度全面诊断调度瓶颈：</p>
    <ol class="outline-list">
      <li><strong>作业概览</strong>：总量、类型分布、库/层分布、依赖情况、cron 触发时间分布</li>
      <li><strong>小时完成率趋势</strong>：各库各层 1~10 点完成进度，识别滞后层级</li>
      <li><strong>SLA 达标分析</strong>：以 9 点完成率 ≥90% 为目标，评估各库/层达标情况</li>
      <li><strong>Hive 资源分析</strong>：内存用量、计算核时、执行耗时 Top20、队列分布、每小时并发量</li>
      <li><strong>调度排队等待分析</strong>：fire_time → start_time 的排队时长分时分布，定位集群高峰拥塞段</li>
      <li><strong>调度配置 Gap 优化</strong>：上游已完成但 cron 未触发的空闲等待 &gt;60 分钟的作业，附建议调整时间</li>
      <li><strong>综合结论与行动计划</strong>：根因分类 + 分优先级行动</li>
    </ol>
  </div>
</div>

<!-- 作业概览 -->
<div class="section" id="sec-overview">
  <div class="section-header"><h2>&#128230; 二、作业概览</h2><span class="badge-num">etl + tp 作业范围</span></div>
  <div class="section-body">
    <div class="kpi-grid">
      <div class="kpi-card"><div class="kpi-val">__TOTAL_JOBS__</div><div class="kpi-label">活跃作业总数</div></div>
      <div class="kpi-card ok"><div class="kpi-val">__DEP_RATE__%</div><div class="kpi-label">有上游依赖比例</div></div>
      <div class="kpi-card warn"><div class="kpi-val">__RETRY_GT0__</div><div class="kpi-label">配置重试的作业数</div></div>
      <div class="kpi-card warn"><div class="kpi-val">__SELF_RELY__</div><div class="kpi-label">开启自依赖作业数</div></div>
      <div class="kpi-card ok"><div class="kpi-val">__SLA_OK__</div><div class="kpi-label">SLA 达标组合(≥90%@9点)</div></div>
      <div class="kpi-card danger"><div class="kpi-val">__SLA_FAIL_CNT__</div><div class="kpi-label">SLA 未达标组合(&lt;80%@10点)</div></div>
    </div>
    <div class="chart-row cols-3" style="margin-top:16px">
      <div class="chart-box"><div class="chart-title">作业类型分布</div><div id="c-type" class="chart h300"></div></div>
      <div class="chart-box"><div class="chart-title">数据层级分布（作业数）</div><div id="c-layer" class="chart h300"></div></div>
      <div class="chart-box"><div class="chart-title">Cron 触发时间分布（小时粒度）</div><div id="c-cron" class="chart h300"></div></div>
    </div>
    <div class="chart-row cols-2" style="margin-top:16px">
      <div class="chart-box"><div class="chart-title">Top 20 库：作业数量</div><div id="c-db" class="chart h350"></div></div>
      <div class="chart-box"><div class="chart-title">历史执行状态分布</div><div id="c-status" class="chart h350"></div></div>
    </div>
  </div>
</div>

<!-- 完成率 -->
<div class="section" id="sec-progress">
  <div class="section-header"><h2>&#9201; 三、库层级小时完成率趋势</h2></div>
  <div class="section-body">
    <div class="chart-box" style="margin-bottom:16px">
      <div class="chart-title">各分层平均完成率（全库聚合，1~10 点）</div>
      <div id="c-layer-trend" class="chart h350"></div>
    </div>
    <div class="ctrl-row">
      <label>选择库：</label>
      <select id="db-sel" class="form-select"></select>
    </div>
    <div id="c-db-trend" class="chart h500"></div>
    <div style="margin-top:20px">
      <h5 style="font-size:.92rem;font-weight:700;margin-bottom:10px">&#9888;&#65039; SLA 未达标库层（10点完成率 &lt; 80%）</h5>
      <div class="sla-grid" id="sla-list"></div>
    </div>
  </div>
</div>

<!-- 资源 -->
<div class="section" id="sec-resource">
  <div class="section-header"><h2>&#128187; 四、Hive 资源使用分析</h2></div>
  <div class="section-body">
    <div class="chart-row cols-2" style="margin-bottom:16px">
      <div class="chart-box"><div class="chart-title">队列（Queue）分布（Application 数量）</div><div id="c-queue" class="chart h300"></div></div>
      <div class="chart-box"><div class="chart-title">每小时触发 Application 数（fire_time 分布）</div><div id="c-hourly-con" class="chart h300"></div></div>
    </div>
    <div class="chart-row cols-2" style="margin-bottom:16px">
      <div class="chart-box"><div class="chart-title">Top 20 库：内存消耗总量（GB·h）</div><div id="c-db-res" class="chart h400"></div></div>
      <div class="chart-box"><div class="chart-title">Top 20 耗时最长作业（平均执行时长，分钟）</div><div id="c-top-dur" class="chart h400"></div></div>
    </div>
    <div class="chart-box">
      <div class="chart-title">Top 20 内存消耗最高作业（累计 GB·h，10天）</div>
      <div id="c-top-mem" class="chart h400"></div>
    </div>
  </div>
</div>

<!-- 排队 -->
<div class="section" id="sec-queue">
  <div class="section-header"><h2>&#128678; 五、调度排队等待分析（fire→start 延迟）</h2></div>
  <div class="section-body">
    <div class="insight-block warn">
      排队等待 = start_time - fire_time，代表作业从被调度触发到实际开始执行的等待时间。数值越大说明该时段集群资源越紧张。
    </div>
    <div class="chart-box" style="margin-top:12px">
      <div class="chart-title">各小时段排队等待时长（均值 / 中位数 / P90，分钟）</div>
      <div id="c-queue-wait" class="chart h400"></div>
    </div>
  </div>
</div>

<!-- 优化 -->
<div class="section" id="sec-opt">
  <div class="section-header">
    <h2>&#128295; 六、调度配置 Gap 优化建议</h2>
    <span class="badge-num">上游完成 → 作业启动 空闲 &gt; 60 分钟</span>
  </div>
  <div class="section-body">
    <div class="insight-block">
      以下作业的上游依赖均已完成，但作业本身的 cron 配置时间远晚于上游完成时刻，存在大量空闲等待。
      建议调整 cron 到「建议时间」（上游最晚完成 + 15分钟缓冲），可将整体完成时间显著提前。
    </div>
    <div class="table-responsive mt-3">
      <table id="opt-tbl" class="table table-striped table-hover table-sm">
        <thead>
          <tr>
            <th>作业名称</th><th>负责人</th><th>库名</th><th>分层</th>
            <th>当前配置</th><th>建议时间</th><th>平均空闲(分)</th><th>最小空闲(分)</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
</div>

<!-- ads 库中间层作业分析 -->
<div class="section" id="sec-ads-pivot">
  <div class="section-header">
    <h2>&#128204; 六-A、分析师自建"伪中间层"作业专题</h2>
    <span class="badge-num">ads 库作业 · 被下游依赖 · 存在架构治理风险</span>
  </div>
  <div class="section-body">
    <div class="insight-block warn">
      <strong>背景说明：</strong>库名含 <code>ads</code> 的作业由数据分析师创建，属于应用层；库名不含 <code>ads</code> 的属于数仓公共层。
      正常架构下，数仓公共层产出数据供 ads 层消费，数据流单向流动。但以下作业虽建在 ads 库，却被大量其他作业作为上游依赖，
      事实上承担了"中间层"职责，造成：① 维护责任不清；② 一旦延迟/失败，影响范围远超预期；③ 难以统一调度优化。
      建议将高危/中危作业迁移至数仓公共层（去掉库名中的 ads 前缀），统一纳入数仓管控。
    </div>

    <!-- KPI -->
    <div class="kpi-grid" style="margin-top:16px">
      <div class="kpi-card danger"><div class="kpi-val">__ADS_TOTAL__</div><div class="kpi-label">ads库有下游依赖作业总数</div></div>
      <div class="kpi-card danger"><div class="kpi-val">__ADS_HIGH__</div><div class="kpi-label">高危（下游≥20个）</div></div>
      <div class="kpi-card warn"><div class="kpi-val">__ADS_MID__</div><div class="kpi-label">中危（下游10~19个）</div></div>
      <div class="kpi-card"><div class="kpi-val">__ADS_LOW__</div><div class="kpi-label">低危（下游3~9个）</div></div>
      <div class="kpi-card danger"><div class="kpi-val">__ADS_MAX__</div><div class="kpi-label">单作业最大下游数</div></div>
    </div>

    <!-- 图表 -->
    <div class="chart-row cols-2" style="margin-top:16px">
      <div class="chart-box">
        <div class="chart-title">各 ads 库"伪中间层"作业数量及其带动下游总量（下游≥3）</div>
        <div id="c-ads-db" class="chart h400"></div>
      </div>
      <div class="chart-box">
        <div class="chart-title">风险矩阵：下游依赖数 vs 平均执行时长（气泡=下游依赖数，颜色=风险等级）</div>
        <div id="c-ads-scatter" class="chart h400"></div>
      </div>
    </div>

    <!-- 明细表格 -->
    <div style="margin-top:16px">
      <h5 style="font-size:.9rem;font-weight:700;margin-bottom:8px">伪中间层作业明细（下游依赖数 ≥ 1，按影响数量排序）</h5>
      <div class="table-responsive">
        <table id="ads-pivot-tbl" class="table table-striped table-hover table-sm">
          <thead>
            <tr>
              <th>作业名称</th><th>所属库</th><th>负责人</th><th>Cron时间</th>
              <th>下游依赖数</th><th>均值耗时(分)</th><th>内存(GB·h)</th><th>风险等级</th><th>下游代表作业（前5个）</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>

    <!-- 治理建议 -->
    <div style="margin-top:20px">
      <h5 style="font-size:.9rem;font-weight:700;margin-bottom:10px">&#128295; 治理建议</h5>
      <div class="chart-row cols-2">
        <div class="insight-block">
          <strong>短期（1~2周）</strong><br>
          ① 对高危作业（下游≥20）在调度系统中设置高优先级，确保其在 SLA 时间内完成；<br>
          ② 与负责人确认：该作业是否真的属于中间层数据（可复用），还是纯个人分析表；<br>
          ③ 为高危作业添加超时告警和监控，一旦延迟立即通知。
        </div>
        <div class="insight-block warn">
          <strong>中期（1个月内）</strong><br>
          ① 将被5个以上作业依赖的 ads 层作业迁移至对应数仓公共库（如 dws_pa → 去掉 ads 前缀）；<br>
          ② 迁移步骤：新建同名表于公共库 → 修改下游作业引用 → 灰度切换 → 下线 ads 层旧表；<br>
          ③ 制定规范：ads 层作业禁止被超过 N 个其他作业依赖（建议 N=3），超出须申请转层。
        </div>
      </div>
    </div>
  </div>
</div>

<!-- SQL 优化分析 -->
<div class="section" id="sec-sql">
  <div class="section-header">
    <h2>&#128269; 六-B、根因 C 专项：Top 10 高耗时作业 SQL 优化分析</h2>
    <span class="badge-num">排除 dws_common 和 _rt 库</span>
  </div>
  <div class="section-body">
    <div class="insight-block warn">
      以下作业为资源消耗最高 Top 10（按平均执行时长排序），已结合实际 SQL 代码诊断出核心问题，并给出可直接参考的优化示例。
    </div>
    <div id="sql-cards" style="margin-top:16px;"></div>
  </div>
</div>

<!-- 结论 -->
<div class="section" id="sec-conclusion">
  <div class="section-header"><h2>&#128221; 七、综合分析结论与优化行动计划</h2></div>
  <div class="section-body">
    <h5 style="font-size:.9rem;font-weight:700;margin-bottom:10px">根因分类</h5>
    <div class="chart-row cols-2" style="margin-bottom:16px">
      <div class="insight-block">
        <strong>根因 A — 调度配置冗余（Cron 配置过晚）</strong><br>
        上游依赖已完成，但 cron 触发时间滞后 1~8 小时。本次识别 <strong id="gap-cnt">—</strong> 个此类作业，最大可节省 &gt;8 小时。<span class="opt-tag">P0</span>
      </div>
      <div class="insight-block warn">
        <strong>根因 B — 集群高峰并发过载（06:00~08:00）</strong><br>
        大量作业在 06:00、06:30、08:10 等整点集中触发，造成排队高峰，P90 等待可能超 30 分钟。<span class="opt-tag">P1</span>
      </div>
      <div class="insight-block warn">
        <strong>根因 C — 单作业执行时间过长</strong><br>
        部分作业均值执行时长超 60 分钟，需 SQL 调优、数据倾斜处理或逻辑拆分。<span class="opt-tag">P2</span>
      </div>
      <div class="insight-block">
        <strong>根因 D — 底层数据就绪时间过晚</strong><br>
        部分库 dts/ods 层 10 点完成率仍低于 60%（如 dws_erp、dws_common/dts），需专项治理。<span class="opt-tag">P2</span>
      </div>
    </div>
    <h5 style="font-size:.9rem;font-weight:700;margin-bottom:10px">分优先级行动计划</h5>
    <div class="table-responsive">
      <table class="table table-bordered table-sm" style="font-size:.82rem">
        <thead style="background:#1a73e8;color:#fff">
          <tr><th>优先级</th><th>行动项</th><th>涉及范围</th><th>预期收益</th><th>实施成本</th></tr>
        </thead>
        <tbody>
          <tr><td><span class="opt-tag">P0</span></td><td>批量调整 Top50 Gap 作业的 Cron 配置（可节省 &gt;300 分钟）</td><td>dws_ads_pg / dws_ads_pd_o / dws_ads_op_o 等</td><td>ADS 层完成时间整体提前 1~3 小时</td><td>极低</td></tr>
          <tr><td><span class="opt-tag">P0</span></td><td>排查 SLA 完全未达标的库层（10点完成率=0%）</td><td>dws_haddock/dim、dws_ps_rt/dts 等</td><td>消除"永久未完成"风险</td><td>低</td></tr>
          <tr><td><span class="opt-tag">P1</span></td><td>错峰 06:00~08:00 高峰段 Cron（按 5 分钟粒度分散）</td><td>所有在此段触发的作业</td><td>降低排队等待，减少尾延迟</td><td>低</td></tr>
          <tr><td><span class="opt-tag">P1</span></td><td>调高关键路径作业的 priority 配置</td><td>各库 ods/dts → dwd → ads 主链路作业</td><td>优先调度核心链路</td><td>低</td></tr>
          <tr><td><span class="opt-tag">P2</span></td><td>Top20 高耗时作业 SQL 调优（P90 &gt; 60 分钟）</td><td>见资源分析章节</td><td>缩短关键路径长度</td><td>中</td></tr>
          <tr><td><span class="opt-tag">P2</span></td><td>高重试率作业稳定性治理</td><td>retry_num &gt; 0 的高频失败作业</td><td>减少资源浪费，降低下游延迟传导</td><td>中</td></tr>
          <tr><td><span class="opt-tag">P3</span></td><td>dws_erp / dws_common dts 层数据源专项优化</td><td>dws_erp、dws_common</td><td>彻底解决底层就绪晚的根因</td><td>高</td></tr>
          <tr><td><span class="opt-tag">P3</span></td><td>建立各库各层 SLA 监控报警体系</td><td>全平台</td><td>日常运营保障，提前预警延迟</td><td>中</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</div>

<div class="footer">调度作业全面瓶颈分析报告 &nbsp;|&nbsp; 生成时间：__GEN_TIME__ &nbsp;|&nbsp; 数据区间：2026-03-01 ~ 2026-03-10</div>
</div>

<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/dataTables.bootstrap5.min.js"></script>
<script>
// ── 数据注入 ──
var typeDistData  = __TYPE_DIST__;
var layerDistData = __LAYER_DIST__;
var dbDistData    = __DB_DIST__;
var cronHourData  = __CRON_H__;
var statusData    = __STATUS__;
var chartData     = __CHART_DATA__;
var dbList        = __DB_LIST__;
var queueDist     = __QUEUE_DIST__;
var topDur        = __TOP_DUR__;
var topMem        = __TOP_MEM__;
var dbRes         = __DB_RES__;
var hourlyCon     = __HOURLY_CON__;
var gapData       = __GAP_DATA__;
var queueWait     = __QUEUE_WAIT__;
var layerAvgData  = __LAYER_AVG__;
var slaFailData   = __SLA_FAIL_DATA__;
var sqlOptData    = __SQL_OPT__;



// ── 工具函数 ──
var palette = ['#1a73e8','#34a853','#fbbc04','#ea4335','#9c27b0','#00bcd4','#ff5722','#607d8b','#795548','#4caf50'];
function initChart(id){ return echarts.init(document.getElementById(id)); }
var layerColor = {ods:'#1a73e8',dts:'#9c27b0',dwd:'#34a853',dim:'#ff9800',tms:'#e91e63',ads:'#ea4335'};

// ── 1. 作业类型饼图 ──
(function(){
  var c = initChart('c-type');
  c.setOption({
    tooltip:{trigger:'item'},
    legend:{orient:'vertical',right:10,top:'center',textStyle:{fontSize:12}},
    color:palette,
    series:[{type:'pie',radius:['45%','70%'],center:['40%','50%'],
      data:Object.entries(typeDistData).map(function(e){return {name:e[0],value:e[1]};}),
      label:{formatter:'{b}\n{d}%'},emphasis:{itemStyle:{shadowBlur:8}}
    }]
  });
})();

// ── 2. 分层柱状图 ──
(function(){
  var c = initChart('c-layer');
  var entries = Object.entries(layerDistData).sort(function(a,b){return b[1]-a[1];});
  c.setOption({
    tooltip:{trigger:'axis'},
    grid:{left:60,right:20,top:20,bottom:50},
    xAxis:{type:'category',data:entries.map(function(x){return x[0];}),axisLabel:{rotate:30,fontSize:11}},
    yAxis:{type:'value',name:'作业数'},
    series:[{type:'bar',data:entries.map(function(x){return x[1];}),
      itemStyle:{color:function(p){return layerColor[p.name]||'#607d8b';},borderRadius:[4,4,0,0]},
      label:{show:true,position:'top',fontSize:11}
    }]
  });
})();

// ── 3. Cron 触发时间分布 ──
(function(){
  var c = initChart('c-cron');
  var hrs = Array.from({length:24},function(_,i){return i;});
  var vals = hrs.map(function(h){return cronHourData[String(h)]||0;});
  c.setOption({
    tooltip:{trigger:'axis',formatter:function(params){return params[0].axisValue+':00  '+params[0].value+' 个作业';}},
    grid:{left:50,right:20,top:20,bottom:50},
    xAxis:{type:'category',data:hrs.map(function(h){return h+':00';}),axisLabel:{rotate:45,fontSize:10}},
    yAxis:{type:'value',name:'作业数'},
    series:[{type:'bar',data:vals,
      itemStyle:{color:function(p){return p.value>100?'#ea4335':p.value>50?'#fbbc04':'#1a73e8';},borderRadius:[3,3,0,0]},
      markLine:{data:[{type:'max',name:'最大值',lineStyle:{color:'#ea4335'}}]}
    }]
  });
})();

// ── 4. Top20 库作业数 ──
(function(){
  var c = initChart('c-db');
  var entries = Object.entries(dbDistData).sort(function(a,b){return b[1]-a[1];});
  c.setOption({
    tooltip:{trigger:'axis'},
    grid:{left:160,right:20,top:20,bottom:20},
    xAxis:{type:'value',name:'作业数'},
    yAxis:{type:'category',data:entries.map(function(x){return x[0];}).reverse(),axisLabel:{fontSize:11}},
    series:[{type:'bar',data:entries.map(function(x){return x[1];}).reverse(),
      itemStyle:{color:'#1a73e8',borderRadius:[0,4,4,0]},
      label:{show:true,position:'right',fontSize:11}
    }]
  });
})();

// ── 5. 执行状态饼图 ──
(function(){
  var c = initChart('c-status');
  var colorMap = {SUCCESSFUL:'#34a853',FAILED:'#ea4335',KILLED:'#fbbc04',RUNNING:'#1a73e8'};
  c.setOption({
    tooltip:{trigger:'item',formatter:'{b}: {c} ({d}%)'},
    legend:{orient:'vertical',right:10,top:'center'},
    series:[{type:'pie',radius:['45%','70%'],center:['40%','50%'],
      data:Object.entries(statusData).map(function(e){return {name:e[0],value:e[1],itemStyle:{color:colorMap[e[0]]||'#607d8b'}};}),
      emphasis:{itemStyle:{shadowBlur:8}}
    }]
  });
})();

// ── 6. 全层级平均完成率趋势 ──
(function(){
  var c = initChart('c-layer-trend');
  var hours = [1,2,3,4,5,6,7,8,9,10];
  var series = layerAvgData.map(function(row){
    var key = row.layer;
    var vals = hours.map(function(h){return row['h'+h];});
    return {
      name:key, type:'line', smooth:true, data:vals,
      lineStyle:{width:3,color:layerColor[key]||'#607d8b'},
      itemStyle:{color:layerColor[key]||'#607d8b'},
      symbol:'circle', symbolSize:6,
      markLine:{data:[{yAxis:90,name:'SLA 90%',lineStyle:{type:'dashed',color:'#ccc',width:1},label:{formatter:'SLA 90%'}}],silent:true}
    };
  });
  c.setOption({
    tooltip:{trigger:'axis',formatter:function(params){
      var s='<b>'+params[0].axisValue+'</b><br>';
      params.forEach(function(p){s+=p.marker+p.seriesName+': <b>'+(p.value||0).toFixed(1)+'%</b><br>';});
      return s;
    }},
    legend:{bottom:0,textStyle:{fontSize:12}},
    grid:{left:50,right:20,top:20,bottom:60},
    xAxis:{type:'category',data:hours.map(function(h){return h+'点';}),boundaryGap:false},
    yAxis:{type:'value',name:'完成率(%)',max:100},
    series:series
  });
})();

// ── 7. 按库完成率趋势（带下拉）──
var dbSel = document.getElementById('db-sel');
dbList.forEach(function(db){var o=document.createElement('option');o.value=o.text=db;dbSel.appendChild(o);});
var dbTrendChart = null;
function renderDbTrend(db){
  if(!dbTrendChart) dbTrendChart = initChart('c-db-trend');
  var data = chartData[db]||{};
  var hours = [1,2,3,4,5,6,7,8,9,10];
  var series = Object.entries(data).map(function(e){
    var layer=e[0], vals=e[1];
    return {name:layer,type:'line',smooth:true,data:vals,
      lineStyle:{width:3,color:layerColor[layer]||'#607d8b'},
      itemStyle:{color:layerColor[layer]||'#607d8b'},
      symbol:'circle',symbolSize:7,
      markLine:{data:[{yAxis:90,lineStyle:{type:'dashed',color:'#ccc',width:1},label:{formatter:'90%'}}],silent:true}
    };
  });
  dbTrendChart.setOption({
    title:{text:db+' — 各层级 1~10 点完成率',left:'center',textStyle:{fontSize:14,fontWeight:600}},
    tooltip:{trigger:'axis',formatter:function(params){
      var s='<b>'+params[0].axisValue+'</b><br>';
      params.forEach(function(p){s+=p.marker+p.seriesName+': <b>'+(p.value||0).toFixed(1)+'%</b><br>';});
      return s;
    }},
    legend:{bottom:0},
    grid:{left:50,right:20,top:50,bottom:60},
    xAxis:{type:'category',data:hours.map(function(h){return h+'点';}),boundaryGap:false},
    yAxis:{type:'value',name:'完成率(%)',max:100},
    series:series
  },true);
}
if(dbList.length>0){renderDbTrend(dbList[0]);dbSel.value=dbList[0];}
dbSel.addEventListener('change',function(e){renderDbTrend(e.target.value);});

// ── 8. SLA 未达标列表 ──
(function(){
  var el = document.getElementById('sla-list');
  slaFailData.forEach(function(row){
    var div=document.createElement('div');
    div.className='sla-card sla-fail';
    div.innerHTML='<span>'+row.db_name+' <span class="badge-layer l-'+(row.layer||'other')+'">'+row.layer+'</span></span>'+
      '<span style="font-weight:700;color:#e53935">9点:'+(row.h9||0).toFixed(1)+'% / 10点:'+(row.h10||0).toFixed(1)+'%</span>';
    el.appendChild(div);
  });
  if(slaFailData.length===0){el.innerHTML='<div class="sla-card sla-ok" style="grid-column:1/-1">&#10003; 全部库层均达标</div>';}
})();

// ── 9. 队列分布饼图 ──
(function(){
  var c = initChart('c-queue');
  c.setOption({
    tooltip:{trigger:'item',formatter:'{b}: {c} ({d}%)'},
    legend:{orient:'vertical',right:10,top:'center',textStyle:{fontSize:11}},
    color:palette,
    series:[{type:'pie',radius:['45%','70%'],center:['38%','50%'],
      data:Object.entries(queueDist).map(function(e){return {name:e[0]||'default',value:e[1]};}),
      label:{formatter:'{b}\n{d}%'},emphasis:{itemStyle:{shadowBlur:8}}
    }]
  });
})();

// ── 10. 每小时 Application 并发量 ──
(function(){
  var c = initChart('c-hourly-con');
  var hrs = Array.from({length:24},function(_,i){return i;});
  var vals = hrs.map(function(h){return hourlyCon[h]||0;});
  c.setOption({
    tooltip:{trigger:'axis'},
    grid:{left:50,right:20,top:20,bottom:50},
    xAxis:{type:'category',data:hrs.map(function(h){return h+':00';}),axisLabel:{rotate:45,fontSize:10}},
    yAxis:{type:'value',name:'Application数'},
    series:[{type:'bar',data:vals,
      itemStyle:{color:function(p){return p.value>3000?'#ea4335':p.value>1500?'#fbbc04':'#34a853';},borderRadius:[3,3,0,0]}
    }]
  });
})();

// ── 11. 按库内存消耗 ──
(function(){
  var c = initChart('c-db-res');
  var entries = dbRes.slice(0,20).sort(function(a,b){return b.total_mem_gb_h-a.total_mem_gb_h;});
  c.setOption({
    tooltip:{trigger:'axis',formatter:function(params){return params[0].name+'<br>内存: '+(params[0].value||0).toFixed(0)+' GB·h';}},
    grid:{left:170,right:20,top:20,bottom:20},
    xAxis:{type:'value',name:'GB·h'},
    yAxis:{type:'category',data:entries.map(function(x){return x.db_name;}).reverse(),axisLabel:{fontSize:11}},
    series:[{type:'bar',data:entries.map(function(x){return x.total_mem_gb_h;}).reverse(),
      itemStyle:{color:'#9c27b0',borderRadius:[0,4,4,0]},
      label:{show:true,position:'right',fontSize:10,formatter:function(p){return (p.value||0).toFixed(0);}}
    }]
  });
})();

// ── 12. Top20 耗时最长作业 ──
(function(){
  var c = initChart('c-top-dur');
  var data = topDur.slice(0,20).sort(function(a,b){return b.avg_elapsed_min-a.avg_elapsed_min;});
  function shortName(n){return n&&n.length>40?'...'+n.slice(-37):n;}
  c.setOption({
    tooltip:{trigger:'axis',formatter:function(params){
      var d=data[data.length-1-params[0].dataIndex];
      return d?(d.job_name+'<br>均值: '+params[0].value+' 分钟<br>P90: '+d.p90_elapsed_min+' 分钟'):'';
    }},
    grid:{left:300,right:60,top:20,bottom:20},
    xAxis:{type:'value',name:'分钟'},
    yAxis:{type:'category',data:data.map(function(x){return shortName(x.job_name);}).reverse(),axisLabel:{fontSize:10}},
    series:[
      {name:'均值',type:'bar',data:data.map(function(x){return x.avg_elapsed_min;}).reverse(),
        itemStyle:{color:'#1a73e8',borderRadius:[0,4,4,0]},barCategoryGap:'40%',
        label:{show:true,position:'right',fontSize:10,formatter:function(p){return (p.value||0).toFixed(1);}}
      },
      {name:'P90',type:'scatter',data:data.map(function(x){return x.p90_elapsed_min;}).reverse(),
        symbolSize:8,itemStyle:{color:'#ea4335'}
      }
    ],
    legend:{top:0,right:0}
  });
})();

// ── 13. Top20 内存消耗最高作业 ──
(function(){
  var c = initChart('c-top-mem');
  var data = topMem.slice(0,20).sort(function(a,b){return b.total_mem_gb_h-a.total_mem_gb_h;});
  function shortName(n){return n&&n.length>50?'...'+n.slice(-47):n;}
  c.setOption({
    tooltip:{trigger:'axis',formatter:function(params){return params[0].name+'<br>内存: '+(params[0].value||0).toFixed(0)+' GB·h';}},
    grid:{left:360,right:20,top:20,bottom:20},
    xAxis:{type:'value',name:'GB·h'},
    yAxis:{type:'category',data:data.map(function(x){return shortName(x.job_name);}).reverse(),axisLabel:{fontSize:10}},
    series:[{type:'bar',data:data.map(function(x){return x.total_mem_gb_h;}).reverse(),
      itemStyle:{color:'#e91e63',borderRadius:[0,4,4,0]},
      label:{show:true,position:'right',fontSize:10,formatter:function(p){return (p.value||0).toFixed(0);}}
    }]
  });
})();

// ── 14. 排队等待分析 ──
(function(){
  var c = initChart('c-queue-wait');
  var hrs = queueWait.map(function(x){return x.hour+':00';});
  c.setOption({
    tooltip:{trigger:'axis'},
    legend:{bottom:0},
    grid:{left:60,right:20,top:20,bottom:50},
    xAxis:{type:'category',data:hrs},
    yAxis:{type:'value',name:'等待时长(分钟)'},
    series:[
      {name:'均值',type:'line',smooth:true,data:queueWait.map(function(x){return +(x.avg_queue||0).toFixed(1);}),
        lineStyle:{width:2,color:'#1a73e8'},areaStyle:{color:'rgba(26,115,232,.1)'}},
      {name:'中位数',type:'line',smooth:true,data:queueWait.map(function(x){return +(x.median_queue||0).toFixed(1);}),
        lineStyle:{width:2,color:'#34a853'}},
      {name:'P90',type:'line',smooth:true,data:queueWait.map(function(x){return +(x.p90_queue||0).toFixed(1);}),
        lineStyle:{width:2,color:'#ea4335',type:'dashed'}}
    ]
  });
})();

// ── 15. 优化建议表格 ──
(function(){
  var tbody = document.querySelector('#opt-tbl tbody');
  var lc = {ods:'l-ods',dts:'l-dts',dwd:'l-dwd',dim:'l-dim',tms:'l-tms',ads:'l-ads'};
  gapData.forEach(function(r){
    var tr=document.createElement('tr');
    var lyr = r['\u5206\u5c42']||r['分层']||'';
    tr.innerHTML=
      '<td style="max-width:280px;word-break:break-all;font-size:.75rem">'+(r['\u4f5c\u4e1a\u540d']||r['作业名']||'')+'</td>'+
      '<td>'+(r['\u8d1f\u8d23\u4eba']||r['负责人']||'-')+'</td>'+
      '<td style="font-size:.8rem">'+(r['\u5e93\u540d']||r['库名']||'')+'</td>'+
      '<td><span class="badge-layer '+(lc[lyr]||'l-other')+'">'+lyr+'</span></td>'+
      '<td style="color:#666">'+(r['\u5f53\u524d\u914d\u7f6e']||r['当前配置']||'')+'</td>'+
      '<td style="font-weight:700;color:#1a73e8">'+(r['\u5efa\u8bae\u65f6\u95f4']||r['建议时间']||'')+'</td>'+
      '<td>'+(r['\u5e73\u5747\u7a7a\u95f2(\u5206)']||r['平均空闲(分)']||0)+'</td>'+
      '<td style="font-weight:700;color:#ea4335">'+(r['\u6700\u5c0f\u7a7a\u95f2(\u5206)']||r['最小空闲(分)']||0)+'</td>';
    tbody.appendChild(tr);
  });
  document.getElementById('gap-cnt').textContent = gapData.length;
  $(document).ready(function(){
    $('#opt-tbl').DataTable({
      pageLength: 20,
      lengthMenu: [[10,20,50,100,-1],[10,20,50,100,'全部']],
      order: [[7,'desc']],
      dom: '<"d-flex justify-content-between align-items-center mb-2"lf>tip',
      language: {
        lengthMenu: '每页显示 _MENU_ 条',
        zeroRecords: '没有找到匹配数据',
        info: '第 _PAGE_ / _PAGES_ 页，共 _TOTAL_ 条',
        infoEmpty: '无数据',
        infoFiltered: '（从 _MAX_ 条中过滤）',
        search: '搜索：',
        paginate: { first:'首页', last:'末页', next:'下一页', previous:'上一页' }
      },
      columnDefs:[{targets:[0],width:'280px'}]
    });
  });
})();

// ── 16. ads 伪中间层分析 ──
(function(){
  var pivotList   = __ADS_PIVOT_LIST__;
  var byDb        = __ADS_BY_DB__;
  var riskMatrix  = __ADS_RISK_MATRIX__;
  var riskColor   = {'高危':'#ea4335','中危':'#fbbc04','低危':'#34a853','观察':'#9e9e9e'};

  // ── 图1：按 db 的 pivot 数量 + 总下游 ──
  (function(){
    var c = initChart('c-ads-db');
    var dbs       = byDb.map(function(x){return x.db_name;}).reverse();
    var pivotCnts = byDb.map(function(x){return x.pivot_cnt;}).reverse();
    var downCnts  = byDb.map(function(x){return x.total_downstream;}).reverse();
    c.setOption({
      tooltip:{trigger:'axis'},
      legend:{bottom:0},
      grid:{left:180,right:60,top:20,bottom:50},
      xAxis:{type:'value',name:'数量'},
      yAxis:{type:'category',data:dbs,axisLabel:{fontSize:11}},
      series:[
        {name:'伪中间层作业数',type:'bar',data:pivotCnts,
          itemStyle:{color:'#fbbc04',borderRadius:[0,4,4,0]},
          label:{show:true,position:'right',fontSize:10}},
        {name:'带动下游总数',type:'bar',data:downCnts,
          itemStyle:{color:'#ea4335',borderRadius:[0,4,4,0]},barGap:'30%',
          label:{show:true,position:'right',fontSize:10}}
      ]
    });
  })();

  // ── 图2：风险散点图（x=执行时长, y=下游数, bubble=下游数）──
  (function(){
    var c = initChart('c-ads-scatter');
    var groups = {'高危':[],'中危':[],'低危':[],'观察':[]};
    riskMatrix.forEach(function(d){
      var g = d.risk||'观察';
      if(!groups[g]) groups[g]=[];
      groups[g].push([
        +(d.avg_dur||0).toFixed(1),
        d.downstream_cnt,
        d.downstream_cnt,
        d.name,
        d.db_name
      ]);
    });
    var series = Object.entries(groups).filter(function(e){return e[1].length>0;}).map(function(e){
      return {
        name:e[0], type:'scatter',
        data:e[1],
        symbolSize:function(val){return Math.min(50, 8+val[2]*2);},
        itemStyle:{color:riskColor[e[0]]||'#9e9e9e',opacity:.8},
        emphasis:{focus:'series'}
      };
    });
    c.setOption({
      tooltip:{
        trigger:'item',
        formatter:function(p){
          var d=p.data;
          return '<b>'+d[3]+'</b><br>库: '+d[4]+'<br>执行时长: '+d[0]+' 分钟<br>下游依赖: '+d[1]+' 个';
        }
      },
      legend:{bottom:0},
      grid:{left:60,right:20,top:20,bottom:60},
      xAxis:{type:'value',name:'平均执行时长（分钟）',nameLocation:'middle',nameGap:28,
        axisLabel:{formatter:function(v){return v>=60?(v/60).toFixed(1)+'h':v+'m';}}},
      yAxis:{type:'value',name:'下游依赖数'},
      series:series
    });
  })();

  // ── 表格 ──
  (function(){
    var tbody = document.querySelector('#ads-pivot-tbl tbody');
    var riskBadge = function(r){
      var c={'高危':'background:#f8d7da;color:#842029;','中危':'background:#fff3cd;color:#664d03;',
             '低危':'background:#d1e7dd;color:#0a3622;','观察':'background:#f8f9fa;color:#555;'};
      return '<span style="'+c[r]+'border-radius:10px;padding:2px 8px;font-size:.72rem;font-weight:700;">'+r+'</span>';
    };
    pivotList.forEach(function(r){
      var tr=document.createElement('tr');
      var dnames=(r.downstream_names||[]).slice(0,5).map(function(n){
        return '<span style="display:inline-block;background:#e8f0fe;border-radius:4px;padding:1px 6px;font-size:.7rem;margin:1px;">'+n+'</span>';
      }).join('');
      tr.innerHTML=
        '<td style="max-width:260px;word-break:break-all;font-size:.75rem">'+r.name+'</td>'+
        '<td style="font-size:.78rem">'+r.db_name+'</td>'+
        '<td>'+r.owner+'</td>'+
        '<td>'+r.cron_time+'</td>'+
        '<td style="font-weight:700;color:#ea4335">'+r.downstream_cnt+'</td>'+
        '<td>'+r.avg_dur+'</td>'+
        '<td>'+r.total_mem+'</td>'+
        '<td>'+riskBadge(r.risk)+'</td>'+
        '<td style="max-width:300px">'+dnames+'</td>';
      tbody.appendChild(tr);
    });
    $(document).ready(function(){
      $('#ads-pivot-tbl').DataTable({
        pageLength: 20,
        lengthMenu: [[10,20,50,100,-1],[10,20,50,100,'全部']],
        order: [[4,'desc']],
        dom: '<"d-flex justify-content-between align-items-center mb-2"lf>tip',
        language: {
          lengthMenu: '每页显示 _MENU_ 条',
          zeroRecords: '没有找到匹配数据',
          info: '第 _PAGE_ / _PAGES_ 页，共 _TOTAL_ 条',
          infoEmpty: '无数据',
          infoFiltered: '（从 _MAX_ 条中过滤）',
          search: '搜索：',
          paginate: { first:'首页', last:'末页', next:'下一页', previous:'上一页' }
        },
        columnDefs:[{targets:[0,8],width:'260px'}]
      });
    });
  })();
})();

// ── 17. SQL 优化分析卡片 ──
(function(){
  var sqlData = __SQL_OPT__;
  var container = document.getElementById('sql-cards');
  if(!sqlData||!sqlData.length){container.innerHTML='<p>暂无数据</p>';return;}

  var severityColor = ['#ea4335','#fbbc04','#34a853','#1a73e8','#9c27b0'];

  sqlData.forEach(function(d, idx){
    var card = document.createElement('div');
    card.style.cssText='background:#fff;border:1px solid #e8eaed;border-radius:10px;margin-bottom:20px;overflow:hidden;box-shadow:0 1px 6px rgba(0,0,0,.08);';

    // 标题栏
    var hdr = document.createElement('div');
    hdr.style.cssText='background:linear-gradient(90deg,#1a73e8,#1557b0);color:#fff;padding:12px 18px;display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;';
    hdr.innerHTML=
      '<span style="font-weight:700;font-size:.9rem;word-break:break-all">'+
      '<span style="background:rgba(255,255,255,.3);border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;margin-right:8px;font-size:.75rem;">'+(idx+1)+'</span>'+
      d.job_name+'</span>'+
      '<span style="display:flex;gap:8px;flex-shrink:0;">'+
        '<span style="background:#fff3cd;color:#856404;border-radius:10px;padding:2px 10px;font-size:.75rem;">均值 '+d.avg_dur+' 分钟</span>'+
        '<span style="background:#f8d7da;color:#842029;border-radius:10px;padding:2px 10px;font-size:.75rem;">P90 '+d.p90_dur+' 分钟</span>'+
        '<span style="background:#cff4fc;color:#055160;border-radius:10px;padding:2px 10px;font-size:.75rem;">内存 '+d.total_mem+' GB·h</span>'+
        '<span style="background:#d1e7dd;color:#0a3622;border-radius:10px;padding:2px 10px;font-size:.75rem;">'+d.db_name+'</span>'+
      '</span>';
    card.appendChild(hdr);

    // 内容区
    var body = document.createElement('div');
    body.style.cssText='padding:16px 18px;display:grid;grid-template-columns:1fr 1fr;gap:16px;';

    // 左：诊断问题 + 建议
    var left = document.createElement('div');
    var issueHtml='<div style="margin-bottom:10px"><div style="font-weight:700;font-size:.85rem;color:#c62828;margin-bottom:6px;">&#10008; 诊断出的问题</div>';
    (d.issues||['未检测到特定规则问题，请人工审查']).forEach(function(iss,i){
      issueHtml+='<div style="background:#fff3f3;border-left:3px solid #ea4335;border-radius:0 6px 6px 0;padding:8px 10px;margin-bottom:6px;font-size:.8rem;line-height:1.6;">'+
        '<span style="font-weight:700;color:#ea4335">[问题'+(i+1)+']</span> '+iss+'</div>';
    });
    issueHtml+='</div>';

    var suggHtml='<div><div style="font-weight:700;font-size:.85rem;color:#1a6b1e;margin-bottom:6px;">&#10003; 优化建议</div>';
    (d.suggestions||['开启向量化执行；动态 Reducer 数量']).forEach(function(sg,i){
      suggHtml+='<div style="background:#f0fff4;border-left:3px solid #34a853;border-radius:0 6px 6px 0;padding:8px 10px;margin-bottom:6px;font-size:.8rem;line-height:1.6;">'+
        '<span style="font-weight:700;color:#34a853">[建议'+(i+1)+']</span> '+sg+'</div>';
    });
    suggHtml+='</div>';
    left.innerHTML = issueHtml + suggHtml;

    // 右：SQL 优化示例
    var right = document.createElement('div');
    right.innerHTML='<div style="font-weight:700;font-size:.85rem;color:#1557b0;margin-bottom:6px;">&#9998; SQL 优化示例</div>'+
      '<pre style="background:#1e1e2e;color:#cdd6f4;border-radius:8px;padding:12px;font-size:.75rem;line-height:1.7;overflow-x:auto;white-space:pre-wrap;max-height:320px;overflow-y:auto;">'+
      escHtml(d.opt_sql||'-- 请人工审查 SQL')+'</pre>';

    body.appendChild(left);
    body.appendChild(right);
    card.appendChild(body);
    container.appendChild(card);
  });

  function escHtml(s){
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
})();

// ── resize ──
window.addEventListener('resize',function(){
  ['c-type','c-layer','c-cron','c-db','c-status','c-layer-trend',
   'c-queue','c-hourly-con','c-db-res','c-top-dur','c-top-mem','c-queue-wait'].forEach(function(id){
    var inst = echarts.getInstanceByDom(document.getElementById(id));
    if(inst) inst.resize();
  });
  if(dbTrendChart) dbTrendChart.resize();
});
</script>
</body>
</html>"""


def build_html(stats, progress, res_data, gap_df, queue_data, sql_opt, ads_pivot, gen_time):
    print("[6/6] 生成 HTML 报告...")

    hours = list(range(1, 11))

    # --- 小时完成率图表数据 ---
    chart_data = {}
    for _, row in progress.iterrows():
        db = row['db_name']
        if db not in chart_data:
            chart_data[db] = {}
        chart_data[db][row['layer']] = [row['h%d' % h] for h in hours]
    db_list = sorted(chart_data.keys())

    # --- SLA 达标（9点 90%）---
    sla_ok   = progress[(progress['h9'] >= 90)][['db_name','layer','h9','h10']].copy()
    sla_fail = progress[(progress['h10'] < 80)][['db_name','layer','h9','h10']].copy()

    # --- 层级汇总：全库平均完成率（所有小时）---
    h_cols = ['h%d' % h for h in hours]
    layer_avg = progress.groupby('layer')[h_cols].mean().reset_index().round(2)

    # ── JSON 序列化 ──
    def j(x):
        return json.dumps(x, ensure_ascii=False, default=str)

    # ── 占位符替换 ──
    replacements = {
        '__GEN_TIME__':   gen_time,
        '__TOTAL_JOBS__': str(stats['total_jobs']),
        '__DEP_RATE__':   str(stats['dep_rate']),
        '__RETRY_GT0__':  str(stats['retry_gt0']),
        '__SELF_RELY__':  str(stats['self_rely']),
        '__SLA_OK__':     str(len(sla_ok)),
        '__SLA_FAIL_CNT__': str(len(sla_fail)),
        '__TYPE_DIST__':  j(stats['type_dist']),
        '__LAYER_DIST__': j(dict(sorted(stats['layer_dist'].items(), key=lambda x: -x[1])[:15])),
        '__DB_DIST__':    j(dict(list(stats['db_dist'].items())[:20])),
        '__CRON_H__':     j({str(k): v for k, v in sorted(stats['cron_hour_dist'].items())}),
        '__STATUS__':     j(stats['status_dist']),
        '__CHART_DATA__': j(chart_data),
        '__DB_LIST__':    j(db_list),
        '__QUEUE_DIST__': j(res_data['queue_dist']),
        '__TOP_DUR__':    j(res_data['top_dur']),
        '__TOP_MEM__':    j(res_data['top_mem']),
        '__DB_RES__':     j(res_data['db_res']),
        '__HOURLY_CON__': j(res_data['hourly_concurrent']),
        '__GAP_DATA__':   j(gap_df.head(200).to_dict('records')),
        '__QUEUE_WAIT__': j(queue_data),
        '__LAYER_AVG__':  j(layer_avg.to_dict('records')),
        '__SLA_FAIL_DATA__':    j(sla_fail.to_dict('records')),
        '__SQL_OPT__':          j(sql_opt[:10]),
        '__ADS_TOTAL__':        str(ads_pivot[3]['total_pivot']),
        '__ADS_HIGH__':         str(ads_pivot[3]['high_risk']),
        '__ADS_MID__':          str(ads_pivot[3]['mid_risk']),
        '__ADS_LOW__':          str(ads_pivot[3]['low_risk']),
        '__ADS_MAX__':          str(ads_pivot[3]['max_downstream']),
        '__ADS_PIVOT_LIST__':   j(ads_pivot[0][:200]),
        '__ADS_BY_DB__':        j(ads_pivot[1]),
        '__ADS_RISK_MATRIX__':  j(ads_pivot[2]),
    }

    html = _tpl()
    for k, v in replacements.items():
        html = html.replace(k, v)
    return html

# ─────────────────────────────────────────────
# 导出 Excel
# ─────────────────────────────────────────────
def export_data(gap_df, ads_pivot, sql_opt):
    os.makedirs(EXPORT_DIR, exist_ok=True)

    # 1. 调度配置 Gap 优化建议（最小空闲 >= 30 分钟）
    gap_export = gap_df[gap_df['最小空闲(分)'] >= 30].copy()
    gap_path = os.path.join(EXPORT_DIR, '调度配置Gap优化建议_30min以上.xlsx')
    gap_export.to_excel(gap_path, index=False)
    print(f"  已导出 Gap 优化建议 ({len(gap_export)} 行): {gap_path}")

    # 2. 伪中间层作业明细
    pivot_records = ads_pivot[0]
    df_pivot = pd.DataFrame(pivot_records)
    # downstream_names 是 list，转为逗号分隔字符串
    if 'downstream_names' in df_pivot.columns:
        df_pivot['downstream_names'] = df_pivot['downstream_names'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else x
        )
    pivot_path = os.path.join(EXPORT_DIR, '伪中间层作业明细.xlsx')
    df_pivot.to_excel(pivot_path, index=False)
    print(f"  已导出 伪中间层作业明细 ({len(df_pivot)} 行): {pivot_path}")

    # 3. 高耗时作业 SQL 优化分析
    sql_records = []
    for item in sql_opt:
        sql_records.append({
            '作业名':     item.get('job_name', ''),
            '所属库':     item.get('db_name', ''),
            '平均耗时(分)': item.get('avg_dur', ''),
            'P90耗时(分)': item.get('p90_dur', ''),
            '内存(GB·h)': item.get('total_mem', ''),
            '运行次数':    item.get('runs', ''),
            '诊断问题':   '\n'.join(item.get('issues', [])),
            '优化建议':   '\n'.join(item.get('suggestions', [])),
            'SQL优化示例': item.get('opt_sql', ''),
            'SQL片段':    item.get('snippet', ''),
        })
    df_sql_opt = pd.DataFrame(sql_records)
    sql_path = os.path.join(EXPORT_DIR, '高耗时作业SQL优化分析.xlsx')
    df_sql_opt.to_excel(sql_path, index=False)
    print(f"  已导出 高耗时作业SQL优化分析 ({len(df_sql_opt)} 行): {sql_path}")


# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────
def main():
    df_meta, df_history, df_res, df_sql = load_data()
    stats   = basic_stats(df_meta, df_history)
    succ    = stats['succ']
    hist    = stats['hist']

    progress   = hourly_progress(succ)
    res_data   = resource_analysis(df_res)
    gap_df     = gap_analysis(df_meta, succ)
    queue_data = queue_wait_analysis(hist)
    sql_opt    = sql_optimization_analysis(df_history, df_res, df_sql)
    ads_pivot  = ads_pivot_analysis(df_meta, df_res)

    gen_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    html     = build_html(stats, progress, res_data, gap_df, queue_data, sql_opt, ads_pivot, gen_time)

    out_path = os.path.join(BASE_DIR, 'result_report.html')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\n✅ 报告已生成: {out_path}")

    print("\n[导出 Excel] 开始导出...")
    export_data(gap_df, ads_pivot, sql_opt)
    print(f"✅ Excel 文件已全部导出至: {EXPORT_DIR}")


if __name__ == '__main__':
    main()
