#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import mysql.connector
import datetime
import warnings

import config

warnings.filterwarnings("ignore")
print("国内周报自动化,开始运行：", datetime.datetime.now())
now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
time_end = now - datetime.timedelta(0)
time_start = now - datetime.timedelta(7)
# time_start = now.replace(year=2024, month=8, day=1)  # 数据开始日期
# time_end = now.replace(year=2024, month=8, day=8)  # 数据截止日期+1day
month_start = time_start.replace(day=1, hour=6, minute=0, second=0, microsecond=0)
print(time_start.date())
# month_start = now.replace(year=2024, month=1, day=1)   # 本月初开始日期

print('周报数据时间范围{}--->{}'.format(str(time_start)[5:10],
                                str(time_end - datetime.timedelta(1))[:10]))
# =============================================================================
# 数据导入
# # =============================================================================


# 每日架构查询
sql_day_uo = '''SELECT pt_date as '日期', 
                manager_user_name '主管',
                leader_user_name '组长',
                user_id as '催员ID'
                FROM `dwd_fox_collect_user_df`
                                where pt_date >='{0}'
                                AND pt_date <'{1}'
                union all 
                SELECT date('{1}') as '日期', 
                manager_user_name '主管',
                leader_user_name '组长',
                user_id as '催员ID'
                FROM `dwd_fox_collect_user_df`
                                where pt_date >='{2}'
                                AND pt_date <'{1}'

                    '''.format(str(month_start)[0:10], str(time_end - datetime.timedelta(1))[0:10],
                               str(time_end - datetime.timedelta(2))[0:10])
day_uo_df = config.chinese_bd_engine_read(sql_day_uo, database='fox_dw')  # 每日架构查询

# day_uo_df.to_excel(r"D:\DailyReport\国内报表\周报\1.xlsx")


day_uo_df_month_end = day_uo_df[day_uo_df['日期'] == time_end.date() - datetime.timedelta(1)]

# 计算组员上线天数
sql_user_info = '''SELECT 
                    o.user_id as '催员ID' ,
                    o.work_day as '日期',
                    o.asset_group_name as '组别',
                    IF(DATEDIFF(DATE(t2.max_date),DATE(min_date))>=30,"否","是") as '是否新人', 
                    o.`attendance_status`  as "上线天数"
                    FROM ods_fox_collect_attendance_dtl o
                    LEFT JOIN 
                    (
                    select cad.user_id,
                    min(work_day) min_date
                    from ods_fox_collect_attendance_dtl cad
                    group by cad.user_id
                    ) t1
                    ON t1.user_id=o.user_id
                    LEFT JOIN 
                    (
                    select cad.user_id,
                    max(work_day) max_date
                    from ods_fox_collect_attendance_dtl cad
                     WHERE work_day>='{0}'
                    AND work_day< '{1}'
                    group by cad.user_id
                    ) t2
                    ON t2.user_id=o.user_id
                    WHERE o.work_day>='{0}'
                    AND o.work_day< '{1}'
                    '''.format(str(month_start)[0:10], str(time_end))

user_info = config.chinese_bd_engine_read(sql_user_info)  # 上线天数
user_info = pd.merge(user_info, day_uo_df, on=['催员ID', '日期'], how='left')

# 计算组员上线天数
sql_online_days = '''SELECT
                    ca.user_id as '催员ID',
                    ca.asset_group_name as '组别',
                    count(ca.`attendance_status`) AS  '上线天数'
                    FROM ods_fox_collect_attendance_dtl ca
                    WHERE ca.work_day>= '{}'
                    AND ca.work_day< '{}'
                    AND ca.attendance_status_flag = 1
                    GROUP BY 1 ,2
                    '''.format(str(time_start), str(time_end))

online_days_df = config.chinese_bd_engine_read(sql_online_days)  # 上线天数
work_user = online_days_df[online_days_df['上线天数'] > 0]
work_user['上线'] = 1
user_info = pd.merge(user_info, work_user[['催员ID', '组别', '上线']], on=['催员ID', '组别'], how='left')
user_info = user_info[user_info['上线'] == 1]
user_info.sort_values(by="日期", ascending=False, inplace=True)
user_info.to_excel(r"D:\DailyReport\国内报表\周报\1.xlsx")


# 字段一小组人数计算（小组人数-所在组数）
def Columns1(user_info):
    temp_df = user_info[['组别', '是否新人', '主管', '组长']].copy()
    temp_df1 = temp_df[temp_df['是否新人'] == '是']
    temp_df1 = temp_df1.drop_duplicates()
    temp_df2 = temp_df[temp_df['是否新人'] == '否']
    temp_df2 = temp_df2.drop_duplicates()
    temp_df = pd.concat([temp_df1, temp_df2])
    temp_df = temp_df.groupby(by=['组别', '是否新人', '主管'], as_index=False).agg({"组长": "count"})
    sum_temp_df = temp_df.groupby(by=['组别', '是否新人'], as_index=False).agg({"组长": "sum"})
    sum_temp_df['主管'] = '汇总'
    temp_df = pd.concat([temp_df, sum_temp_df])
    temp_df = temp_df.rename(columns={"组长": "组数"})
    temp_df_user = user_info[['组别', '是否新人', '主管', '组长', '催员ID']].copy()
    temp_df_user = temp_df_user.drop_duplicates()
    temp_df1_user = temp_df_user[temp_df_user['是否新人'] == '是']
    temp_df1_user = temp_df1_user.drop_duplicates()
    temp_df2_user = temp_df_user[temp_df_user['是否新人'] == '否']
    temp_df2_user = temp_df2_user.drop_duplicates()
    temp_df_user = pd.concat([temp_df1_user, temp_df2_user])
    temp_df_user = temp_df_user.groupby(by=['组别', '是否新人', '主管'], as_index=False).agg({"催员ID": "count"})
    sum_temp_df_user = temp_df_user.groupby(by=['组别', '是否新人'], as_index=False).agg({"催员ID": "sum"})
    sum_temp_df_user['主管'] = '汇总'
    temp_df_user = pd.concat([temp_df_user, sum_temp_df_user])
    temp_df_user = temp_df_user.rename(columns={"催员ID": "催员数"})
    temp_df = pd.merge(temp_df, temp_df_user, on=['组别', '是否新人', '主管'], how='left')
    temp_df[['组数', '催员数']] = temp_df[['组数', '催员数']].astype(str)
    temp_df['人数/组数'] = temp_df['催员数'] + '-' + temp_df['组数']
    return temp_df


finish_df = Columns1(user_info)
finish_df['匹配字段'] = finish_df['组别'] + finish_df['是否新人'] + finish_df['主管']
finish_df = finish_df[['组别', '是否新人', '主管', '匹配字段', '人数/组数']]

# 实收统计
sql_collect_recovery = '''SELECT 
                            cr.sys_user_id as '催员ID' ,
                            DATE(cr.repay_date) as '日期',
                            SUM(cr.repaid_total_amount)/100 as '累计实收'
                            FROM
                            ods_fox_collect_recovery cr 
                            LEFT join ods_fox_collect_attendance_dtl cad on cr.sys_user_id = cad.user_id and DATE(cr.repay_date) = cad.work_day 
                            left join ods_fox_mission_log_unassign_reason mlur on mlur.assign_mission_log_id = cr.mission_log_id
                            left join ods_fox_mission_log unassign on unassign.mission_log_id = mlur.mission_log_id and unassign.mission_log_operator = 'unassign' 
                            LEFT JOIN ods_fox_mission_log assign ON assign.mission_log_id = cr.mission_log_id AND assign.mission_log_operator = 'assign' 
                            WHERE 1 = 1 
                            and assign.mission_log_create_at >= '{0}'
                            AND  cr.repay_date>='{1}'
                            AND cr.repay_date < '{2}'
                            AND cr.batch_num IS NOT NULL 
                            and NOT (cad.attendance_status_flag in (4,6) and DATE(cr.repay_date) = cr.assigned_date ) -- 剔除短假当日分案回款
                            and NOT (cad.attendance_status_flag in (5,7)) -- 剔除长假当天所有回款
                            GROUP BY 催员ID,日期

'''.format(str(month_start), str(time_start), str(time_end))

collect_recovery_df = config.chinese_bd_engine_read(sql_collect_recovery)  # 实收记录


# 字段二日人均实收
def Columns2(collect_recovery_df, user_info, finish_df):
    temp_df = pd.merge(collect_recovery_df, user_info, on=['催员ID', '日期'], how='left')
    temp_df = temp_df.groupby(by=['组别', '是否新人', '主管'],
                              as_index=False).agg({"累计实收": "sum", "上线天数": "sum"})
    sum_temp_df = temp_df.groupby(by=['组别', '是否新人'], as_index=False).agg({"累计实收": "sum", "上线天数": "sum"})
    sum_temp_df['主管'] = '汇总'
    temp_df = pd.concat([temp_df, sum_temp_df])
    temp_df['日人均实收'] = round(temp_df['累计实收'] / temp_df['上线天数'], 0)
    temp_df = temp_df[['组别', '是否新人', '主管', '日人均实收']]
    finish_df = pd.merge(finish_df, temp_df, on=['组别', '是否新人', '主管'], how='left')
    return finish_df


finish_df = Columns2(collect_recovery_df, user_info, finish_df)

# 新案统计
sql_new_misson_log = '''  
                    select 
                    a.`被分配到催员ID` as '催员ID',
                    DATE(a.`分案时间`) as '日期',
                    a.`被分配到催员` as '催员',
                    count(DISTINCT(a.debtor_id)) as '户数',
                    ROUND(sum(a.`分案本金`),2) as '分案本金',
                    round(sum(rpa.催回本金),2) as '催回本金'
                    from 
                    (select
                    assign.mission_log_id as '分案ID',
                    assign.mission_log_asset_id as 'asset_id', 
                    assign.debtor_id,
                    assign.mission_group_name as '组别',
                    assign.group_leader_name as '组长',
                    assign.assign_principal_amount / 100 as '分案本金',
                    assign.mission_log_create_at as '分案时间',
                    assign.mission_log_assigned_user_name as '被分配到催员',
                    assign.assigned_sys_user_id as '被分配到催员ID',
                    assign.assign_debtor_late_days as '分案时债务逾期天数',
                    assign.assign_asset_late_days as '分案时资产逾期天数',
                    unassign.mission_log_create_at as '撤案时间',
                    mlur.mission_log_unassign_reason as '撤案原因',
                    cad.attendance_status_flag as '请假状态'
                    from ods_fox_mission_log assign
                    LEFT join ods_fox_collect_attendance_dtl cad on assign.assigned_sys_user_id = cad.user_id and DATE(assign.mission_log_create_at) = cad.work_day 
                    left join ods_fox_mission_log_unassign_reason mlur on mlur.assign_mission_log_id = assign.mission_log_id
                    left join ods_fox_mission_log unassign on unassign.mission_log_id = mlur.mission_log_id and unassign.mission_log_operator = 'unassign' 
                    and unassign.mission_log_create_at <'{1}'
                    where 1=1
                    and assign.mission_log_operator = 'assign'
                    and assign.mission_log_create_at >= '{0}'
                    and assign.mission_log_create_at < '{1}' 
                    ) a
                    left join 
                    (
                        SELECT 
                    cr.mission_log_id,
                            ifnull( sum( cr.`repaid_principal_amount` ), 0 )/ 100 AS '催回本金',
                                            ifnull( sum( cr.`repaid_total_amount` ), 0 )/ 100 AS '催回金额'
                    FROM
                    ods_fox_collect_recovery cr 
                    LEFT join ods_fox_collect_attendance_dtl cad on cr.sys_user_id = cad.user_id and DATE(cr.repay_date) = cad.work_day 
                    WHERE 1 = 1 
                    AND  cr.repay_date>='{0}'  -- 本月初
                    AND cr.repay_date < '{1}'
                    AND cr.`late_days` >= 1
                    and cr.repaid_total_amount >=0
                    AND cr.batch_num IS NOT NULL 
                    -- 剔除长假当天所有回款
                    and NOT (cad.attendance_status_flag in (5,7)) 
                    GROUP BY cr.mission_log_id
                        ) rpa -- 催回本金表
                        on a.分案ID = rpa.mission_log_id
                    where  1=1

                    and a.`组别` != '预提醒组'
                        -- 剔除请假当天分案
                    and NOT ( a.`请假状态`in (4,6,5,7))     
                        -- 月初请假保留老案

                    AND ((a.`分案时资产逾期天数` = 1 AND a.`组别` LIKE "A组%" ) 
                                    OR(a.`分案时资产逾期天数` = 1 AND a.`组别` LIKE "S1-1组%" ) 
                                    OR(a.`分案时资产逾期天数` = 1 AND a.`组别` LIKE "S1组" ) 
                                    OR ( a.`分案时资产逾期天数` in (1,3) AND a.`组别` LIKE "S1-2组" ) 
                                    OR ( a.`分案时资产逾期天数` in (1,5) AND a.`组别` LIKE "S1-3组" ) 
                                    OR ( a.`分案时资产逾期天数` in (1,8) AND a.`组别` LIKE "B-1组" ) 
                                    OR ( a.`分案时资产逾期天数` in (1,16) AND a.`组别` LIKE "B-2组" ) 
                                    OR ( a.`分案时资产逾期天数` in (1,8,16) AND a.`组别` LIKE "B组" ) 
                                     OR ( a.`分案时资产逾期天数` in (1,31,46) AND a.`组别` in ("C组",'天津调解M2'))
                                     OR (a.`分案时资产逾期天数` in (1,61) AND a.`组别` in ("D组",'天津调解M3'))
                                                )


                    -- 剔除撤案原因不在统计范围内的案件
                    and NOT (a.撤案原因 in ('拨打受限组内互换案件','恢复案件撤案','分案不均','分案不均补案','分案前已结清','D0撤案','存在逾期案件时撤预提醒案件','存在到期案件时撤预提醒案件') and a.撤案原因 IS NOT NULL and a.撤案时间 IS NOT NULL )
                    and NOT (a.撤案原因 in ('冻结债务人','超期案件协商还款','因不可抗力因素温和催收','冻结') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and rpa.`催回金额` IS  NULL )
                    and NOT (a.撤案原因 in ('风险上报转客维','稽核异常撤案','稽核投诉倾向撤案') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and rpa.`催回金额` IS  NULL and date(a.撤案时间) = date(a.`分案时间`))
                    and NOT (a.撤案原因 ='每天自动撤案'  and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and a.分案时债务逾期天数 in (15,46)  and date(a.撤案时间)-1 = date(a.`分案时间`))
                    group by a.`被分配到催员ID`,date(a.`分案时间`),a.`被分配到催员`
                    union all
                    -- -- 预提醒
                    select 
                    a.`被分配到催员ID` as '催员ID',
                    DATE(a.`分案时间`) as '日期',
                    a.`被分配到催员` as '催员',
                    count(DISTINCT(a.debtor_id)) as '户数',
                    ROUND(sum(a.`分案本金`),2) as '分案本金',
                    round(sum(rpa.催回本金),2) as '催回本金'
                    from 
                    (select
                    assign.mission_log_id as '分案ID',
                    assign.mission_log_asset_id as 'asset_id', 
                    assign.debtor_id,
                    assign.mission_group_name as '组别',
                    assign.group_leader_name as '组长',
                    assign.assign_principal_amount / 100 as '分案本金',
                    assign.mission_log_create_at as '分案时间',
                    assign.mission_log_assigned_user_name as '被分配到催员',
                    assign.assigned_sys_user_id as '被分配到催员ID',
                    assign.assign_debtor_late_days as '分案时债务逾期天数',
                    assign.assign_asset_late_days as '分案时资产逾期天数',
                    unassign.mission_log_create_at as '撤案时间',
                    mlur.mission_log_unassign_reason as '撤案原因',
                    cad.attendance_status_flag as '请假状态'
                    from ods_fox_mission_log assign
                    LEFT join ods_fox_collect_attendance_dtl cad on assign.assigned_sys_user_id = cad.user_id and DATE(assign.mission_log_create_at) = cad.work_day 
                    left join ods_fox_mission_log_unassign_reason mlur on mlur.assign_mission_log_id = assign.mission_log_id
                    left join ods_fox_mission_log unassign on unassign.mission_log_id = mlur.mission_log_id and unassign.mission_log_operator = 'unassign' 
                    and unassign.mission_log_create_at <'{1}'
                    where 1=1
                    and assign.mission_log_operator = 'assign'
                    and assign.mission_log_create_at >= '{0}'
                    and assign.mission_log_create_at < '{1}' 
                    ) a
                    left join 
                    (
                        SELECT 
                    cr.mission_log_id,
                            ifnull( sum( cr.`repaid_principal_amount` ), 0 )/ 100 AS '催回本金',
                                            ifnull( sum( cr.`repaid_total_amount` ), 0 )/ 100 AS '催回金额'
                    FROM
                    ods_fox_collect_recovery cr 
                    LEFT join ods_fox_collect_attendance_dtl cad on cr.sys_user_id = cad.user_id and DATE(cr.repay_date) = cad.work_day 
                    WHERE 1 = 1 
                    AND  cr.repay_date>='{0}'  -- 本月初
                    AND cr.repay_date < '{1}'
                    AND cr.`late_days` >= -5
                    and cr.repaid_total_amount >=0
                    AND cr.batch_num IS NOT NULL 
                    -- 剔除长假当天所有回款
                    and NOT (cad.attendance_status_flag in (5,7)) 
                    GROUP BY cr.mission_log_id
                        ) rpa -- 催回本金表
                        on a.分案ID = rpa.mission_log_id

                    where  1=1
                    and a.`组别` = '预提醒组'
                        -- 剔除请假当天分案
                    and NOT ( a.`请假状态`in (4,6,5,7))
                    -- 剔除撤案原因不在统计范围内的案件
                    and NOT (a.撤案原因 in ('拨打受限组内互换案件','恢复案件撤案','分案不均','分案不均补案','分案前已结清','D0撤案','存在逾期案件时撤预提醒案件','存在到期案件时撤预提醒案件') and a.撤案原因 IS NOT NULL and a.撤案时间 IS NOT NULL )
                    and NOT (a.撤案原因 in ('冻结债务人','超期案件协商还款','因不可抗力因素温和催收','冻结') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and rpa.`催回金额` IS  NULL )
                    and NOT (a.撤案原因 in ('风险上报转客维','稽核异常撤案','稽核投诉倾向撤案') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and rpa.`催回金额` IS  NULL and date(a.撤案时间) = date(a.`分案时间`))
                    group by a.`被分配到催员ID`,date(a.`分案时间`),a.`被分配到催员`
                    '''.format(str(month_start), str(time_end))

new_misson_log_df = config.chinese_bd_engine_read(sql_new_misson_log)  # 催员本周分案(新案)

# 新案D1催回来
sql_new_D1_misson_log = '''  
                        select 
                                            a.`被分配到催员ID` as '催员ID',
                                            DATE(a.`分案时间`) as '日期',
                                            a.`被分配到催员` as '催员',
                                            count(DISTINCT(a.debtor_id)) as '户数',
                                            ROUND(sum(a.`分案本金`),2) as '分案本金',
                                            round(sum(rpa.催回本金),2) as '催回本金'
                                            from 
                                            (select
                                            assign.mission_log_id as '分案ID',
                                            assign.mission_log_asset_id as 'asset_id', 
                                            assign.debtor_id,
                                            assign.mission_group_name as '组别',
                                            assign.group_leader_name as '组长',
                                            assign.assign_principal_amount / 100 as '分案本金',
                                            assign.mission_log_create_at as '分案时间',
                                            assign.mission_log_assigned_user_name as '被分配到催员',
                                            assign.assigned_sys_user_id as '被分配到催员ID',
                                            assign.assign_debtor_late_days as '分案时债务逾期天数',
                                            assign.assign_asset_late_days as '分案时资产逾期天数',
                                            unassign.mission_log_create_at as '撤案时间',
                                            mlur.mission_log_unassign_reason as '撤案原因',
                                            cad.attendance_status_flag as '请假状态'
                                            from ods_fox_mission_log assign
                                            LEFT join ods_fox_collect_attendance_dtl cad on assign.assigned_sys_user_id = cad.user_id and DATE(assign.mission_log_create_at) = cad.work_day 
                                            left join ods_fox_mission_log_unassign_reason mlur on mlur.assign_mission_log_id = assign.mission_log_id
                                            left join ods_fox_mission_log unassign on unassign.mission_log_id = mlur.mission_log_id and unassign.mission_log_operator = 'unassign' 
                                            and unassign.mission_log_create_at <'{1}'
                                            where 1=1
                                            and assign.mission_log_operator = 'assign'
                                            and assign.mission_log_create_at >= '{0}'
                                            and assign.mission_log_create_at < '{1}' 
                                            ) a
                                            left join 
                                            (
                                                SELECT 
                                            cr.mission_log_id,
                                                                repay_date,
                                                    ifnull( sum( cr.`repaid_principal_amount` ), 0 )/ 100 AS '催回本金',
                                                                    ifnull( sum( cr.`repaid_total_amount` ), 0 )/ 100 AS '催回金额'
                                            FROM
                                            ods_fox_collect_recovery cr 
                                            LEFT join ods_fox_collect_attendance_dtl cad on cr.sys_user_id = cad.user_id and DATE(cr.repay_date) = cad.work_day 
                                            WHERE 1 = 1 
                                            AND  cr.repay_date>='{0}'  -- 本月初
                                            AND cr.repay_date < '{1}'
                                            AND cr.`late_days` >= 1
                                            and cr.repaid_total_amount >=0
                                            AND cr.batch_num IS NOT NULL 
                                            -- 剔除长假当天所有回款
                                            and NOT (cad.attendance_status_flag in (5,7)) 
                                            GROUP BY cr.mission_log_id,repay_date
                                                ) rpa -- 催回本金表
                                                on a.分案ID = rpa.mission_log_id
                                                                        and DATE(a.分案时间) = DATE(rpa.repay_date)
                                            where  1=1

                                            and a.`组别` != '预提醒组'
                                                -- 剔除请假当天分案
                                            and NOT ( a.`请假状态`in (4,6,5,7))     
                                                -- 月初请假保留老案

                                            AND ((a.`分案时资产逾期天数` = 1 AND a.`组别` LIKE "A组%" ) 
                                                    OR(a.`分案时资产逾期天数` = 1 AND a.`组别` LIKE "S1-1组%" ) 
                                                                                    OR(a.`分案时资产逾期天数` = 1 AND a.`组别` LIKE "S1组" ) 
                                                                                    OR ( a.`分案时资产逾期天数` in (1,3) AND a.`组别` LIKE "S1-2组" ) 
                                                                                    OR ( a.`分案时资产逾期天数` in (1,5) AND a.`组别` LIKE "S1-3组" ) 
                                                                                    OR ( a.`分案时资产逾期天数` in (1,8) AND a.`组别` LIKE "B-1组" ) 
                                                                                    OR ( a.`分案时资产逾期天数` in (1,16) AND a.`组别` LIKE "B-2组" ) 
                                                                                    OR ( a.`分案时资产逾期天数` in (1,8,16) AND a.`组别` LIKE "B组" ) 
                                                                                     OR ( a.`分案时资产逾期天数` in (1,31,46) AND a.`组别` in ("C组",'天津调解M2'))
                                                                                     OR (a.`分案时资产逾期天数` in (1,61) AND a.`组别` in ("D组",'天津调解M3'))
                                                                        )


                                            -- 剔除撤案原因不在统计范围内的案件
                                            and NOT (a.撤案原因 in ('拨打受限组内互换案件','恢复案件撤案','分案不均','分案不均补案','分案前已结清','D0撤案','存在逾期案件时撤预提醒案件','存在到期案件时撤预提醒案件') and a.撤案原因 IS NOT NULL and a.撤案时间 IS NOT NULL )
                                            and NOT (a.撤案原因 in ('冻结债务人','超期案件协商还款','因不可抗力因素温和催收','冻结') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and rpa.`催回金额` IS  NULL )
                                            and NOT (a.撤案原因 in ('风险上报转客维','稽核异常撤案','稽核投诉倾向撤案') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and rpa.`催回金额` IS  NULL and date(a.撤案时间) = date(a.`分案时间`))
                                            and NOT (a.撤案原因 ='每天自动撤案'  and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and a.分案时债务逾期天数 in (15,46)  and date(a.撤案时间)-1 = date(a.`分案时间`))
                                            group by a.`被分配到催员ID`,date(a.`分案时间`),a.`被分配到催员`
                                            union all
                                            -- -- 预提醒
                                            select 
                                            a.`被分配到催员ID` as '催员ID',
                                            DATE(a.`分案时间`) as '日期',
                                            a.`被分配到催员` as '催员',
                                            count(DISTINCT(a.debtor_id)) as '户数',
                                            ROUND(sum(a.`分案本金`),2) as '分案本金',
                                            round(sum(rpa.催回本金),2) as '催回本金'
                                            from 
                                            (select
                                            assign.mission_log_id as '分案ID',
                                            assign.mission_log_asset_id as 'asset_id', 
                                            assign.debtor_id,
                                            assign.mission_group_name as '组别',
                                            assign.group_leader_name as '组长',
                                            assign.assign_principal_amount / 100 as '分案本金',
                                            assign.mission_log_create_at as '分案时间',
                                            assign.mission_log_assigned_user_name as '被分配到催员',
                                            assign.assigned_sys_user_id as '被分配到催员ID',
                                            assign.assign_debtor_late_days as '分案时债务逾期天数',
                                            assign.assign_asset_late_days as '分案时资产逾期天数',
                                            unassign.mission_log_create_at as '撤案时间',
                                            mlur.mission_log_unassign_reason as '撤案原因',
                                            cad.attendance_status_flag as '请假状态'
                                            from ods_fox_mission_log assign
                                            LEFT join ods_fox_collect_attendance_dtl cad on assign.assigned_sys_user_id = cad.user_id and DATE(assign.mission_log_create_at) = cad.work_day 
                                            left join ods_fox_mission_log_unassign_reason mlur on mlur.assign_mission_log_id = assign.mission_log_id
                                            left join ods_fox_mission_log unassign on unassign.mission_log_id = mlur.mission_log_id and unassign.mission_log_operator = 'unassign' 
                                            and unassign.mission_log_create_at <'{1}'
                                            where 1=1
                                            and assign.mission_log_operator = 'assign'
                                            and assign.mission_log_create_at >= '{0}'
                                            and assign.mission_log_create_at < '{1}' 
                                            ) a
                                            left join 
                                            (
                                                SELECT 
                                            cr.mission_log_id,
                                                                cr.repay_date,
                                                    ifnull( sum( cr.`repaid_principal_amount` ), 0 )/ 100 AS '催回本金',
                                                                    ifnull( sum( cr.`repaid_total_amount` ), 0 )/ 100 AS '催回金额'
                                            FROM
                                            ods_fox_collect_recovery cr 
                                            LEFT join ods_fox_collect_attendance_dtl cad on cr.sys_user_id = cad.user_id and DATE(cr.repay_date) = cad.work_day 
                                            WHERE 1 = 1 
                                            AND  cr.repay_date>='{0}'  -- 本月初
                                            AND cr.repay_date < '{1}'
                                            AND cr.`late_days` >= -5
                                            and cr.repaid_total_amount >=0
                                            AND cr.batch_num IS NOT NULL 
                                            -- 剔除长假当天所有回款
                                            and NOT (cad.attendance_status_flag in (5,7)) 
                                            GROUP BY cr.mission_log_id,cr.repay_date
                                                ) rpa -- 催回本金表
                                                on a.分案ID = rpa.mission_log_id
                                                and DATE(a.分案时间) = DATE(rpa.repay_date)
                                            where  1=1
                                            and a.`组别` = '预提醒组'
                                                -- 剔除请假当天分案
                                            and NOT ( a.`请假状态`in (4,6,5,7))
                                            -- 剔除撤案原因不在统计范围内的案件
                                            and NOT (a.撤案原因 in ('拨打受限组内互换案件','恢复案件撤案','分案不均','分案不均补案','分案前已结清','D0撤案','存在逾期案件时撤预提醒案件','存在到期案件时撤预提醒案件') and a.撤案原因 IS NOT NULL and a.撤案时间 IS NOT NULL )
                                            and NOT (a.撤案原因 in ('冻结债务人','超期案件协商还款','因不可抗力因素温和催收','冻结') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and rpa.`催回金额` IS  NULL )
                                            and NOT (a.撤案原因 in ('风险上报转客维','稽核异常撤案','稽核投诉倾向撤案') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and rpa.`催回金额` IS  NULL and date(a.撤案时间) = date(a.`分案时间`))
                                            group by a.`被分配到催员ID`,date(a.`分案时间`),a.`被分配到催员`
                    '''.format(str(time_start), str(time_end))

new_D1_misson_log_df = config.chinese_bd_engine_read(sql_new_D1_misson_log)  # 新案D1催回率

# 所有案统计
sql_misson_log = '''  
                select 
                a.`被分配到催员ID` as '催员ID',
                DATE(a.`分案时间`) as '日期',
                a.`被分配到催员` as '催员',
                count(DISTINCT(a.debtor_id)) as '户数',
                ROUND(sum(a.`分案本金`),2) as '分案本金',
                round(sum(rpa.催回本金),2) as '催回本金'
                from 
                (select
                assign.mission_log_id as '分案ID',
                assign.mission_log_asset_id as 'asset_id', 
                assign.debtor_id,
                assign.mission_group_name as '组别',
                assign.group_leader_name as '组长',
                assign.assign_principal_amount / 100 as '分案本金',
                assign.mission_log_create_at as '分案时间',
                assign.mission_log_assigned_user_name as '被分配到催员',
                assign.assigned_sys_user_id as '被分配到催员ID',
                assign.assign_debtor_late_days as '分案时债务逾期天数',
                assign.assign_asset_late_days as '分案时资产逾期天数',
                unassign.mission_log_create_at as '撤案时间',
                mlur.mission_log_unassign_reason as '撤案原因',
                cad.attendance_status_flag as '请假状态'
                from ods_fox_mission_log assign
                LEFT join ods_fox_collect_attendance_dtl cad on assign.assigned_sys_user_id = cad.user_id and DATE(assign.mission_log_create_at) = cad.work_day 
                left join ods_fox_mission_log_unassign_reason mlur on mlur.assign_mission_log_id = assign.mission_log_id
                left join ods_fox_mission_log unassign on unassign.mission_log_id = mlur.mission_log_id and unassign.mission_log_operator = 'unassign' 
                and unassign.mission_log_create_at <'{1}'
                where 1=1
                and assign.mission_log_operator = 'assign'
                and assign.mission_log_create_at >= '{0}'
                and assign.mission_log_create_at < '{1}' 
                ) a
                left join 
                (
                    SELECT 
                cr.mission_log_id,
                        ifnull( sum( cr.`repaid_principal_amount` ), 0 )/ 100 AS '催回本金',
                                        ifnull( sum( cr.`repaid_total_amount` ), 0 )/ 100 AS '催回金额'
                FROM
                ods_fox_collect_recovery cr 
                LEFT join ods_fox_collect_attendance_dtl cad on cr.sys_user_id = cad.user_id and DATE(cr.repay_date) = cad.work_day 
                WHERE 1 = 1 
                AND  cr.repay_date>='{0}'  -- 本月初
                AND cr.repay_date < '{1}'
                AND cr.`late_days` >= 1
                and cr.repaid_total_amount >=0
                AND cr.batch_num IS NOT NULL 
                -- 剔除长假当天所有回款
                and NOT (cad.attendance_status_flag in (5,7)) 
                GROUP BY cr.mission_log_id
                    ) rpa -- 催回本金表
                    on a.分案ID = rpa.mission_log_id
                where  1=1

                and a.`组别` != '预提醒组'
                    -- 剔除请假当天分案
                and NOT (day(a.`分案时间`)>1 and a.`请假状态`in (4,6,5,7))
                    -- 月初请假保留老案
                and NOT (day(a.`分案时间`)=1 and a.`请假状态`in (4,6,5,7) 
                                    AND ((a.`分案时资产逾期天数` = 1 AND a.`组别` LIKE "A组%" ) 
                                    OR(a.`分案时资产逾期天数` = 1 AND a.`组别` LIKE "S1-1组%" ) 
                                                                    OR(a.`分案时资产逾期天数` = 1 AND a.`组别` LIKE "S1组" ) 
                                                                    OR ( a.`分案时资产逾期天数` in (1,3) AND a.`组别` LIKE "S1-2组" ) 
                                                                    OR ( a.`分案时资产逾期天数` in (1,5) AND a.`组别` LIKE "S1-3组" ) 
                                                                    OR ( a.`分案时资产逾期天数` in (1,8) AND a.`组别` LIKE "B-1组" ) 
                                                                    OR ( a.`分案时资产逾期天数` in (1,16) AND a.`组别` LIKE "B-2组" ) 
                                                                    OR ( a.`分案时资产逾期天数` in (1,8,16) AND a.`组别` LIKE "B组" ) 
                                                                     OR ( a.`分案时资产逾期天数` in (1,31,46) AND a.`组别` in ("C组",'天津调解M2'))
                                                                     OR (a.`分案时资产逾期天数` in (1,61) AND a.`组别` in ("D组",'天津调解M3'))
                                            )
                                )

                -- 剔除撤案原因不在统计范围内的案件
                and NOT (a.撤案原因 in ('拨打受限组内互换案件','恢复案件撤案','分案不均','分案不均补案','分案前已结清','D0撤案','存在逾期案件时撤预提醒案件','存在到期案件时撤预提醒案件') and a.撤案原因 IS NOT NULL and a.撤案时间 IS NOT NULL )
                and NOT (a.撤案原因 in ('冻结债务人','超期案件协商还款','因不可抗力因素温和催收','冻结') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and rpa.`催回金额` IS  NULL )
                and NOT (a.撤案原因 in ('风险上报转客维','稽核异常撤案','稽核投诉倾向撤案') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and rpa.`催回金额` IS  NULL and date(a.撤案时间) = date(a.`分案时间`))
                and NOT (a.撤案原因 ='每天自动撤案'  and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL and a.分案时债务逾期天数 in (15,46)  and date(a.撤案时间)-1 = date(a.`分案时间`))
                group by a.`被分配到催员ID`,date(a.`分案时间`),a.`被分配到催员`
                union all
                -- -- 预提醒
                select 
                a.`被分配到催员ID` as '催员ID',
                DATE(a.`分案时间`) as '日期',
                a.`被分配到催员` as '催员',
                count(DISTINCT(a.debtor_id)) as '户数',
                ROUND(sum(a.`分案本金`),2) as '分案本金',
                round(sum(rpa.催回本金),2) as '催回本金'
                from 
                (select
                assign.mission_log_id as '分案ID',
                assign.mission_log_asset_id as 'asset_id', 
                assign.debtor_id,
                assign.mission_group_name as '组别',
                assign.group_leader_name as '组长',
                assign.assign_principal_amount / 100 as '分案本金',
                assign.mission_log_create_at as '分案时间',
                assign.mission_log_assigned_user_name as '被分配到催员',
                assign.assigned_sys_user_id as '被分配到催员ID',
                assign.assign_debtor_late_days as '分案时债务逾期天数',
                assign.assign_asset_late_days as '分案时资产逾期天数',
                unassign.mission_log_create_at as '撤案时间',
                mlur.mission_log_unassign_reason as '撤案原因',
                cad.attendance_status_flag as '请假状态'
                from ods_fox_mission_log assign
                LEFT join ods_fox_collect_attendance_dtl cad on assign.assigned_sys_user_id = cad.user_id and DATE(assign.mission_log_create_at) = cad.work_day 
                left join ods_fox_mission_log_unassign_reason mlur on mlur.assign_mission_log_id = assign.mission_log_id
                left join ods_fox_mission_log unassign on unassign.mission_log_id = mlur.mission_log_id and unassign.mission_log_operator = 'unassign' 
                and unassign.mission_log_create_at <'{1}'
                where 1=1
                and assign.mission_log_operator = 'assign'
                and assign.mission_log_create_at >= '{0}'
                and assign.mission_log_create_at < '{1}' 
                ) a
                left join 
                (
                    SELECT 
                cr.mission_log_id,
                        ifnull( sum( cr.`repaid_principal_amount` ), 0 )/ 100 AS '催回本金',
                                        ifnull( sum( cr.`repaid_total_amount` ), 0 )/ 100 AS '催回金额'
                FROM
                ods_fox_collect_recovery cr 
                LEFT join ods_fox_collect_attendance_dtl cad on cr.sys_user_id = cad.user_id and DATE(cr.repay_date) = cad.work_day 
                WHERE 1 = 1 
                AND  cr.repay_date>='{0}'  -- 本月初
                AND cr.repay_date < '{1}'
                AND cr.`late_days` >= -5
                and cr.repaid_total_amount >=0
                AND cr.batch_num IS NOT NULL 
                -- 剔除长假当天所有回款
                and NOT (cad.attendance_status_flag in (5,7)) 
                GROUP BY cr.mission_log_id
                    ) rpa -- 催回本金表
                    on a.分案ID = rpa.mission_log_id

                where  1=1
                and a.`组别` = '预提醒组'
                    -- 剔除请假当天分案
                and NOT ( a.`请假状态`in (4,6,5,7))
                -- 剔除撤案原因不在统计范围内的案件
                and NOT (a.撤案原因 in ('拨打受限组内互换案件','恢复案件撤案','分案不均','分案不均补案','分案前已结清','D0撤案','存在逾期案件时撤预提醒案件','存在到期案件时撤预提醒案件') 
                and a.撤案原因 IS NOT NULL and a.撤案时间 IS NOT NULL )
                and NOT (a.撤案原因 in ('冻结债务人','超期案件协商还款','因不可抗力因素温和催收','冻结') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL 
                and rpa.`催回金额` IS  NULL )
                and NOT (a.撤案原因 in ('风险上报转客维','稽核异常撤案','稽核投诉倾向撤案') and a.撤案时间 IS NOT NULL and a.撤案原因 IS NOT NULL 
                and rpa.`催回金额` IS  NULL and date(a.撤案时间) = date(a.`分案时间`))
                group by a.`被分配到催员ID`,date(a.`分案时间`),a.`被分配到催员`


                    '''.format(str(month_start), str(time_end))

misson_log_df = config.chinese_bd_engine_read(sql_misson_log)  # 催员本周分案(所有案)

# 计算组员分案天数
sql_misson_days = '''SELECT
                        ca.work_day as '日期',
                            ca.user_id AS '催员ID',
                            1 AS '分案天数' 
                        FROM
                            ods_fox_collect_attendance_dtl ca 
                        WHERE
                            ca.work_day >= '{0}' 
                            AND ca.work_day < '{1}' 
                            AND (ca.attendance_status_flag = 1 
                                    AND ca.asset_group_name IN ( 'A组', '预提醒组' ) 
                                    OR ( ca.attendance_status_flag IN ( 1, 3, 4, 6 ) AND 
                                    ca.asset_group_name NOT IN ( "A组", '预提醒组' ) ))
                    '''.format(str(time_start), str(time_end))

misson_days_df = config.chinese_bd_engine_read(sql_misson_days)  # 上线天数


# 字段3日均分案
def Columns3(misson_log_df, user_info, finish_df, misson_days_df):
    temp_df_week = misson_log_df[misson_log_df['日期'] >= time_start.date()]
    temp_df_week = pd.merge(temp_df_week, user_info, on=['催员ID', '日期'], how='left')
    temp_df_week = pd.merge(temp_df_week, misson_days_df, on=['催员ID', '日期'], how='left')
    temp_df_week = temp_df_week.groupby(by=['组别', '是否新人', '主管'],
                                        as_index=False).agg({"户数": "sum", "分案本金": "sum", "催回本金": "sum", "分案天数": "sum"})
    sum_temp_df_week = temp_df_week.groupby(by=['组别', '是否新人'], as_index=False).agg({"户数": "sum", "分案本金": "sum",
                                                                                    "催回本金": "sum", "分案天数": "sum"})
    sum_temp_df_week['主管'] = '汇总'
    temp_df_week = pd.concat([temp_df_week, sum_temp_df_week])
    temp_df_week['日人均分案个数'] = round(temp_df_week['户数'] / temp_df_week['分案天数'], 0)
    temp_df_week['日人均分案本金'] = round(temp_df_week['分案本金'] / temp_df_week['分案天数'], 0)
    temp_df_month = misson_log_df[misson_log_df['日期'] >= month_start.date()]
    temp_df_month = pd.merge(temp_df_month, user_info, on=['催员ID', '日期'], how='left')
    temp_df_month = pd.merge(temp_df_month, misson_days_df, on=['催员ID', '日期'], how='left')
    temp_df_month = temp_df_month.groupby(by=['组别', '是否新人', '主管'],
                                          as_index=False).agg({"分案本金": "sum", "催回本金": "sum"})
    sum_temp_df_month = temp_df_month.groupby(by=['组别', '是否新人'], as_index=False).agg({"分案本金": "sum", "催回本金": "sum"})
    sum_temp_df_month['主管'] = '汇总'
    temp_df_month = pd.concat([temp_df_month, sum_temp_df_month])
    temp_df_month['总催回率'] = round(temp_df_month['催回本金'] / temp_df_month['分案本金'], 4)
    temp_df = pd.merge(temp_df_week[['组别', '是否新人', '主管', '日人均分案个数', '日人均分案本金']],
                       temp_df_month[['组别', '是否新人', '主管', '总催回率']], on=['组别', '是否新人', '主管'], how='left')
    finish_df = pd.merge(finish_df, temp_df, on=['组别', '是否新人', '主管'], how='left')
    return finish_df


finish_df = Columns3(new_misson_log_df, user_info, finish_df, misson_days_df)
finish_df.rename(columns={"日人均分案个数": "日人均新案个数", "日人均分案本金": "日人均新案本金", "总催回率": "新案催回率"}, inplace=True)
finish_df = Columns3(new_D1_misson_log_df, user_info, finish_df, misson_days_df)
del finish_df['日人均分案个数']
del finish_df['日人均分案本金']
finish_df.rename(columns={"总催回率": "新案D1催回率"}, inplace=True)
finish_df = Columns3(misson_log_df, user_info, finish_df, misson_days_df)

#
# # 字段四计算在手
#
# # 每日在手案件数量
# sql_InHands_Mssion = '''SELECT IH.assigned_sys_user_id as "催员ID" ,
#                     IH.in_hand_date as "日期",
#                     count(DISTINCT(IH.debtor_id)) as '在手案件数量'
#                     FROM `daily_in_hand_case_details` IH
#                     where IH.in_hand_date >= '{}'
#                     AND IH.in_hand_date < '{}'
#                     group by 1,2
#                     '''.format(str(time_start), str(time_end))
# InHands_Mssion_df = config.local_database(sql_InHands_Mssion, base_name='qsq_fox')  # 计算在手

#
# def Columns4(InHands_Mssion_df, user_info, finish_df):
#     temp_df = pd.merge(InHands_Mssion_df, user_info, on=['催员ID', '日期'], how='left')
#     temp_df = temp_df.groupby(by=['组别', '是否新人', '主管'],
#                                   as_index=False).agg({"在手案件数量": "mean"})
#     sum_temp_df = temp_df.groupby(by=['组别', '是否新人'], as_index=False).agg({"在手案件数量": "mean"})
#     sum_temp_df['主管'] = '汇总'
#     temp_df = pd.concat([temp_df, sum_temp_df])
#     temp_df.rename(columns={"在手案件数量": "日人均在手案件个数"}, inplace=True)
#     temp_df[['日人均在手案件个数']] = temp_df[['日人均在手案件个数']].astype(int)
#     temp_df = temp_df[['组别', '是否新人', '主管', '日人均在手案件个数']]
#     finish_df = pd.merge(finish_df, temp_df, on=['组别', '是否新人', '主管'], how='left')
#     return finish_df
#
# finish_df = Columns4(InHands_Mssion_df, user_info, finish_df)

# 通话数据
sql_call = '''
select 
                    a.`日期`,
                    a.dunner_id as 催员ID,
                    a.总共拨打次数,
                    a.总共通话次数,
                    b.总共短信次数,
                    a.`当日通话时长`
                    from 
                    (SELECT
                    se.dunner_id ,
                     DATE(ch.`call_at` ) as 日期,
                    COUNT(ch.id) 总共拨打次数,
                    SUM(if(ch.call_time>0,1,0)) 总共通话次数,
                    SUM(ch.call_time) 当日通话时长
                    FROM ods_audit_call_history ch 
                    LEFT JOIN ods_audit_call_history_extend se
                    ON ch.id=se.source_id
                    WHERE  ch.`call_at` >= '{0}'
                    AND  ch.`call_at` < '{1}'
                    AND ch.call_channel=1
                    -- and ch.call_time=0
                    GROUP BY 1,2)a
                    left join 
                    (SELECT 
                    se.dunner_id,
                     DATE(sh.`sms_at` ) as 日期,
                    COUNT(DISTINCT sh.id)'总共短信次数'
                    FROM ods_audit_sms_history sh
                    JOIN ods_audit_sms_history_extend se
                    ON sh.id=se.source_id
                    WHERE  sh.`sms_channel`=1
                    AND  sh.`sms_at` >= '{0}'
                    AND  sh.`sms_at` < '{1}'
                    AND se.dunner_name IS NOT NULL
                    GROUP BY 1,2)b  
                    on a.dunner_id = b.dunner_id
                    and a.日期 = b.日期
                    '''.format(str(time_start), str(time_end))

call_df = config.chinese_bd_engine_read(sql_call)  # 通话时间


# 字段5通话数据计算
def Columns5(call_df, user_info, finish_df):
    temp_df = pd.merge(call_df, user_info, on=['催员ID', '日期'], how='left')
    temp_df[['总共拨打次数', '总共短信次数', '当日通话时长', '总共通话次数']] = temp_df[['总共拨打次数', '总共短信次数', '当日通话时长', '总共通话次数']].astype(float)
    temp_df = temp_df[temp_df['总共拨打次数'] >= 20]
    temp_df = temp_df.groupby(by=['组别', '是否新人', '主管'],
                              as_index=False).agg({"总共拨打次数": "sum", "总共短信次数": "sum",
                                                   '总共通话次数': "sum", "当日通话时长": "sum", "上线天数": "sum"})
    sum_temp_df = temp_df.groupby(by=['组别', '是否新人'], as_index=False).agg({"总共拨打次数": "sum", "总共短信次数": "sum",
                                                                          '总共通话次数': "sum", "当日通话时长": "sum",
                                                                          "上线天数": "sum"})
    sum_temp_df['主管'] = '汇总'
    temp_df = pd.concat([temp_df, sum_temp_df])
    temp_df['日人均外呼次数'] = round(temp_df['总共拨打次数'] / temp_df['上线天数'], 0)
    temp_df['日人均通次'] = round(temp_df['总共通话次数'] / temp_df['上线天数'], 0)
    temp_df['日人均短信发送量'] = round(temp_df['总共短信次数'] / temp_df['上线天数'], 0)
    temp_df['日人均通时'] = round((temp_df['当日通话时长'] / 3600) / temp_df['上线天数'], 2)
    temp_df['接通率'] = round(temp_df['总共通话次数'] / temp_df['总共拨打次数'], 4)
    temp_df = temp_df[['组别', '是否新人', '主管', '日人均外呼次数', '日人均通时', '日人均通次', '日人均短信发送量', '接通率']]
    finish_df = pd.merge(finish_df, temp_df, on=['组别', '是否新人', '主管'], how='left')
    return finish_df


finish_df = Columns5(call_df, user_info, finish_df)

# 周期内分案通话记录
sql_mission_call = '''
                          select             
                          call_data.日期,
                           call_data.催员ID,
                            call_data.债务人ID,
                            call_data.债务人关系,
                            call_data.拨打号码,
                            call_data.拨打次数,
                            call_data.是否接通
                        from 
                        (SELECT
                                                        DATE( ods_audit_call_history.call_at ) AS '日期',
                                                                ods_audit_call_history.dunner_id as '催员ID',
                                                                debtor_id as '债务人ID',
                                                        ods_audit_call_history_extend.debtor_relationship AS '债务人关系',
                                                        ods_audit_call_history.enc_debtor_phone_number as '拨打号码',
                                                         sum(if(ods_audit_call_history.dial_time>0,1,0)) AS '拨打次数',
                                                          max(if(ods_audit_call_history.call_time>0,1,0)) AS '是否接通'
                                                FROM
                                                        ods_audit_call_history 
                                                        JOIN ods_audit_call_history_extend  ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id 
                                                WHERE
                                                ods_audit_call_history.call_at>= '{0}'
                                                and ods_audit_call_history.call_at< '{1}'
                                                group by 日期,催员ID,债务人ID,债务人关系,拨打号码) call_data

                                     JOIN (select
                                                                                assign.debtor_id as '债务人ID',
                                                                                assign.assigned_sys_user_id as '催员ID',
                                                                                max(assign.mission_log_create_at)
                                                                                from ods_fox_mission_log assign 
                                                                                left join ods_fox_mission_log_unassign_reason mlur on mlur.assign_mission_log_id = assign.mission_log_id
                                                                                left join ods_fox_mission_log unassign on unassign.mission_log_id = mlur.mission_log_id and unassign.mission_log_operator = 'unassign' 
                                                                                and unassign.mission_log_create_at <'{1}'
                                                                                where 1=1
                                                                                and assign.mission_log_operator = 'assign'
                                                                                AND assign.mission_strategy != '新资产自动归户'
                                                                                and assign.director_name IS NOT NULL
                                                                                and assign.mission_log_create_at >= '{0}'
                                                                                and assign.mission_log_create_at < '{1}'
                                                                                 and NOT EXISTS
                                                                                (SELECT 1 FROM ods_fox_collect_attendance_dtl cd
                                                                                WHERE DATE(cd.work_day)=DATE(assign.mission_log_create_at)
                                                                                and cd.user_id = assign.assigned_sys_user_id
                                                                                AND cd.attendance_status_flag in (4,6,5,7)
                                                                                )
                                                                            -- 剔除撤案原因不在统计范围内的案件
                                                                            and NOT (mlur.mission_log_unassign_reason  in ('拨打受限组内互换案件','恢复案件撤案','分案不均','分案不均补案','分案前已结清','D0撤案','存在逾期案件时撤预提醒案件','存在到期案件时撤预提醒案件') 
                                                                            and mlur.mission_log_unassign_reason  IS NOT NULL and unassign.mission_log_create_at IS NOT NULL )
                                                                            and NOT (mlur.mission_log_unassign_reason  in ('冻结债务人','超期案件协商还款','因不可抗力因素温和催收','冻结') and unassign.mission_log_create_at IS NOT NULL and mlur.mission_log_unassign_reason  IS NOT NULL 
                                                                            )
                                                                            and NOT (mlur.mission_log_unassign_reason  in ('风险上报转客维','稽核异常撤案','稽核投诉倾向撤案') and unassign.mission_log_create_at IS NOT NULL and mlur.mission_log_unassign_reason  IS NOT NULL 
                                                                            and date(unassign.mission_log_create_at) = date(assign.mission_log_create_at))
                                                                                GROUP BY 1,2)	mission 
                                                                            ON call_data.`债务人ID` = mission.债务人ID
                                                                            AND call_data.`催员ID` = mission.催员ID
                    '''.format(str(time_start), str(time_end))

mission_call_df = config.chinese_bd_engine_read(sql_mission_call)  # 周期内分案通话记录

sql_mission_debtor = '''
select
                    assign.debtor_id as '债务人ID',
                    assign.assigned_sys_user_id as '催员ID',
                    assign.mission_group_name as '组别',
                    assign.director_name as '主管',
                    COUNT(*)
                     from ods_fox_mission_log assign 
                    left join ods_fox_mission_log_unassign_reason mlur on mlur.assign_mission_log_id = assign.mission_log_id
                    left join ods_fox_mission_log unassign on unassign.mission_log_id = mlur.mission_log_id and unassign.mission_log_operator = 'unassign' 
                    and unassign.mission_log_create_at <'{1}'
                   where 1=1
                   and assign.mission_log_operator = 'assign'
                   AND assign.mission_strategy != '新资产自动归户'
                   and assign.mission_log_create_at >= '{0}'
                   and assign.mission_log_create_at < '{1}'
                 and NOT EXISTS
                (SELECT 1 FROM ods_fox_collect_attendance_dtl cd
                WHERE DATE(cd.work_day)=DATE(assign.mission_log_create_at)
                and cd.user_id = assign.assigned_sys_user_id
                AND cd.attendance_status_flag in (4,6,5,7)
                )
                -- 剔除撤案原因不在统计范围内的案件
                and NOT (mlur.mission_log_unassign_reason  in ('拨打受限组内互换案件','恢复案件撤案','分案不均','分案不均补案','分案前已结清','D0撤案','存在逾期案件时撤预提醒案件','存在到期案件时撤预提醒案件') 
                and mlur.mission_log_unassign_reason  IS NOT NULL and unassign.mission_log_create_at IS NOT NULL )
                and NOT (mlur.mission_log_unassign_reason  in ('冻结债务人','超期案件协商还款','因不可抗力因素温和催收','冻结') and unassign.mission_log_create_at IS NOT NULL and mlur.mission_log_unassign_reason  IS NOT NULL 
                )
                and NOT (mlur.mission_log_unassign_reason  in ('风险上报转客维','稽核异常撤案','稽核投诉倾向撤案') and unassign.mission_log_create_at IS NOT NULL and mlur.mission_log_unassign_reason  IS NOT NULL 
                and date(unassign.mission_log_create_at) = date(assign.mission_log_create_at))
             GROUP BY 1,2,3,4
'''.format(str(time_start), str(time_end))

mission_debtor_df = config.chinese_bd_engine_read(sql_mission_debtor)  # 周期内分案通话记录
user_info1 = user_info.drop_duplicates(subset=['催员ID', '组别', '主管'], keep='first')
mission_debtor_df = pd.merge(mission_debtor_df, user_info1, on=['催员ID', '组别', '主管'], how='left')
mission_call_df = mission_call_df.dropna(subset=['债务人关系'])
mission_call_df.loc[mission_call_df['债务人关系'] != 'self', '债务人关系'] = 'other'


# 单个债务人三方联系个数
def Columns6(mission_call_df, user_info, finish_df):
    temp_df = pd.merge(mission_call_df, user_info, on=['催员ID', '日期'], how='left')
    other_temp_df = temp_df[temp_df['债务人关系'] != 'self']
    other_temp_df = other_temp_df.drop_duplicates(subset=['债务人ID', '催员ID', '组别', '是否新人', '主管', '拨打号码'], keep='first')
    other_temp_df = other_temp_df.groupby(by=['组别', '是否新人', '主管'],
                                          as_index=False).agg({"拨打号码": "count"})
    sum_other_temp_df = other_temp_df.groupby(by=['组别', '是否新人'], as_index=False).agg({"拨打号码": "sum"})
    sum_other_temp_df['主管'] = '汇总'
    other_temp_df = pd.concat([other_temp_df, sum_other_temp_df])
    other_temp_df.rename(columns={"拨打号码": "三方号码拨打个数"}, inplace=True)
    debtor_temp_df = temp_df.drop_duplicates(subset=['债务人ID', '催员ID', '组别', '是否新人', '主管'], keep='first')  # 周期内分案已拨打总债务人
    debtor_temp_df = debtor_temp_df.groupby(by=['组别', '是否新人', '主管'],
                                            as_index=False).agg({"债务人ID": "count"})
    sum_debtor_temp_df = debtor_temp_df.groupby(by=['组别', '是否新人'], as_index=False).agg({"债务人ID": "sum"})
    sum_debtor_temp_df['主管'] = '汇总'
    debtor_temp_df = pd.concat([debtor_temp_df, sum_debtor_temp_df])
    debtor_temp_df.rename(columns={"债务人ID": "已拨打债务人"}, inplace=True)
    temp_df = pd.merge(other_temp_df, debtor_temp_df, on=['组别', '是否新人', '主管'], how='left')
    temp_df['单个债务人三方联系个数'] = round(temp_df['三方号码拨打个数'] / temp_df['已拨打债务人'], 2)
    temp_df = temp_df[['组别', '是否新人', '主管', '单个债务人三方联系个数']]
    finish_df = pd.merge(finish_df, temp_df, on=['组别', '是否新人', '主管'], how='left')
    return finish_df


finish_df = Columns6(mission_call_df, user_info, finish_df)


# 案件覆盖率
def Columns7(mission_call_df, user_info, finish_df, mission_debtor_df):
    temp_call_df = pd.merge(mission_call_df, user_info, on=['催员ID', '日期'], how='left')
    temp_dial_df = temp_call_df.drop_duplicates(subset=['债务人ID', '催员ID', '组别', '是否新人', '主管'], keep='first')
    temp_dial_df = temp_dial_df.groupby(by=['组别', '是否新人', '主管'],
                                        as_index=False).agg({"债务人ID": "count"})
    sum_temp_dial_df = temp_dial_df.groupby(by=['组别', '是否新人'], as_index=False).agg({"债务人ID": "sum"})
    sum_temp_dial_df['主管'] = '汇总'
    temp_dial_df = pd.concat([temp_dial_df, sum_temp_dial_df])
    temp_dial_df.rename(columns={"债务人ID": "外呼债务人"}, inplace=True)
    mission_debtor_df = mission_debtor_df.drop_duplicates(subset=['债务人ID', '催员ID', '组别', '是否新人', '主管'], keep='first')
    temp_debtor_df = mission_debtor_df.groupby(by=['组别', '主管', '是否新人'],
                                               as_index=False).agg({"债务人ID": "count"})
    sum_temp_debtor_df = temp_debtor_df.groupby(by=['组别', '是否新人'], as_index=False).agg({"债务人ID": "sum"})
    sum_temp_debtor_df['主管'] = '汇总'
    temp_debtor_df = pd.concat([temp_debtor_df, sum_temp_debtor_df])
    temp_debtor_df.rename(columns={"债务人ID": "分配债务人"}, inplace=True)
    temp_df = pd.merge(temp_dial_df, temp_debtor_df, on=['组别', '主管', '是否新人'], how='left')
    temp_df['案件覆盖率'] = round(temp_df['外呼债务人'] / temp_df['分配债务人'], 4)
    temp_df.loc[temp_df['案件覆盖率'] > 1, '案件覆盖率'] = 1
    temp_df = temp_df[['组别', '是否新人', '主管', '案件覆盖率']]
    finish_df = pd.merge(finish_df, temp_df, on=['组别', '是否新人', '主管'], how='left')
    return finish_df


finish_df = Columns7(mission_call_df, user_info, finish_df, mission_debtor_df)


# 案件触达率
def Columns8(mission_call_df, user_info, finish_df, mission_debtor_df):
    mission_call_df = mission_call_df[mission_call_df['是否接通'] == 1]
    temp_call_df = pd.merge(mission_call_df, user_info, on=['催员ID', '日期'], how='left')
    temp_call_df = temp_call_df.drop_duplicates(subset=['债务人ID', '催员ID', '组别', '是否新人', '主管', '债务人关系'], keep='first')
    temp_call_df = temp_call_df.groupby(by=['组别', '是否新人', '主管', '债务人关系'],
                                        as_index=False).agg({"债务人ID": "count"})
    sum_temp_call_df = temp_call_df.groupby(by=['组别', '是否新人', '债务人关系'], as_index=False).agg({"债务人ID": "sum"})
    sum_temp_call_df['主管'] = '汇总'
    temp_call_df = pd.concat([temp_call_df, sum_temp_call_df])
    temp_call_df.rename(columns={"债务人ID": "接通债务人"}, inplace=True)
    temp_self_call_df = temp_call_df[temp_call_df['债务人关系'] == 'self']
    temp_self_call_df.rename(columns={"接通债务人": "本人接通债务人"}, inplace=True)
    temp_other_call_df = temp_call_df[temp_call_df['债务人关系'] != 'self']
    temp_other_call_df.rename(columns={"接通债务人": "三方接通债务人"}, inplace=True)
    temp_call_df = pd.merge(temp_self_call_df, temp_other_call_df[['组别', '是否新人', '主管', "三方接通债务人"]],
                            on=['组别', '是否新人', '主管'], how='outer')
    # 总接通债务人
    temp_total_call_df = pd.merge(mission_call_df, user_info, on=['催员ID', '日期'], how='left')
    temp_total_call_df = temp_total_call_df.drop_duplicates(subset=['债务人ID', '催员ID', '组别', '是否新人', '主管'], keep='first')
    temp_total_call_df = temp_total_call_df.groupby(by=['组别', '是否新人', '主管'],
                                                    as_index=False).agg({"债务人ID": "count"})
    sum_temp_total_call_df = temp_total_call_df.groupby(by=['组别', '是否新人'], as_index=False).agg({"债务人ID": "sum"})
    sum_temp_total_call_df['主管'] = '汇总'
    temp_total_call_df = pd.concat([temp_total_call_df, sum_temp_total_call_df])
    temp_total_call_df.rename(columns={"债务人ID": "总接通债务人"}, inplace=True)
    temp_call_df = pd.merge(temp_call_df, temp_total_call_df[['组别', '是否新人', '主管', "总接通债务人"]], on=['组别', '是否新人', '主管'],
                            how='outer')
    mission_debtor_df = mission_debtor_df.drop_duplicates(subset=['债务人ID', '催员ID', '组别', '主管', '是否新人'], keep='first')
    temp_debtor_df = mission_debtor_df.groupby(by=['组别', '主管', '是否新人'],
                                               as_index=False).agg({"债务人ID": "count"})
    sum_temp_debtor_df = temp_debtor_df.groupby(by=['组别', '是否新人'], as_index=False).agg({"债务人ID": "sum"})
    sum_temp_debtor_df['主管'] = '汇总'
    temp_debtor_df = pd.concat([temp_debtor_df, sum_temp_debtor_df])
    temp_debtor_df.rename(columns={"债务人ID": "分配债务人"}, inplace=True)
    temp_df = pd.merge(temp_call_df, temp_debtor_df, on=['组别', '主管', '是否新人'], how='left')
    temp_df['本人触达率'] = round(temp_df['本人接通债务人'] / temp_df['分配债务人'], 4)
    temp_df['三方触达率'] = round(temp_df['三方接通债务人'] / temp_df['分配债务人'], 4)
    temp_df['案件触达率'] = round(temp_df['总接通债务人'] / temp_df['分配债务人'], 4)
    temp_df = temp_df[['组别', '是否新人', '主管', '本人触达率', '三方触达率', '案件触达率']]
    finish_df = pd.merge(finish_df, temp_df, on=['组别', '是否新人', '主管'], how='left')
    return finish_df


finish_df = Columns8(mission_call_df, user_info, finish_df, mission_debtor_df)
finish_df = finish_df.dropna(subset=['日人均分案本金'])
finish_df.sort_values(by=['组别', '是否新人'], ascending=[True, False], inplace=True)
finish_df = finish_df[
    ['组别', '是否新人', '主管', '匹配字段', '人数/组数', '日人均新案个数', '日人均新案本金', '日人均分案个数', '日人均分案本金', '日人均实收', '新案催回率', '总催回率',
     '新案D1催回率', '日人均外呼次数', '日人均通时', '日人均通次', '日人均短信发送量', '接通率', '单个债务人三方联系个数', '案件覆盖率', '本人触达率', '三方触达率', '案件触达率']]

# =============================================================================
# 数据输出
# =============================================================================
now = datetime.datetime.now()
print("开始输出", datetime.datetime.now())
out_path = r"D:\DailyReport\国内报表\周报" + "\\"
writer1 = pd.ExcelWriter(
    out_path + "国内电催新周报{0}-{1}.xlsx".format(str(time_start)[5:10], str(time_end - datetime.timedelta(1))[5:10]),
    engine='xlsxwriter')
finish_df.to_excel(writer1, sheet_name='数据', index=False)
writer1._save()  # 此语句不可少，否则本地文件未保存

print("结束运行", datetime.datetime.now())
print(1)
