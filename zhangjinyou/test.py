#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import mysql.connector
import datetime
import warnings
import mysql_config

warnings.filterwarnings("ignore")
print("印尼周报自动化,开始运行：", datetime.datetime.now())
now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
time_end = now - datetime.timedelta(0)
time_start = now - datetime.timedelta(7)
time_start = now.replace(year=2024, month=7, day=1)  # 数据开始日期
time_end = now.replace(year=2024, month=7, day=12)  # 数据截止日期+1day
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
day_uo_df = mysql_config.indonesia_bd_engine_read(sql_day_uo, database='fox_dw')  # 每日架构查询
print(day_uo_df)

print('TEST Down')
# =============================================================================
# 数据输出
# =============================================================================
