#!/usr/bin/env python
# coding: utf-8


import pymysql
import mysql.connector
import datetime
import pandas as pd
import warnings
from sqlalchemy import create_engine
import config

warnings.filterwarnings("ignore")
print("脚本2：'国内短假折算'\n开始执行!", datetime.datetime.now())
now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
time_end = now
time_start = time_end - datetime.timedelta(1)
# time_start = now.replace(year=2023, month=10, day=2) # 开始日期
# time_end = now.replace(year=2023, month=10, day=3) # 数据截止日期
month_start = time_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
print('数据时间范围{}'.format(str(time_start)[0:10]))


recovery_sql = '''SELECT
                sys_user_id as '催员ID' ,
                sys_user_name as '催员',
                repaid_total_amount/100 as '实收',
                overdue_flag as '是否逾期'

            FROM
                `collect_recovery` cr 
            WHERE
                cr.repay_date >= '{}' 
                AND cr.repay_date < '{}' 
                AND cr.assigned_date >= '{}' 
                AND cr.batch_num IS NOT NULL'''.format(str(time_start), str(time_end), str(month_start)[0:10])
recovery_df = config.fox_engine(recovery_sql)

user_sql = '''SELECT user_id as '催员ID',
                user_name as '催员',
                asset_group_name as '组别'
                FROM `collect_attendance_dtl`dt
                where dt.asset_group_name in ('A【新老混合组】','A【全额催收组】')
                and dt.work_day = '{}'
                and dt.attendance_status = 1
                '''.format(str(time_start)[0:10])
user_df = config.fox_engine(user_sql)


recovery_df = pd.merge(recovery_df, user_df, on=['催员ID', '催员'], how='left')
mix_recovery_df = recovery_df[(recovery_df['组别'] == 'A【新老混合组】') & (recovery_df['是否逾期'] == 1)]

all_recovery_df = recovery_df[recovery_df['组别'] == 'A【全额催收组】']


def result(recovery_df):
    temp_df = recovery_df.groupby(by=['催员ID', '催员'], as_index=False).agg({"实收": "sum"})
    temp_df['排名'] = temp_df['实收'].rank(ascending=False, method='max')
    temp_df = temp_df.sort_values(by="排名", ascending=True)
    temp_df.loc[temp_df['排名'] <= round(len(temp_df) * 0.05, 0), '区间'] = 'TOP5%'
    temp_df.loc[(temp_df['排名'] > round(len(temp_df) * 0.05, 0)) &
                (temp_df['排名'] <= round(len(temp_df) * 0.25,
                                        0)), '区间'] = '5%-25%'
    temp_df.loc[(temp_df['排名'] > round(len(temp_df) * 0.25, 0)) &
                (temp_df['排名'] <= round(len(temp_df) * 0.50,
                                        0)), '区间'] = '25%-50%'
    temp_df.loc[(temp_df['排名'] > round(len(temp_df) * 0.50, 0)) &
                (temp_df['排名'] <= round(len(temp_df) * 0.70,
                                        0)), '区间'] = '50%-70%'
    temp_df.loc[(temp_df['排名'] > round(len(temp_df) * 0.70, 0)) &
                (temp_df['排名'] <= round(len(temp_df) * 0.90,
                                        0)), '区间'] = '70%-90%'
    temp_df['区间'].fillna(value='bottom10%', inplace=True)
    temp_df.to_excel(r"D:\DailyReport\国内报表\2号折算.xlsx")
    temp_df = temp_df.groupby(by=['区间'], as_index=False).agg({"实收": "mean"})
    return temp_df


mix_finish_df = result(mix_recovery_df)
mix_finish_df['组别'] = 'A【新老混合组】'
print (mix_finish_df)
all_finish_df = result(all_recovery_df)
all_finish_df['组别'] = 'A【全额催收组】'

finish_df = pd.concat([mix_finish_df, all_finish_df])
finish_df['日期'] = time_start.date()
finish_df = finish_df[['日期', '组别', '区间', '实收']]

# finish_df.to_excel(r"D:\DailyReport\国内报表\战报\9月日报\9月30日\30号折算.xlsx")
config.to_local_database(df=finish_df, name='recovery_obversion')


