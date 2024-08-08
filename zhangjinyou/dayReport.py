#!/usr/bin/env python
# coding: utf-8
from email.mime.application import MIMEApplication

import pandas as pd
import mysql.connector
import datetime
import warnings
# import Workbook
import xlsxwriter
import  mysql_config
import email_init
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib


warnings.filterwarnings("ignore")
print("印尼数仓数据,开始运行：", datetime.datetime.now())
now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
time_end = now - datetime.timedelta(0)
time_start = now - datetime.timedelta(7)
time_start = now.replace(year=2024, month=7, day=1)  # 数据开始日期
time_end = now.replace(year=2024, month=7, day=12)  # 数据截止日期+1day
month_start = time_start.replace(day=1, hour=6, minute=0, second=0, microsecond=0)
print(time_start.date())

print('周报数据时间范围{}--->{}'.format(str(time_start)[5:10],
                                str(time_end - datetime.timedelta(1))[:10]))


# =============================================================================
# 查询各数据指标
# # ===========================================================================


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
#print(sql_day_uo)
#print(day_uo_df)

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

#print(sql_user_info)
user_info = mysql_config.indonesia_bd_engine_read(sql_user_info)  # 上线天数
user_info = pd.merge(user_info, day_uo_df, on=['催员ID', '日期'], how='left')

#print(user_info)

# 计算组员上线天数

sql_online_days = '''SELECT
                    ca.user_id as '催员ID',
                    ca.user_name as '催员', 
                    ca.asset_group_name as '组别',
                    count(ca.`attendance_status`) AS  '上线天数'
                    FROM ods_fox_collect_attendance_dtl ca
                    WHERE ca.work_day>= '{}'
                    AND ca.work_day< '{}'
                    AND ca.attendance_status_flag = 1
                    GROUP BY 1 ,2,3
                    '''.format(str(time_start), str(time_end))
#print(sql_online_days)
online_days_df = mysql_config.indonesia_bd_engine_read(sql_online_days)  # 上线天数


work_user = online_days_df[online_days_df['上线天数'] > 0]
work_user['上线'] = 1
user_info = pd.merge(user_info, work_user[['催员ID', '组别', '上线']], on=['催员ID', '组别'], how='left')
user_info = user_info[user_info['上线'] == 1]
user_info.loc[user_info['组别'].str.contains('SSS'), '主管'] = 'SSS'
user_info.sort_values(by="日期", ascending=False, inplace=True)


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



# =============================================================================
# 数据输出
# =============================================================================
now = datetime.datetime.now()
print("开始输出", datetime.datetime.now())
out_path = r"D:/util-work/PythonProject/PythonProject/report/" # + "\\"
writer1 = pd.ExcelWriter(
    out_path + "印尼电催新周报{0}-{1}.xlsx".format(str(time_start)[5:10], str(time_end - datetime.timedelta(1))[5:10]),
    engine='xlsxwriter')
finish_df.to_excel(writer1, sheet_name='数据', index=False)
writer1._save()  # 此语句不可少，否则本地文件未保存

excel_filename="印尼电催新周报{0}-{1}.xlsx".format(str(time_start)[5:10], str(time_end - datetime.timedelta(1))[5:10])
print("数据结束运行", datetime.datetime.now())
print('excel_filename::' + out_path + excel_filename )
print(out_path +  "印尼电催新周报{0}-{1}.xlsx".format(str(time_start)[5:10], str(time_end - datetime.timedelta(1))[5:10]))

import excel_beatful

excel_beatful.beautify_excel(out_path+excel_filename)

# =============================================================================
# 邮件发送
# =============================================================================

my_file_name = "印尼电催新周报{0}-{1}.xlsx".format(str(time_start)[5:10], str(time_end - datetime.timedelta(1))[5:10])
#print('my_file_name::'+ my_file_name)
# 文件路径
file_path = 'D:/util-work/PythonProject/PythonProject/report/' + my_file_name

#print('file_path::'+file_path)
# 得到昨天的日期
yesterdaystr = email_init.getYesterday()
my_email_from = 'BI部门自动报表机器人'
my_email_to = '524559124@qq.com'
# 邮件标题
my_email_Subject = 'dayReport ' + yesterdaystr
# 邮件正文
my_email_text = "Dear all,\n\t附件为每周数据，请查收！\n\nBI团队 "
#附件地址
my_annex_path = file_path
#附件名称
my_annex_name = my_file_name
# 生成邮件
my_msg = email_init.create_email(my_email_from, my_email_to, my_email_Subject,
                          my_email_text, my_annex_path, my_annex_name)
my_sender = 'shwd_operation@weidu.ac.cn'
my_password = 'Shanghai0615'
my_receiver = ['524559124@qq.com']#接收人邮箱列表
# 发送邮件
email_init.send_email(my_sender, my_password, my_receiver, my_msg)

print("邮件发送结束运行", datetime.datetime.now())

