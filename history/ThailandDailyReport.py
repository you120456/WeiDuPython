# -*- coding: utf-8 -*-
"""
Created on Thu Jan  19 17:22:00 2023

@author: yeyuhao
"""
import pandas as pd
import datetime

print("ThailandDailyReport,start：", datetime.datetime.now())
# =============================================================================
# 数据导入
# =============================================================================
path = str(r"D:\DailyReport\ThailandDailyReport\data")  # 运行sql脚本数据存储路径
Collection_organization = pd.read_excel(path + r"\员工组别.xlsx")  # 导入员工架构表
Collection_no = pd.read_excel(path + r"\员工工号.xlsx")  # 导入员工编号表
Collection_Online = pd.read_excel(path + r"\最早上线天数.xlsx")  # 导入员工最早上线天数表
Collection_repaid = pd.read_excel(path + r"\实收.xlsx")  # 导入总实收
Collection_mission = pd.read_excel(path + r"\分案.xlsx")  # 导入总实收


# =============================================================================
# 数据处理
# =============================================================================

# 为员工和组长增加工号
def add_no(organization, no):
    temp_no = no.copy()
    temp_no = temp_no.rename(columns={"name": "userName"})
    temp_df = pd.merge(organization, temp_no,
                       left_on='Team Leader', right_on='userName', how='left')  # 匹配组长编号
    temp_df.rename(columns={"no": "TL NO."}, inplace=True)  # 重命名组长编号
    del temp_df['userName']
    temp_df = pd.merge(temp_df, temp_no, left_on='name',
                       right_on='userName', how='left')  # 匹配催员编号
    temp_df.rename(columns={"no": "Collection Executive NO."}, inplace=True)  # 重命名组长编号
    del temp_df['userName']
    return temp_df


finish_df = add_no(Collection_organization, Collection_no)

# 匹配匹配员工最早上线日期
finish_df = pd.merge(finish_df, Collection_Online, on='user_id', how='left')
# 当前日期备注今天运行昨天数据
now_day = pd.Timestamp((datetime.datetime.now() - datetime.timedelta(1)).date())


# 是否新员工
def is_new(df, now):
    temp_df = df.copy()
    temp_df['days'] = now - temp_df["Min Online Date"]
    temp_df['days'] = temp_df['days'].map(lambda x: x.days)
    temp_df['Newly enrolled'] = temp_df.days.apply(lambda x: 'NO' if x > 30 else 'YES')
    del temp_df['days']
    return temp_df


finish_df = is_new(finish_df, now_day)


# 计算员工当日总实收
def repay(df, repaid) -> object:
    temp_df = df.copy()
    repaid = repaid[['user_id', '总实收', '回款日期']].copy()
    repaid = repaid[repaid['回款日期'] == now_day]
    repaid = repaid[['user_id', '总实收']]
    temp_df = pd.merge(temp_df, repaid, on='user_id', how='left')  # 匹配催员实收
    temp_df['实收排名'] = temp_df.groupby(['Group'])['总实收'].rank(ascending=False)
    return temp_df


finish_df = repay(finish_df, Collection_repaid)


# 计算员工当日分案
def mission(df,mission) -> object:
    temp_df = df.copy()
    mission= mission[['name', '派发本金', '回款本金']].copy()
    temp_df = pd.merge(temp_df, mission, on='name', how='left')  # 匹配分案
    temp_df['个人催回率'] = temp_df['回款本金']/temp_df['派发本金']
    temp_df['个人催回率'] = temp_df['个人催回率'].map(lambda x: round(x, 4))
    # temp_df['个人催回率'] = temp_df['个人催回率'].map(lambda x: str(x * 100) + '%')
    return temp_df

finish_df = mission(finish_df, Collection_mission)


print(finish_df['个人催回率'])
