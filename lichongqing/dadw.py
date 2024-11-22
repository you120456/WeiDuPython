# 这是一个示例 Python 脚本。
import sys
import os
import pandas as pd
from 各国业务组 import 业务组
import sc
from datetime import date,timedelta,datetime
from dateutil.relativedelta import relativedelta
import sql
from saveexcel import save
import numpy as np
from excel导出美化 import beautify_excel
import mail
#判断月初
def dt(x):
    global first_day,yesterday
    today = date.today()
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    if  x == 26:
        if today.day <= 26:
            first_day=(today.replace(day = 26) - relativedelta(months=1)).strftime("%Y-%m-%d")
        else: first_day = today.replace(day = 26).strftime("%Y-%m-%d")
        return first_day,yesterday
    elif x == 1:
        first_day = date.today().replace(day=1).strftime("%Y-%m-%d")
        return first_day,yesterday
    else: sys.exit()  # 终止程序
def 数据(link,新案):
    global df1,df2,df3,df4,df5,df6,df7,df8
    data= 业务组(link)
    df1 = pd.read_sql(sql.架构表(first_day,yesterday,",".join(data["组别"].apply(lambda x: f'"{x}"'))),link)
    df2 = pd.read_sql(sql.分案回款(新案[0],新案[1],新案[2],first_day +" 06:00:00",yesterday+" 23:59:59",新案[3]),link)
    df3 = pd.read_sql(sql.新老天数(first_day,yesterday),link)
    df4 = pd.read_sql(sql.离职时间(),link)
    df5 = pd.read_sql(sql.最早上线日期(),link)
    df6 = pd.read_sql(sql.外呼(first_day,yesterday),link)
    df7 = pd.read_sql(sql.晋升时间(),link)
    # df8 = pd.read_sql(sql.usertable(),link)

if __name__ == '__main__':
    # 设月初为26号
    dt(26)
    #修改各国新案口径
    新案1 = ['因不可抗力因素温和催收,正常撤案,冻结债务人,冻结',
             '撤销分案-23652,稽核异常撤案,风险上报转客维,稽核投诉倾向撤案,转客维',
             '分案不均,拨打受限组内互换案件,分案前已结清,恢复案件撤案,分案不均补案,存在逾期案件时撤d-2案件',
             """(ml.mission_group_name LIKE 'Pre%' AND ml.assign_asset_late_days = -2)
            OR (ml.mission_group_name LIKE 'A%' AND ml.assign_asset_late_days = 1)
            OR (ml.mission_group_name LIKE 'B1%' AND ml.assign_asset_late_days IN (1,8))
            OR (ml.mission_group_name LIKE 'B2%' AND ml.assign_asset_late_days IN (1,15))"""]
    #获取数据,修改数仓,新案
    数据(sc.tg(),新案1)
    #每日明细
    mg1 = (df1.merge(df2,left_on = ["pt_date","user_id"],right_on=["分案日期","催员ID"],how="left")
        .merge(df3,left_on = ["pt_date","user_id"],right_on=["date(work_day)","user_id"],how="left")
           .merge(df6,left_on = ["pt_date","user_id"],right_on=["日期","id"],how="left"))
    mg1.sort_values(["user_id","pt_date"],ascending=[False,True],inplace=True)
    #个人最新架构汇总
    gb1=mg1.groupby(["asset_group_name","user_no","组员","user_id"],as_index= False).agg(
        manager_user_name = ("manager_user_name",lambda x: x.iloc[-1]),
        manager_user_no=("manager_user_no", lambda x: x.iloc[-1]),
        leader_user_name = ("leader_user_name",lambda x: x.iloc[-1]),
        leader_user_no=("leader_user_no", lambda x: x.iloc[-1]),
        新案分案本金 = ("新案分案本金",'sum'),
        新案回款本金 =("新案回款本金","sum"),
        新案展期费用=("新案展期费用","sum"),
        总实收=("总实收","sum"),
        外呼次数=("外呼次数","sum"),
        通时=("通时","sum"),
        新人天数 = ("days", lambda x: x[mg1.loc[x.index, "if_new"] == "YES"].sum()),
        老人天数 = ("days", lambda x: x[mg1.loc[x.index, "if_new"] == "NO"].sum()),
        总天数 = ("days","sum")
        )
    #个人每日架构汇总
    gb2=mg1.groupby(["asset_group_name","manager_user_no","manager_user_name","leader_user_no","leader_user_name","组员","user_id"],as_index= False).agg(
        新案分案本金 = ("新案分案本金",'sum'),
        新案回款本金 =("新案回款本金","sum"),
        新案展期费用=("新案展期费用","sum"),
        总实收=("总实收","sum"),
        外呼次数=("外呼次数","sum"),
        通时=("通时","sum"),
        新人天数 = ("days", lambda x: x[mg1.loc[x.index, "if_new"] == "YES"].sum()),
        老人天数 = ("days", lambda x: x[mg1.loc[x.index, "if_new"] == "NO"].sum()),
        总天数 = ("days","sum")
        )

    mg1.to_excel("./xcasd.xlsx")
    gb1.to_excel("./xcas.xlsx")