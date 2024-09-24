# 这是一个示例 Python 脚本。
import sys
import os
import pandas as pd
from 各国业务组 import 业务组
from 通用过程 import gc1
import sc
from datetime import date,timedelta,datetime
from dateutil.relativedelta import relativedelta
import sql
from saveexcel import save
from excel导出美化 import beautify_excel
import mail
import new1 as n1

#判断月初
def dt(x):
    global first_day,yesterday,today
    today = date.today()
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    if  x == 26:
        if today.day <= 26:
            first_day=(today.replace(day = 26) - relativedelta(months=1)).strftime("%Y-%m-%d")
        else: first_day = today.replace(day = 26).strftime("%Y-%m-%d")
        return first_day,yesterday
    elif x == 1:
        if today.day == 1:
            first_day = (today - relativedelta(months=1)).strftime("%Y-%m-%d")
        else:
            first_day = date.today().replace(day=1).strftime("%Y-%m-%d")
        return first_day,yesterday
    else: sys.exit()  # 终止程序
def 数据(link,新案):
    global data,df1,df2,df3,df4,df5,df6,df7,df8
    data= 业务组(link)
    df1 = pd.read_sql(sql.架构表(first_day,yesterday,",".join(data["组别"].apply(lambda x: f'"{x}"'))),link)
    df2 = pd.read_sql(sql.分案回款(新案[0],新案[1],新案[2],first_day +" 06:00:00",yesterday+" 23:59:59",新案[3]),link)
    df3 = pd.read_sql(sql.新老天数(first_day,yesterday),link)
    df4 = pd.read_sql(sql.离职时间(),link)
    df5 = pd.read_sql(sql.最早上线日期(),link)
    df6 = pd.read_sql(sql.外呼(first_day,yesterday),link)
    df7 = pd.read_sql(sql.晋升时间(),link)
    df8 = pd.read_sql(sql.usertable(),link)
def 过程():
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
    gb1.loc[gb1['总天数'] == 0,["新案分案本金","新案回款本金",
                             "新案展期费用","总实收","外呼次数","通时","新人天数","老人天数"]] = None
    resign = n1.离职(today,current_file_name)
    gb1= gb1[~gb1["user_id"].isin(resign["user_id"])]
    #组长架构汇总
    gb2=mg1.groupby(["asset_group_name","leader_user_no","leader_user_name","组员","user_id"],as_index= False).agg(
        manager_user_name=("manager_user_name", lambda x: x.iloc[-1]),
        manager_user_no=("manager_user_no", lambda x: x.iloc[-1]),
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
    gb2.loc[gb2['总天数'] == 0,["新案分案本金","新案回款本金",
                             "新案展期费用","总实收","外呼次数","通时","新人天数","老人天数"]] = None
    #合并其它数据,剔除本月之前离职的
    mg2 = gb1.merge(df4,on="user_id",how = "left").merge(df5,on="user_id",how = "left").\
        merge(df7,on=["user_id","asset_group_name"],how = "left").merge(df8,on=["user_id"],how = "left"). \
        merge(data[["组别", "账龄"]], left_on="asset_group_name", right_on="组别", how="left")
    mg2.loc[~mg2["asset_group_name"].isin(["B1 Group", "B2 Group","M2 Group","B Group"]), "首次晋升队列日期"] = None
    pa = mg2[(mg2["删除时间"]>=datetime.strptime(first_day,'%Y-%m-%d').date()) | mg2["删除时间"].isnull()]
    mg3 = gb2.merge(df4, on="user_id", how="left").merge(df8,left_on = "leader_user_no",right_on="no",how = "left").merge(data[["组别","账龄"]], left_on="asset_group_name",right_on="组别", how="left")
    dfs = mg3[(mg3["删除时间"] >= datetime.strptime(first_day, '%Y-%m-%d').date()) | mg3["删除时间"].isnull()]

    #主管架构汇总
    gb3=mg1.groupby(["asset_group_name","manager_user_no","manager_user_name","leader_user_no","leader_user_name","user_no","组员","user_id"],as_index= False).agg(
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
    gb3.loc[gb3['总天数'] == 0,["新案分案本金","新案回款本金",
                             "新案展期费用","总实收","外呼次数","通时","新人天数","老人天数"]] = None
    mg4 = gb3.merge(df4, on="user_id", how="left").merge(df8, left_on="leader_user_no", right_on="no",
                                                         how="left").merge(data[["组别", "账龄"]],
                                                                           left_on="asset_group_name", right_on="组别",
                                                                           how="left")
    dfm = mg4[(mg4["删除时间"] >= datetime.strptime(first_day, '%Y-%m-%d').date()) | mg4["删除时间"].isnull()]
    return pa,dfs,mg1,dfm

if __name__ == '__main__':
    # 设月初为26号
    dt(1)
    current_file_name = os.path.basename(__file__).split(".")[0]
    filename = "{0}{1}.xlsx".format(yesterday,current_file_name)
    n1.dlt("./"+current_file_name,filename)
    # yesterday = "2024-08-07"
    #修改各国新案口径
    新案3 = ['因不可抗力因素温和催收,正常撤案,冻结债务人,冻结',
             '稽核异常撤案,风险上报转客维,稽核投诉倾向撤案,转客维',
             '分案不均,拨打受限组内互换案件,分案前已结清,恢复案件撤案,分案不均补案,存在逾期案件时撤d-2案件,撤销分案-24862',
             """(ml.mission_group_name like 'PreRemind-D-2' AND ml.assign_asset_late_days = -2)
                or (ml.mission_group_name like 'PreRemind-D-2-OS-HIR' AND ml.assign_asset_late_days = -2)
                or (ml.mission_group_name like 'PreRemind-D-1' AND ml.assign_asset_late_days = -1)
                or (ml.mission_group_name like 'PreRemind-D0' AND ml.assign_asset_late_days = 0)
                OR (ml.mission_group_name LIKE 'A%' AND ml.assign_asset_late_days = 1)
                OR (ml.mission_group_name LIKE 'B%' AND ml.assign_asset_late_days IN (1,8))
                OR (ml.mission_group_name LIKE 'M2%' AND ml.assign_asset_late_days IN (1,31))"""]
    #获取数据,修改数仓,新案
    数据(sc.mxg(),新案3)
    #运行过程
    pa,dfs,mg1,dfm= 过程()
    rs = n1.renshu(mg1,df4,first_day,yesterday)
    ce,tl,sp=gc1(pa,dfs,dfm,first_day,rs)
    top催回率 = n1.top催回率(ce)
    ce,resign = n1.rg(ce,today,current_file_name)
    #保存文件
    path = "./{}".format(current_file_name)
    save("{0}{1}".format(yesterday,current_file_name), path, ce=ce, tl=tl, sp=sp,resign=resign,top催回率=top催回率)
    #美化文件
    beautify_excel(path+"/{0}{1}.xlsx".format(yesterday,current_file_name))
    #发送邮件
    df_email = pd.read_csv('./邮箱.txt', sep='=', header=None,encoding='gbk')
    df_email.columns = ['key', 'value']
    email_variable = df_email.loc[df_email['key'] == '邮箱', 'value'].values[0]
    email_password = df_email.loc[df_email['key'] == '密码', 'value'].values[0]
    to_email = df_email.loc[df_email['key'] == '{}收件人邮箱'.format(current_file_name), 'value'].values[0]
    to_email = to_email.split(',')
    # to_email = ["lichongqing@weidu.ac.cn"]
    title = "{0}{1}".format(yesterday,current_file_name)
    mail.send_email(title,path+"/{0}{1}.xlsx".format(yesterday,current_file_name),
                    filename,to_email,email_variable,email_password)