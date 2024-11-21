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
    global data,df1,df2,df3,df4,df5,df6,df7,df8,df9
    data= 业务组(link)
    df1 = pd.read_sql(sql.架构表(first_day,yesterday,",".join(data["组别"].apply(lambda x: f'"{x}"'))),link)
    df2 = pd.read_sql(sql.国内分案回款(新案[0],新案[1],新案[2],first_day +" 06:00:00",yesterday+" 23:59:59",新案[3]),link)
    df3 = pd.read_sql(sql.新老天数(first_day,yesterday),link)
    df4 = pd.read_sql(sql.离职时间(yesterday),link)
    df5 = pd.read_sql(sql.最早上线日期(),link)
    df6 = pd.read_sql(sql.外呼(first_day,yesterday),link)
    df7 = pd.read_sql(sql.晋升时间(),link)
    df8 = pd.read_sql(sql.usertable(),link)
    df9 = pd.read_sql(sql.总分案回款(first_day,yesterday),link)

def 过程():
    #每日明细
    mg1 = (df1.merge(df2,left_on = ["pt_date","user_id"],right_on=["分案日期","催员ID"],how="left")
        .merge(df3,left_on = ["pt_date","user_id"],right_on=["date(work_day)","user_id"],how="left")
           .merge(df6,left_on = ["pt_date","user_id"],right_on=["日期","id"],how="left")
           .merge(df9, left_on=["pt_date", "user_id"], right_on=["分案日期", "催员ID"], how="left"))
    mg1.sort_values(["user_id","pt_date"],ascending=[False,True],inplace=True)
    # mg1.loc[mg1["leader_user_no"]=="WDBK009881","leader_user_id"] = "ec3af6b3ae3442dfb522ca6c6fd3ff46"



    #个人最新架构汇总
    gb1=mg1.groupby(["asset_group_name","user_no","组员","user_id"],as_index= False).agg(
        manager_user_name = ("manager_user_name",lambda x: x.iloc[-1]),
        manager_user_no=("manager_user_no", lambda x: x.iloc[-1]),
        leader_user_name = ("leader_user_name",lambda x: x.iloc[-1]),
        leader_user_no=("leader_user_no", lambda x: x.iloc[-1]),
        新案分案本金 = ("分案本金",'sum'),
        新案回款本金 =("回款本金","sum"),
        新案展期费用=("展期费用","sum"),
        总实收=("总实收","sum"),
        外呼次数=("外呼次数","sum"),
        通时=("通时","sum"),
        新人天数 = ("days", lambda x: x[mg1.loc[x.index, "if_new"] == "YES"].sum()),
        老人天数 = ("days", lambda x: x[mg1.loc[x.index, "if_new"] == "NO"].sum()),
        总天数 = ("days","sum"),
        分案本金总=("分案本金(总)", "sum"),
        催回本金总=("催回本金(总)", "sum"),
        实收总=("实收(总)", "sum"),
        债务人数=("债务人数", "sum"),
        新案债务人数=("新案债务人数", "sum"),
        催回债务人数=("催回债务人数", "sum"),
        新案催回债务人数=("新案催回债务人数", "sum"),
        资产数=("资产数", "sum"),
        新案资产数=("新案资产数", "sum"),
        分案本金=("分案本金", "sum"),
        回款本金=("回款本金", "sum"),
        催回资产数=("催回资产数", "sum"),
        新案催回资产数=("新案催回资产数", "sum")
        )
    gb1.loc[gb1['总天数'] == 0,["新案分案本金","新案回款本金",
                             "新案展期费用","总实收","外呼次数","通时","新人天数","老人天数"]] = None

    #合并其它数据,剔除本月之前离职的
    mg2 = gb1.merge(df4,on="user_id",how = "left").merge(df5,on="user_id",how = "left").\
        merge(df7,on=["user_id","asset_group_name"],how = "left").merge(df8,on=["user_id"],how = "left").\
        merge(data[["组别","账龄"]], left_on="asset_group_name",right_on="组别", how="left")
    mg2.loc[~mg2["asset_group_name"].isin(["B1 Group", "B2 Group","M2 Group","天津调解M3","鹰潭调解M2","鹰潭调解M3"
                                           "B Group","B-1优组","B-1差组","B-2组","天津调解M2"]), "首次晋升队列日期"] = None

    resign = n1.离职(yesterday,current_file_name)
    mg2= mg2[~mg2["user_id"].isin(resign["user_id"])]
    pa = mg2[(mg2["删除时间"]>=datetime.strptime(first_day,'%Y-%m-%d').date()) | mg2["删除时间"].isnull()]
    #组长架构汇总
    gb2=mg1.groupby(["asset_group_name","leader_user_no","leader_user_name","user_no","组员","user_id"],as_index= False).agg(
        manager_user_name=("manager_user_name", lambda x: x.iloc[-1]),
        manager_user_no=("manager_user_no", lambda x: x.iloc[-1]),
        新案分案本金 = ("分案本金",'sum'),
        新案回款本金 =("回款本金","sum"),
        新案展期费用=("展期费用","sum"),
        总实收=("总实收","sum"),
        外呼次数=("外呼次数","sum"),
        通时=("通时","sum"),
        新人天数 = ("days", lambda x: x[mg1.loc[x.index, "if_new"] == "YES"].sum()),
        老人天数 = ("days", lambda x: x[mg1.loc[x.index, "if_new"] == "NO"].sum()),
        总天数 = ("days","sum"),
        分案本金总=("分案本金(总)", "sum"),
        催回本金总=("催回本金(总)", "sum"),
        实收总=("实收(总)", "sum")
        )
    gb2.loc[gb2['总天数'] == 0,["新案分案本金","新案回款本金",
                             "新案展期费用","总实收","外呼次数","通时","新人天数","老人天数"]] = None
    mg3 = gb2.merge(df4, on="user_id", how="left").merge(df8,left_on = "leader_user_no",right_on="no",how = "left").merge(data[["组别","账龄"]], left_on="asset_group_name",right_on="组别", how="left")
    dfs = mg3[(mg3["删除时间"] >= datetime.strptime(first_day, '%Y-%m-%d').date()) | mg3["删除时间"].isnull()]

    #主管架构汇总
    gb3=mg1.groupby(["asset_group_name","manager_user_no","manager_user_name","leader_user_no","leader_user_name","user_no","组员","user_id"],as_index= False).agg(
        新案分案本金 = ("分案本金",'sum'),
        新案回款本金 =("回款本金","sum"),
        新案展期费用=("展期费用","sum"),
        总实收=("总实收","sum"),
        外呼次数=("外呼次数","sum"),
        通时=("通时","sum"),
        新人天数 = ("days", lambda x: x[mg1.loc[x.index, "if_new"] == "YES"].sum()),
        老人天数 = ("days", lambda x: x[mg1.loc[x.index, "if_new"] == "NO"].sum()),
        总天数 = ("days","sum"),
        分案本金总=("分案本金(总)", "sum"),
        催回本金总=("催回本金(总)", "sum"),
        实收总=("实收(总)", "sum")
        )
    gb3.loc[gb3['总天数'] == 0,["新案分案本金","新案回款本金",
                             "新案展期费用","总实收","外呼次数","通时","新人天数","老人天数"]] = None
    mg4 = gb3.merge(df4, on="user_id", how="left").merge(df8, left_on="leader_user_no", right_on="no",
                                                         how="left").merge(data[["组别", "账龄"]],
                                                                           left_on="asset_group_name", right_on="组别",
                                                                           how="left")
    dfm = mg4[(mg4["删除时间"] >= datetime.strptime(first_day, '%Y-%m-%d').date()) | mg4["删除时间"].isnull()]
    return pa,dfs,mg1,dfm,gb1

if __name__ == '__main__':
    for i in range(5):
        # 设月初为26号
        # dt(1)
        first_day = "2024-09-01"
        yesterday = "2024-09-30"
        # for i in range(1,15):
        #     yesterday = f"2024-10-{i:02}".format(i)
        current_file_name = os.path.basename(__file__).split(".")[0]
        filename = "{0}{1}.xlsx".format(yesterday, current_file_name)
        n1.dlt("./" + current_file_name, filename)
        # 修改各国新案口径
        新案1 = ['因不可抗力因素温和催收,正常撤案,冻结债务人,冻结,超期案件协商还款',
               '撤销分案-23652,稽核异常撤案,风险上报转客维,稽核投诉倾向撤案,转客维',
               '分案不均,拨打受限组内互换案件,分案前已结清,恢复案件撤案,分案不均补案,存在逾期案件时撤d-2案件,D0撤案,存在逾期案件时撤预提醒案件,存在到期案件时撤预提醒案件',
               """(ml.mission_group_name LIKE '预提醒%' AND ml.assign_asset_late_days = 0)
              OR (ml.mission_group_name LIKE 'S1组%' AND ml.assign_asset_late_days = 1)
              OR (ml.mission_group_name LIKE 'S1-1组%' AND ml.assign_asset_late_days = 1)
              OR (ml.mission_group_name LIKE 'S1-2组%' AND ml.assign_asset_late_days IN (1,3))
              OR (ml.mission_group_name LIKE 'S1-3组%' AND ml.assign_asset_late_days IN (1,5))
              OR (ml.mission_group_name LIKE 'B-1差组%' AND ml.assign_asset_late_days IN (1,8))
              OR (ml.mission_group_name LIKE 'B-1优组%' AND ml.assign_asset_late_days IN (1,8))
              OR (ml.mission_group_name LIKE '鹰潭调解M2%' AND ml.assign_asset_late_days IN (1,31,46))
              OR (ml.mission_group_name LIKE '天津调解M2%' AND ml.assign_asset_late_days IN (1,31,46))
              OR (ml.mission_group_name LIKE '鹰潭调解M3%' AND ml.assign_asset_late_days IN (1,61))
              OR (ml.mission_group_name LIKE '天津调解M3%' AND ml.assign_asset_late_days IN (1,61))
              OR (ml.mission_group_name LIKE 'B-2组%' AND ml.assign_asset_late_days IN (1,16))"""]
        # 获取数据,修改数仓,新案
        数据(sc.ch(), 新案1)
        # 运行过程
        pa, dfs, mg1, dfm, gb1 = 过程()
        rs = n1.renshu(mg1, df4, first_day, yesterday)
        ce, tl, sp = gc1(pa, dfs, dfm, first_day, yesterday, rs, current_file_name)
        top催回率 = n1.top催回率(ce)
        top总催回率_主管 = n1.top总催回率主管(ce)
        top总催回率_业务组 = n1.top总催回率业务组(ce)
        日均分案 = n1.日均分案(gb1)
        ce, resign = n1.rg(ce, yesterday, current_file_name)
        ce.columns = ["组别", "远程/现场", "委外/自营", "帐龄", "主管工号", "主管", "组长工号", "组长", "催员工号", "催员",
                      "催员id", "是否新老", "最早上线日期", "最后工作日", "删除日期", "流失率标记", "实收", "实收排名", "实收排名区间", "分案本金",
                      "回款本金", "催回率", "日均拨次", "日均通时", "上线天数", "新人天数", "老人天数", "是否首次进入组别", "首次进入组别时间",
                      "催回率排名", "催回率排名区间", "日均实收", "日均实收排名", "日均实收排名区间", "综合排名", "综合排名区间", "参与排名业务组人数"
            , "分案本金总", "催回本金总", "实收总"]
        tl.colums = ["组别", "远程/现场", "委外/自营", "账龄", "主管工号", "主管", "组长工号", "组长", "分案本金", "回款本金", "催回率",
                     "催回率排名", "实收", "上线天数", "日均实收", "日均实收排名", "分案本金总", "催回本金总", "实收总", "分案本金>7",
                     "回款本金>7", "催回率>7", "催回率排名>7", "实收>7", "上线天数>7", "日均实收>7", "日均实收排名>7", "总人数", "在职人数",
                     "应出勤人数", "出勤人数", "离职人数", "出勤率", "离职率", ]
        # #保存文件
        path = "./{}".format(current_file_name)
        save("{0}{1}".format(yesterday, current_file_name), path, ce=ce, tl=tl, sp=sp, resign=resign, top催回率=top催回率,
             top总催回率_主管=top总催回率_主管, top总催回率_业务组=top总催回率_业务组, 日均分案=日均分案)
        # 美化文件
        beautify_excel(path + "/{0}{1}.xlsx".format(yesterday, current_file_name))
        # 发送邮件
        df_email = pd.read_csv('./邮箱.txt', sep='=', header=None, encoding='gbk')
        df_email.columns = ['key', 'value']
        email_variable = df_email.loc[df_email['key'] == '邮箱', 'value'].values[0]
        email_password = df_email.loc[df_email['key'] == '密码', 'value'].values[0]
        to_email = df_email.loc[df_email['key'] == '{}收件人邮箱'.format(current_file_name), 'value'].values[0]
        to_email = to_email.split(',')
        # to_email = ["lichongqing@weidu.ac.cn"]
        title = "{0}{1}".format(yesterday, current_file_name)
        mail.send_email(title, path + "/{0}{1}.xlsx".format(yesterday, current_file_name),
                        filename, to_email, email_variable, email_password)