from datetime import date,timedelta,datetime
# import sql
import  pandas as pd
import os
import numpy as np
# first_day = ""
# yesterday = ""
# dfc = pd.read_sql(sql.架构表(first_day, yesterday, ",".join(data["组别"].apply(lambda x: f'"{x}"'))), link)

def renshu(mg1,df4,first_day,yesterday):
    a = mg1.merge(df4,on="user_id",how = "left")
        # .merge(dfc,on="user_id",how = "left")
    b = a[(a["删除时间"] >= datetime.strptime(first_day, '%Y-%m-%d').date()) | a["删除时间"].isnull()]
    b["date(work_day)"] = b["date(work_day)"].astype(str)
    人数 = b.groupby(["asset_group_name","manager_user_no","manager_user_name","leader_user_no","leader_user_name"],as_index = False).agg(
        总人数 = ("user_no","nunique"),
        在职人数=("user_no",lambda x: x[b.loc[x.index,"删除时间"].isnull()].nunique()),
        应出勤人数 = ("预排班状态", lambda x: x[b.loc[x.index, "date(work_day)"] == yesterday].sum()),
        出勤人数 = ("days", lambda x: x[b.loc[x.index, "date(work_day)"] == yesterday].sum()),
        离职人数 = ("user_no",lambda x: x[b.loc[x.index,"删除时间"].notnull()].nunique())
        # 转组人数 = ("user_no", lambda x: x[b["asset_group_name"] != b["上月group"]].nunique())
    # 流失人数=("user_no", lambda x: x[b.loc[x.index, (x["删除时间"].notnull()) & (x["Include loss rate YES/NO"] == "YES")]].nunique())
    )
    人数["出勤率"] = 人数["出勤人数"]/人数["应出勤人数"]
    人数["离职率"] = 人数["离职人数"]/人数["总人数"]
    return 人数
def top催回率(ce):
    ce['排名'] = ce.groupby('Group')['Individual Collection Rate'].rank(ascending=False,method='min')
    ce['催回率排名比'] = ce['排名'] / ce.groupby('Group')['Individual Collection Rate'].transform('count')
    top25催回率 = ce[ce["催回率排名比"]< 0.25].groupby("Group",as_index = False).agg(
        分案本金 = ("Divided principal adjust","sum"),
        回款本金=("Repaid principal adjust", "sum")
    )
    top50催回率 = ce[ce["催回率排名比"]< 0.5].groupby("Group",as_index = False).agg(
        分案本金 = ("Divided principal adjust","sum"),
        回款本金=("Repaid principal adjust", "sum")
    )
    总催回率 = ce.groupby("Group",as_index = False).agg(
        分案本金 = ("Divided principal adjust","sum"),
        回款本金=("Repaid principal adjust", "sum")
    )
    top25催回率['top25催回率'] = top25催回率['回款本金']/top25催回率['分案本金']
    top50催回率['top50催回率'] = top50催回率['回款本金']/top50催回率['分案本金']
    总催回率["总催回率"] =总催回率['回款本金']/总催回率['分案本金']
    top催回率 = 总催回率[["Group",'总催回率']].merge(top25催回率[["Group",'top25催回率']],on = "Group",how='left').merge(top50催回率[["Group",'top50催回率']],on = "Group",how='left')
    ce.drop(["排名","催回率排名比"], axis=1, inplace=True)
    return top催回率
def 离职(yesterday,name):
    # print(yesterday)
    # if yesterday.day == 1:
    if datetime.strptime(yesterday, "%Y-%m-%d").day == 1:
        resign = pd.DataFrame()
        resign["user_id"]=None
    else:
        folder_path = "./"+name
        # 过滤,只保留xlsx文件
        files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
        # 按文件名降序排序
        files.sort(reverse=True)
        # 获取按名称排序后的第一个文件
        try:
            resign = pd.read_excel(folder_path + "/" + files[0], sheet_name="resign")
        except Exception:
            resign = pd.DataFrame()
            resign["user_id"] = None

    return resign
def rg(ce,yesterday,name):
    resign = 离职(yesterday,name)
    rg1 = ce[ce["deletion_tate"].notnull()]
    ce = ce[ce["deletion_tate"].isnull()]  #删除离职的
    resign = rg1.append(resign,ignore_index = True)
    return ce,resign
def dlt(folder_path,file_name):
    file_path = os.path.join(folder_path, file_name)
    # 检查文件是否存在
    if os.path.isfile(file_path):
        os.remove(file_path)  # 删除文件
        print(f"文件 {file_name} 已删除")
    else:
        print(f"文件 {file_name} 不存在")

def top绩效催回率(ce,yesterday):
    ce = ce[ce["Online days"] > 7]
    ce['排名c'] = ce.groupby(["业务组", "Group", "正式主管中文名"])['Individual Collection Rate'].rank(ascending=False, method='min')
    ce['绩效催回率百分比'] = (ce['排名c'] - 1) / (ce.groupby(["业务组", "Group", "正式主管中文名"])['Individual Collection Rate'].transform('count') - 1)
    def top绩效催回率主管(ce):
        top25催回率 = ce[ce["绩效催回率百分比"]< 0.25].groupby(["业务组","Group","正式主管中文名"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        top50催回率 = ce[ce["绩效催回率百分比"]< 0.5].groupby(["业务组","Group","正式主管中文名"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        绩效催回率 = ce.groupby(["业务组","Group","正式主管中文名"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        top25催回率['top25绩效催回率'] = top25催回率['回款本金']/top25催回率['分案本金']
        top50催回率['top50绩效催回率'] = top50催回率['回款本金']/top50催回率['分案本金']
        绩效催回率["绩效催回率"] =绩效催回率['回款本金']/绩效催回率['分案本金']
        绩效催回率["统计日期"]=yesterday
        top催回率 = 绩效催回率[["统计日期","业务组","Group","正式主管中文名",'绩效催回率']].merge(top25催回率[["业务组","Group","正式主管中文名",'top25绩效催回率']],on = ["业务组","Group","正式主管中文名"],how='left').merge(top50催回率[["业务组","Group","正式主管中文名",'top50绩效催回率']],on = ["业务组","Group","正式主管中文名"],how='left')
        top催回率.columns = ["统计日期","业务组","Group","管理层/委外公司",'绩效催回率','top25绩效催回率','top50绩效催回率']
        return top催回率
    def top绩效催回率group(ce):
        top25催回率 = ce[ce["绩效催回率百分比"]< 0.25].groupby(["业务组","Group"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        top50催回率 = ce[ce["绩效催回率百分比"]< 0.5].groupby(["业务组","Group"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        绩效催回率 = ce.groupby(["业务组","Group"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        top25催回率['top25绩效催回率'] = top25催回率['回款本金']/top25催回率['分案本金']
        top50催回率['top50绩效催回率'] = top50催回率['回款本金']/top50催回率['分案本金']
        绩效催回率["绩效催回率"] =绩效催回率['回款本金']/绩效催回率['分案本金']
        绩效催回率["统计日期"] = yesterday
        top催回率 = 绩效催回率[["统计日期","业务组","Group",'绩效催回率']].merge(top25催回率[["业务组","Group",'top25绩效催回率']],on = ["业务组","Group"],how='left').merge(top50催回率[["业务组","Group",'top50绩效催回率']],on = ["业务组","Group"],how='left')
        return top催回率
    def top绩效催回率业务组(ce):
        top25催回率 = ce[ce["绩效催回率百分比"]< 0.25].groupby(["业务组"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        top50催回率 = ce[ce["绩效催回率百分比"]< 0.5].groupby(["业务组"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        绩效催回率 = ce.groupby(["业务组"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        top25催回率['top25催回率'] = top25催回率['回款本金']/top25催回率['分案本金']
        top50催回率['top50催回率'] = top50催回率['回款本金']/top50催回率['分案本金']
        绩效催回率["绩效催回率"] =绩效催回率['回款本金']/绩效催回率['分案本金']
        绩效催回率["统计日期"] = yesterday
        top催回率 = 绩效催回率[["统计日期","业务组",'绩效催回率']].merge(top25催回率[["业务组",'top25催回率']],on = ["业务组"],how='left').merge(top50催回率[["业务组",'top50催回率']],on = ["业务组"],how='left')
        return top催回率
    def top绩效催回率Out(ce):
        top25催回率 = ce[ce["绩效催回率百分比"]< 0.25].groupby(["业务组","Out_self"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        top50催回率 = ce[ce["绩效催回率百分比"]< 0.5].groupby(["业务组","Out_self"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        绩效催回率 = ce.groupby(["业务组","Out_self"],as_index = False).agg(
            分案本金 = ("Divided principal adjust","sum"),
            回款本金=("Repaid principal adjust", "sum")
        )
        top25催回率['top25绩效催回率'] = top25催回率['回款本金']/top25催回率['分案本金']
        top50催回率['top50绩效催回率'] = top50催回率['回款本金']/top50催回率['分案本金']
        绩效催回率["绩效催回率"] =绩效催回率['回款本金']/绩效催回率['分案本金']
        绩效催回率["统计日期"] = yesterday
        top催回率 = 绩效催回率[["统计日期","业务组","Out_self",'绩效催回率']].merge(top25催回率[["业务组","Out_self",'top25绩效催回率']],on = ["业务组","Out_self"],how='left').merge(top50催回率[["业务组","Out_self",'top50绩效催回率']],on = ["业务组","Out_self"],how='left')
        return top催回率
    # ce.drop(["排名c","绩效催回率百分比", "绩效催回率"], axis=1, errors='ignore', inplace=True)
    return top绩效催回率主管(ce),top绩效催回率Out(ce)
def top总催回率(ce,yesterday):
    ce.rename(columns={"asset_group_name": "Group"}, inplace=True)
    # ce = ce[ce["总天数"] != 0]
    ce['总催回率'] = ce['催回本金总'] / ce['分案本金总']
    ce['排名c'] = ce.groupby(["业务组", "Group", "正式主管中文名"])['总催回率'].rank(ascending=False, method='min')
    ce['总催回率百分比'] = (ce['排名c'] - 1) / (ce.groupby(["业务组", "Group", "正式主管中文名"])['总催回率'].transform('count') - 1)
    def top总催回率主管(ce):
        top25催回率 = ce[ce["总催回率百分比"]< 0.25].groupby(["业务组","Group","正式主管中文名"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        top50催回率 = ce[ce["总催回率百分比"]< 0.5].groupby(["业务组","Group","正式主管中文名"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        总催回率 = ce.groupby(["业务组","Group","正式主管中文名"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        top25催回率['top25整体催回率'] = top25催回率['回款本金']/top25催回率['分案本金']
        top50催回率['top50整体催回率'] = top50催回率['回款本金']/top50催回率['分案本金']
        总催回率["整体催回率"] =总催回率['回款本金']/总催回率['分案本金']
        总催回率["统计日期"]=yesterday
        top催回率 = 总催回率[["统计日期","业务组","Group","正式主管中文名",'分案本金','回款本金','整体催回率']].merge(top25催回率[["业务组","Group","正式主管中文名",'top25整体催回率']],on = ["业务组","Group","正式主管中文名"],how='left').merge(top50催回率[["业务组","Group","正式主管中文名",'top50整体催回率']],on = ["业务组","Group","正式主管中文名"],how='left')
        top催回率.columns = ["统计日期","业务组","Group","管理层/委外公司",'分案本金','回款本金','整体催回率','top25整体催回率','top50整体催回率']
        return top催回率
    def top总催回率group(ce):
        top25催回率 = ce[ce["总催回率百分比"]< 0.25].groupby(["业务组","Group"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        top50催回率 = ce[ce["总催回率百分比"]< 0.5].groupby(["业务组","Group"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        总催回率 = ce.groupby(["业务组","Group"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        top25催回率['top25整体催回率'] = top25催回率['回款本金']/top25催回率['分案本金']
        top50催回率['top50整体催回率'] = top50催回率['回款本金']/top50催回率['分案本金']
        总催回率["整体催回率"] =总催回率['回款本金']/总催回率['分案本金']
        总催回率["统计日期"] = yesterday
        top催回率 = 总催回率[["统计日期","业务组","Group",'整体催回率']].merge(top25催回率[["业务组","Group",'top25整体催回率']],on = ["业务组","Group"],how='left').merge(top50催回率[["业务组","Group",'top50整体催回率']],on = ["业务组","Group"],how='left')
        return top催回率
    def top总催回率业务组(ce):
        top25催回率 = ce[ce["总催回率百分比"]< 0.25].groupby(["业务组"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        top50催回率 = ce[ce["总催回率百分比"]< 0.5].groupby(["业务组"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        总催回率 = ce.groupby(["业务组"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        top25催回率['top25整体催回率'] = top25催回率['回款本金']/top25催回率['分案本金']
        top50催回率['top50整体催回率'] = top50催回率['回款本金']/top50催回率['分案本金']
        总催回率["整体催回率"] =总催回率['回款本金']/总催回率['分案本金']
        总催回率["统计日期"]=yesterday
        top催回率 = 总催回率[["统计日期","业务组",'整体催回率']].merge(top25催回率[["业务组",'top25整体催回率']],on = ["业务组"],how='left').merge(top50催回率[["业务组",'top50整体催回率']],on = ["业务组"],how='left')
        top催回率.columns = ["统计日期","业务组",'整体催回率','top25整体催回率','top50整体催回率']
        return top催回率
    def top总催回率Out(ce):
        top25催回率 = ce[ce["总催回率百分比"]< 0.25].groupby(["业务组","Out_self"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        top50催回率 = ce[ce["总催回率百分比"]< 0.5].groupby(["业务组","Out_self"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        总催回率 = ce.groupby(["业务组","Out_self"],as_index = False).agg(
            分案本金 = ("分案本金总","sum"),
            回款本金=("催回本金总", "sum")
        )
        top25催回率['top25整体催回率'] = top25催回率['回款本金']/top25催回率['分案本金']
        top50催回率['top50整体催回率'] = top50催回率['回款本金']/top50催回率['分案本金']
        总催回率["整体催回率"] =总催回率['回款本金']/总催回率['分案本金']
        总催回率["统计日期"] = yesterday
        top催回率 = 总催回率[["统计日期","业务组","Out_self",'整体催回率']].merge(top25催回率[["业务组","Out_self",'top25整体催回率']],on = ["业务组","Out_self"],how='left').merge(top50催回率[["业务组","Out_self",'top50整体催回率']],on = ["业务组","Out_self"],how='left')
        return top催回率
    # ce.drop(["排名c","总催回率百分比", "总催回率"], axis=1, errors='ignore', inplace=True)
    return top总催回率主管(ce),top总催回率Out(ce)
    # return  top总催回率业务组(ce),top总催回率业务组(ce)
def 分案监控(gb1,yesterday):
    gb1 = gb1[gb1["总天数"] != 0]
    gb1["日均分案数"] = gb1["资产数"] / gb1["总天数"]
    gb1["日均分案金额"] = gb1["分案本金"] / gb1["总天数"]
    gb1["每日上线去第一天日均分案数"]= gb1["每日上线去第一天资产数"] / gb1["去第一天上线数"]
    gb1["每日上线去第一天日均分案金额"]= gb1["每日上线去第一天分案本金"] / gb1["去第一天上线数"]
    def 分案监控主管(gb1):
        avecase = gb1.groupby(["业务组","asset_group_name","正式主管中文名"],as_index = False).agg(
            最小日均分案数 = ("日均分案数","min"),
            最大日均分案数 = ("日均分案数","max"),
            平均日均分案数=("日均分案数", "mean"),
            最小日均分案金额 = ("日均分案金额","min"),
            最大日均分案金额=("日均分案金额", "max"),
            平均日均分案金额=("日均分案金额", "mean"),
            每日上线去第一天最小日均分案数=("每日上线去第一天日均分案数", "min"),
            每日上线去第一天最大日均分案数=("每日上线去第一天日均分案数", "max"),
            每日上线去第一天平均日均分案数=("每日上线去第一天日均分案数", "mean"),
            每日上线去第一天最小日均分案金额=("每日上线去第一天日均分案金额", "min"),
            每日上线去第一天最大日均分案金额=("每日上线去第一天日均分案金额", "max"),
            每日上线去第一天平均日均分案金额=("每日上线去第一天日均分案金额", "mean")
        )
        avecase["统计日期"] = yesterday
        avecase["每日上线去第一天催员最大日均分案数-平均"] = avecase["每日上线去第一天最大日均分案数"] - avecase["每日上线去第一天平均日均分案数"]
        avecase["每日上线去第一天催员最小日均分案数-平均"] = avecase["每日上线去第一天最小日均分案数"] - avecase["每日上线去第一天平均日均分案数"]
        avecase["每日上线去第一天催员最大日均分案金额-平均"] = avecase["每日上线去第一天最大日均分案金额"] - avecase["每日上线去第一天平均日均分案金额"]
        avecase["每日上线去第一天催员最小日均分案金额-平均"] = avecase["每日上线去第一天最小日均分案金额"] - avecase["每日上线去第一天平均日均分案金额"]

        avecase["最大日均分案数-平均"] = avecase["最大日均分案数"] - avecase["平均日均分案数"]
        avecase["最小日均分案数-平均"] = avecase["最小日均分案数"] - avecase["平均日均分案数"]
        avecase["最大日均分案金额-平均"] = avecase["最大日均分案金额"] - avecase["平均日均分案金额"]
        avecase["最小日均分案金额-平均"] = avecase["最小日均分案金额"] - avecase["平均日均分案金额"]
        a = avecase[["统计日期","业务组","asset_group_name","正式主管中文名","每日上线去第一天催员最大日均分案数-平均","每日上线去第一天催员最小日均分案数-平均",
                     "每日上线去第一天催员最大日均分案金额-平均","每日上线去第一天催员最小日均分案金额-平均",
                     "最大日均分案数-平均","最小日均分案数-平均",
                    "最大日均分案金额-平均","最小日均分案金额-平均"]]
        a.rename(columns={'正式主管中文名': '管理层/委外公司'}, inplace=True)
        return a

    return 分案监控主管(gb1)
def 承载监控(gb1,yesterday,df13):
    gb1 = gb1[gb1["总天数"] != 0]
    gb1["本日在线在手案件"] = np.where(gb1["本日在线状态"] == 1, gb1["本日在手案件"], None)
    gb1["本日在线在手案件金额"] = np.where(gb1["本日在线状态"] == 1, gb1["本日在手案件金额"], None)
    df13
    def 承载监控主管(gb1,df13):
        avecase = gb1.groupby(["业务组","asset_group_name","正式主管中文名"],as_index = False).agg(
            资产数 = ("资产数","sum"),
            分案本金 = ("分案本金","sum"),
            上线天数 = ("总天数","sum")
            # 本日在手案件 = ("本日在手案件","mean"),
            # 本日在线在手案件 = ("本日在线在手案件","mean")
            # 本日在手案件金额=("本日在手案件金额", "mean")
            # 本日在线在手案件金额=("本日在线在手案件金额", "mean")
        )
        avecase["统计日期"] = yesterday
        avecase["日人均新案分案量"] = avecase["资产数"]/avecase["上线天数"]
        avecase["日人均新案分案金额"] = avecase["分案本金"]/avecase["上线天数"]
        avecase["预估日人均新案分案量"] =avecase.merge(df13[["business_group", "renjun_estimated_daily_reminder_debt"]], left_on=["业务组"],right_on=["business_group"], how="left")["renjun_estimated_daily_reminder_debt"]  ##预估日人均新案分案量 20241105
        #avecase["预估日人均在手案件量"] =avecase.merge(df13[["business_group", "renjun_plan_preman_online_asset"]], left_on=["业务组"],right_on=["business_group"], how="left")["renjun_plan_preman_online_asset"]  ##预估日人均新案分案量 20241105 去掉预估数据20241119
        avecase["本日在线在手案件金额"]=None
        avecase["本日在手案件金额"]=None
        avecase["本日在手案件"]=None
        avecase["本日在线在手案件"]=None
        # a = avecase[["统计日期","业务组","asset_group_name","正式主管中文名","预估日人均新案分案量","日人均新案分案量","日人均新案分案金额","预估日人均在手案件量",
        #              "本日在手案件","本日在线在手案件","本日在手案件金额","本日在线在手案件金额"]]
        a = avecase[["统计日期","业务组","asset_group_name","正式主管中文名","预估日人均新案分案量","日人均新案分案量","日人均新案分案金额"]] ###去掉在手字段 20241119

        a.rename(columns={'正式主管中文名': '管理层'}, inplace=True)
        return a
    def 承载监控组长(gb1):
        avecase = gb1.groupby(["asset_group_name","正式主管中文名","leader_user_name"],as_index = False).agg(
            资产数 = ("资产数","sum"),
            分案本金 = ("分案本金","sum"),
            上线天数 = ("总天数","sum"),
            本日在手案件 = ("本日在手案件","mean"),
            本日在线在手案件 = ("本日在线在手案件","mean"),
            本日在手案件金额=("本日在手案件金额", "mean"),
            本日在线在手案件金额=("本日在线在手案件金额", "mean")
        )
        # a = avecase[["asset_group_name","正式主管中文名","leader_user_name",
        #              "本日在手案件","本日在线在手案件","本日在手案件金额","本日在线在手案件金额"]]
        a = avecase[["asset_group_name", "正式主管中文名", "leader_user_name"]]##去掉在手字段 20241119
        a.rename(columns={'正式主管中文名': '管理层'}, inplace=True)
        return a
    def 承载监控组员(gb1):
        avecase = gb1.groupby(["asset_group_name","正式主管中文名","leader_user_name","组员"],as_index = False).agg(
            资产数 = ("资产数","sum"),
            分案本金 = ("分案本金","sum"),
            上线天数 = ("总天数","sum"),
            本日在手案件 = ("本日在手案件","mean"),
            本日在线在手案件 = ("本日在线在手案件","mean"),
            本日在手案件金额=("本日在手案件金额", "mean"),
            本日在线在手案件金额=("本日在线在手案件金额", "mean")
        )
        # a = avecase[["asset_group_name","正式主管中文名","leader_user_name","组员",
        #              "本日在手案件","本日在线在手案件","本日在手案件金额","本日在线在手案件金额"]]
        a = avecase[["asset_group_name", "正式主管中文名", "leader_user_name", "组员"]]##去掉在手字段 20241119
        a.rename(columns={'正式主管中文名': '管理层'}, inplace=True)
        return a
    return 承载监控主管(gb1,df13)
def 案件量监控(gb1,yesterday,df12):
    gb1 = gb1[gb1["总天数"] != 0]
    case = gb1.groupby(["业务组"], as_index=False).agg(
        资产数=("去第一天资产数", "sum"),
        总天数 = ("总天数","sum")
    )
    day = int(yesterday.split("-")[-1])
    if day > 1:
        case["月累计日均入催量（剔除每月1日）"] = case["资产数"] / (int(day) - 1)
    else: case["月累计日均入催量（剔除每月1日）"] = None
    case["统计日期"] = yesterday
    # case["预估日均入催量"]= None
    case["预估日均入催量"] = case.merge(df12[["group_type", "mean_group_value"]], left_on=["业务组"], right_on=["group_type"], how="left")["mean_group_value"]  ##3修改预估日均入催量 20241105
    # case["月累计日均新案入催量"] = case["新案资产数"]/case["总天数"]
    return case[["统计日期","业务组","预估日均入催量","月累计日均入催量（剔除每月1日）"]]


def 日均分案(gb1):
    avecase = gb1.groupby(["asset_group_name"],as_index = False).agg(
        总资产数 = ("资产数","sum"),
        新案资产数 = ("新案资产数","sum"),
        催回资产数=("催回资产数", "sum"),
        新案催回资产数=("新案催回资产数", "sum"),
        新案分案本金=("新案分案本金", "sum"),
        新案回款本金=("新案回款本金", "sum"),
        总实收=("总实收", "sum"),
        总天数 = ("总天数","sum"),
        分案本金=("分案本金","sum"),
        回款本金=("回款本金","sum"),
        债务人数=("债务人数","sum"),
        新案债务人数=("新案债务人数","sum"),
        催回债务人数=("催回债务人数","sum"),
        新案催回债务人数=("新案催回债务人数","sum")
    )
    avecase["日人均资产数"] = round(avecase["总资产数"]/avecase["总天数"],2)
    avecase["日人均催回资产数"] = round(avecase["催回资产数"] / avecase["总天数"], 2)
    avecase["日人均分案本金"] = round(avecase["分案本金"] / avecase["总天数"], 2)
    avecase["日人均回款本金"] = round(avecase["回款本金"] / avecase["总天数"], 2)

    avecase["日人均新案资产数"] = round(avecase["新案资产数"]/avecase["总天数"],2)
    avecase["日人均新案催回资产数"] = round(avecase["新案催回资产数"] / avecase["总天数"], 2)
    avecase["日人均新案分案本金"] = round(avecase["新案分案本金"] / avecase["总天数"], 2)
    avecase["日人均新案回款本金"] = round(avecase["新案回款本金"] / avecase["总天数"], 2)

    avecase["日人均债务人数"] = round(avecase["债务人数"]/avecase["总天数"],2)
    avecase["日人均催回债务人数"] = round(avecase["催回债务人数"] / avecase["总天数"], 2)
    avecase["日人均新案债务人数"] = round(avecase["新案债务人数"] / avecase["总天数"], 2)
    avecase["日人均新案催催回债务人数"] = round(avecase["新案催回债务人数"] / avecase["总天数"], 2)

    return avecase[["asset_group_name","总资产数","总天数","日人均资产数","日人均催回资产数","日人均分案本金","日人均回款本金",
                    "日人均新案资产数","日人均新案催回资产数","日人均新案分案本金","日人均新案回款本金"
        ,"日人均债务人数","日人均催回债务人数","日人均新案债务人数","日人均新案催催回债务人数"
                    ]]


def 添加组长在手(a,组长g):
    a = a.merge(组长g,left_on = ["队列","管理层","组长"],right_on=["asset_group_name","管理层","leader_user_name"],how="left")
    a.fillna(0,inplace=True)
    a = a[["统计日期","业务组","队列","管理层","组长","排班天数","出勤天数","出勤率","日人均新案量",
           "本日在手案件","本日在线在手案件","本日在手案件金额","本日在线在手案件金额",
           "日人均拨打案件数","日人均拨打次数","日人均通话时长","日人均接通次数","日人均WA发送案件数",
           "日人均WA发送次数","日人均短信发送案件数","日人均短信发送条数"]]
    return a
def 添加组员在手(a,组员g):
    a = a.merge(组员g,left_on = ["队列","管理层","组长","催员name"],right_on=["asset_group_name","管理层","leader_user_name","组员"],how="left")
    a.fillna(0,inplace=True)
    a = a["统计日期", "业务组", "队列", "管理层", "组长", "催员name","上线率", "日均新案量", "日均新案金额",
          "本日在手案件", "本日在线在手案件", "本日在手案件金额", "本日在线在手案件金额",
          "日均拨打案件数", "日均拨打次数", "日均通话时长", "日均接通次数", "日均短信发送案件数", "日均短信发送条数"
        "日人均WA发送次数","日人均短信发送案件数","日人均短信发送条数"]
    return a
