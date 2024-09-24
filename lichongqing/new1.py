from datetime import date,timedelta,datetime
# import sql
import  pandas as pd
import os
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
    top25催回率 = ce[ce["Ranking intervals of recall rates"].isin(["Top5%", "5%-25%"])].groupby("Group",as_index = False).agg(
        分案本金 = ("Divided principal adjust","sum"),
        回款本金=("Repaid principal adjust", "sum")
    )
    top50催回率 = ce[ce["Ranking intervals of recall rates"].isin(["Top5%", "5%-25%","25%-50%"])].groupby("Group",as_index = False).agg(
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
    top催回率 = 总催回率[["Group",'总催回率']].merge(top25催回率[["Group",'top25催回率']],on = "Group").merge(top50催回率[["Group",'top50催回率']],on = "Group")
    return top催回率
def 离职(today,name):
    if today.day == 1:
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
            resign = pd.read_excel(folder_path+"/"+files[0],sheet_name="resign")
        except ValueError:
            resign = pd.DataFrame()
            resign["user_id"] = None
    return resign
def rg(ce,today,name):
    resign = 离职(today,name)
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
