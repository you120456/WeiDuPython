# 这是一个示例 Python 脚本。

# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
import pandas as pd
import sys
import sc
from 各国业务组 import 业务组
from datetime import date,timedelta,datetime
from dateutil.relativedelta import relativedelta
import sql
from saveexcel import save

def dt(x):
    global first_day, yesterday
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
dt(26)
data= 业务组(sc.bjst())
#数据
df1 = pd.read_sql(sql.架构表(first_day,yesterday,",".join(data["组别"].apply(lambda x: f'"{x}"'))),sc.bjst())
新案1 =['因不可抗力因素温和催收,正常撤案,冻结债务人,冻结',
    '撤销分案-23652,稽核异常撤案,风险上报转客维,稽核投诉倾向撤案,转客维',
    '分案不均,拨打受限组内互换案件,分案前已结清,恢复案件撤案,分案不均补案,存在逾期案件时撤d-2案件',
    """(ml.mission_group_name LIKE 'Pre%' AND ml.assign_asset_late_days = -2)
    OR (ml.mission_group_name LIKE 'A%' AND ml.assign_asset_late_days = 1)
    OR (ml.mission_group_name LIKE 'B1%' AND ml.assign_asset_late_days IN (1,8))
    OR (ml.mission_group_name LIKE 'B2%' AND ml.assign_asset_late_days IN (1,15))"""]
df2 = pd.read_sql(sql.分案回款(新案1[0],新案1[1],新案1[2],first_day +" 06:00:00",yesterday+" 23:59:59",新案1[3]),sc.bjst())
df3 = pd.read_sql(sql.新老天数(first_day,yesterday),sc.bjst())
df4 = pd.read_sql(sql.离职时间(),sc.bjst())
df5 = pd.read_sql(sql.最早上线日期(),sc.bjst())
df6 = pd.read_sql(sql.外呼(first_day,yesterday),sc.bjst())
df7 = pd.read_sql(sql.晋升时间(),sc.bjst())
df8 = pd.read_sql(sql.usertable(),sc.bjst())
#每日明细
mg1 = (df1.merge(df2,left_on = ["pt_date","user_id"],right_on=["分案日期","催员ID"],how="left")
    .merge(df3,left_on = ["pt_date","user_id"],right_on=["date(work_day)","user_id"],how="left")
       .merge(df6,left_on = ["pt_date","user_id"],right_on=["日期","id"],how="left"))
mg1.sort_values(["user_id","pt_date"],ascending=[False,True],inplace=True)
#个人最新架构汇总
gb1=mg1.groupby(["asset_group_name","组员","user_id"],as_index= False).agg(
    manager_user_name = ("manager_user_name",lambda x: x.iloc[-1]),
    leader_user_name = ("leader_user_name",lambda x: x.iloc[-1]),
    新案分案本金 = ("新案分案本金",'sum'),
    新案回款本金 =("新案回款本金","sum"),
    新案展期费用=("新案展期费用","sum"),
    总实收=("总实收","sum"),
    外呼次数=("外呼次数","sum"),
    通时=("通时","sum"),
    新人天数 = ("days", lambda x: x.sum() if (df3["if_new"] == "YES").any() else 0),
    老人天数 = ("days", lambda x: x.sum() if (df3["if_new"] == "NO").any() else 0),
    总天数 = ("days","sum")
    )
#个人每日架构汇总
gb2=mg1.groupby(["asset_group_name","manager_user_name","leader_user_name","组员","user_id"],as_index= False).agg(
    新案分案本金 = ("新案分案本金",'sum'),
    新案回款本金 =("新案回款本金","sum"),
    新案展期费用=("新案展期费用","sum"),
    总实收=("总实收","sum"),
    外呼次数=("外呼次数","sum"),
    通时=("通时","sum"),
    新人天数 = ("days", lambda x: x.sum() if (df3["if_new"] == "YES").any() else 0),
    老人天数 = ("days", lambda x: x.sum() if (df3["if_new"] == "NO").any() else 0),
    总天数 = ("days","sum")
    )
#合并其它数据,剔除本月之前离职的
mg2 = gb1.merge(df4,on="user_id",how = "left").merge(df5,on="user_id",how = "left").merge(df7,on=["user_id","asset_group_name"],how = "left").merge(df8,on=["user_id"],how = "left")
mg2.loc[~mg2["asset_group_name"].isin(["B1 Group", "B2 Group","M2 Group","B Group"]), "首次晋升队列日期"] = None
pa = mg2[(mg2["删除时间"]>=datetime.strptime(first_day,'%Y-%m-%d').date()) | mg2["删除时间"].isnull()]

#计算字段
pa["Repaid principal adjust"]=pa['新案回款本金']+pa['新案展期费用']
pa["Individual Collection Rate"]=(pa['新案回款本金']+pa['新案展期费用'])/pa['新案分案本金']
pa["Newly enrolled Yes/ No"]= (date.today() - timedelta(days=1)-pa["min_date"]).dt.days.apply(lambda x: 'YES' if x < 30 else 'NO')
pa["Gross collectiion ranking"]=pa.groupby(["asset_group_name"])["总实收"].rank(ascending=False,method='first')
pa["Average daily number of calls"]=pa["外呼次数"]/pa["总天数"]
pa["Average daily talk time"]=pa["通时"]/pa["总天数"]/60
pa["To Group First"]=pa["首次晋升队列日期"].apply(lambda x: "YES" if pd.notna(x) and x >= pd.to_datetime(first_day) else ("NO" if pd.notna(x) else None))
pa["催回率排名"] = pa.groupby("asset_group_name")["Individual Collection Rate"].rank(ascending=False, method='first')
pa["日均实收"] = pa["总实收"]/pa["总天数"]
pa.loc[(pa["总实收"] == 0)|(pa["总天数"] == 0),"日均实收"] = None
pa["日均实收排名"]=pa.groupby("asset_group_name")["日均实收"].rank(ascending=False,method='first')
pa["Integrated Ranking"] = (pa["催回率排名"]*0.5+pa["日均实收排名"]*0.5)
pa["综合排名序列"] = pa.groupby("asset_group_name")["Integrated Ranking"].rank(ascending=True, method='first')
# 实收区间
def calculate_range(row,df):
    max_m = df[df['asset_group_name'] == row['asset_group_name']]["Gross collectiion ranking"].max()
    if row["Gross collectiion ranking"] <= round(max_m * 0.05):
        return "Top5%"
    elif row["Gross collectiion ranking"] <= round(max_m * 0.25):
        return "5%-25%"
    elif row["Gross collectiion ranking"] <= round(max_m * 0.5):
        return "25%-50%"
    elif row["Gross collectiion ranking"] <= round(max_m * 0.7):
        return "50%-70%"
    elif row["Gross collectiion ranking"] <= round(max_m * 0.9):
        return "70%-90%"
    else:
        return "bottom10%"
pa["Ranking intervals"] = pa.apply(lambda  x: calculate_range(x,pa), axis=1)
#综合排名区间
def calculate_range1(row,df):
    max_m = df[df['asset_group_name'] == row['asset_group_name']]["综合排名序列"].max()
    if row["综合排名序列"] <= round(max_m * 0.2):
        return "Top20%"
    elif row["综合排名序列"] <= round(max_m * 0.3):
        return "20%-30%"
    elif row["综合排名序列"] <= round(max_m * 0.5):
        return "30%-50%"
    elif row["综合排名序列"] <= round(max_m * 0.7):
        return "50%-70%"
    elif row["综合排名序列"] <= round(max_m * 0.9):
        return "70%-90%"
    else:
        return "bottom10%"
pa["Integrated Ranking interval"] = pa.apply(lambda  x: calculate_range1(x,pa), axis=1)

#大于7天重新排名
pa_filtered = pa[pa['总天数'] > 7]
try:
    pa_filtered['Gross collectiion ranking'] = pa_filtered.groupby(["asset_group_name"])["总实收"].rank(
        ascending=False, method='first')
    pa_filtered["Ranking intervals"] = pa_filtered.apply(lambda  x: calculate_range(x,pa), axis=1)
    pa_filtered["催回率排名"] = pa_filtered.groupby("asset_group_name")["Individual Collection Rate"].rank(ascending=False, method='first')
    pa_filtered["日均实收排名"]=pa_filtered.groupby("asset_group_name")["日均实收"].rank(ascending=False,method='first')
    pa_filtered["Integrated Ranking"] = (pa_filtered["日均实收排名"]*0.5+pa_filtered["催回率排名"]*0.5)
    pa_filtered["综合排名序列"] = pa_filtered.groupby("asset_group_name")["Integrated Ranking"].rank(ascending=True, method='first')
    pa_filtered["Integrated Ranking interval"] = pa_filtered.apply(lambda  x: calculate_range1(x,pa), axis=1)
    pa.loc[pa_filtered.index, ['Gross collectiion ranking', 'Ranking intervals', 'Integrated Ranking',
                                 'Integrated Ranking interval','催回率排名','日均实收排名']] = \
        pa_filtered[['Gross collectiion ranking', 'Ranking intervals', 'Integrated Ranking', 'Integrated Ranking interval','催回率排名','日均实收排名']]
except Exception as e:
    pass

# ce数据
ce = pa[["asset_group_name","manager_user_name","leader_user_name","no","组员","Newly enrolled Yes/ No",
"min_date","最后工作日","删除时间","总实收","Gross collectiion ranking","Ranking intervals","新案分案本金","Repaid principal adjust",
    "Individual Collection Rate","Average daily number of calls","Average daily talk time","总天数",
     "新人天数","老人天数","To Group First","首次晋升队列日期",'催回率排名','日均实收排名','Integrated Ranking','Integrated Ranking interval']]

# tl
gb2["Repaid principal adjust"]=gb2['新案回款本金']+gb2['新案展期费用']
mg3 = gb2.merge(df4,on="user_id",how = "left").merge(df8,left_on=["leader_user_name"],right_on=["name"],how = "left")
dfs = mg3[(mg3["删除时间"]>=datetime.strptime(first_day,'%Y-%m-%d').date()) | mg3["删除时间"].isnull()]
dfr= dfs.loc[dfs["总天数"]>7,:]

g1 = dfs.groupby(["asset_group_name","no","manager_user_name","leader_user_name"],as_index=False)["新案分案本金","Repaid principal adjust","总实收","总天数"].sum()
g2 = dfr.groupby(["asset_group_name","manager_user_name","leader_user_name"],as_index=False)["新案分案本金","Repaid principal adjust","总实收","总天数"].sum()
g1["Team Collection Rate1"] = g1["Repaid principal adjust"]/g1["新案分案本金"]
g1["Rank1"] = g1.groupby("asset_group_name")["Team Collection Rate1"].rank(ascending=False, method='first')
g2["Team Collection Rate2(Online days >7)"] = g2["Repaid principal adjust"]/g2["新案分案本金"]
g2["Rank2"] = g2.groupby("asset_group_name")["Team Collection Rate2(Online days >7)"].rank(ascending=False, method='first')
g2["Ave.Gross collection"] = g2["总实收"]/g2["总天数"]
g2["Rank3"] = g2.groupby("asset_group_name")["Ave.Gross collection"].rank(ascending=False, method='first')

tl = g1[['asset_group_name', 'manager_user_name','no','leader_user_name','Team Collection Rate1',
       'Rank1']].merge(g2[['asset_group_name', 'manager_user_name', 'leader_user_name','Team Collection Rate2(Online days >7)', 'Rank2',
       'Ave.Gross collection', 'Rank3']],on=['asset_group_name', 'manager_user_name', 'leader_user_name'],how="left")

#sp

m1 = dfs.groupby(["asset_group_name","manager_user_name"],as_index=False)["新案分案本金","Repaid principal adjust","总天数"].sum()
m2 = dfr.groupby(["asset_group_name","manager_user_name"],as_index=False)["新案分案本金","Repaid principal adjust","总天数"].sum()
m1["Team Collection Rate1"] = m1["Repaid principal adjust"]/m1["新案分案本金"]
m2["Team Collection Rate2"] = m2["Repaid principal adjust"]/m2["新案分案本金"]


sp = m1[['asset_group_name', 'manager_user_name','Team Collection Rate1']].merge(m2,on=['asset_group_name', 'manager_user_name'],how = "left")
sp=sp[["asset_group_name","manager_user_name","Team Collection Rate1",
       "Team Collection Rate2","新案分案本金","Repaid principal adjust","总天数"]]




if __name__ == '__main__':
    save("巴基斯坦", ".", ce=ce, tl=tl, sp=sp)

