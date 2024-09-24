# decompyle3 version 3.9.2
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.11.4 (tags/v3.11.4:d2340ef, Jun  7 2023, 05:45:37) [MSC v.1934 64 bit (AMD64)]
# Embedded file name: E:\WeiDuPython\lichongqing\通用过程.py
# Compiled at: 2024-08-29 16:00:27
# Size of source mod 2**32: 9846 bytes
import numpy as np, pandas as pd
from datetime import date, timedelta, datetime
from 排名区间 import calculate_range, calculate_range1

def gc1(pa, dfs,dfm,first_day,rs):
    pa["Out/self"] = pa["asset_group_name"].apply(lambda x: "委外" if "os" in x.lower() else "自营")
    pa["Repaid principal adjust"] = pa["新案回款本金"] + pa["新案展期费用"]
    pa["Individual Collection Rate"] = (pa["新案回款本金"] + pa["新案展期费用"]) / pa["新案分案本金"]
    pa["Newly enrolled Yes/ No"] = (date.today() - timedelta(days=1) - pa["min_date"]).dt.days.apply(lambda x: "YES" if x < 30 else "NO")
    pa["Gross collectiion ranking"] = pa.groupby(["asset_group_name"])["总实收"].rank(ascending=False, method="first")
    pa["Average daily number of calls"] = pa["外呼次数"] / pa["总天数"]
    pa["Average daily talk time"] = pa["通时"] / pa["总天数"] / 60
    pa["To Group First"] = pa["首次晋升队列日期"].apply(lambda x: "YES" if pd.notna(x) and x >= pd.to_datetime(first_day) else "NO" if pd.notna(x) else None)
    pa["催回率排名"] = pa.groupby("asset_group_name")["Individual Collection Rate"].rank(ascending=False, method="first")
    pa["日均实收"] = pa["总实收"] / pa["总天数"]
    pa.loc[((pa["总实收"] == 0) | (pa["总天数"] == 0), "日均实收")] = None
    pa["日均实收排名"] = pa.groupby("asset_group_name")["日均实收"].rank(ascending=False, method="first")
    pa["Integrated Ranking"] = pa["催回率排名"] * 0.5 + pa["日均实收排名"] * 0.5
    pa["综合排名序列"] = pa.groupby("asset_group_name")["Integrated Ranking"].rank(ascending=True, method="first")
    pa["Ranking intervals"] = pa.apply((lambda x: calculate_range(x, pa, "Gross collectiion ranking")), axis=1)
    pa["Integrated Ranking interval"] = pa.apply((lambda x: calculate_range1(x, pa)), axis=1)

    pa["日均实收排名区间"] = pa.apply((lambda x: calculate_range(x, pa,"日均实收排名")), axis=1)
    pa["催回率排名区间"] = pa.apply((lambda x: calculate_range(x, pa, "催回率排名")), axis=1)
    pa_filtered = pa[pa["总天数"] > 7]
    try:
        pa_filtered["Gross collectiion ranking"] = pa_filtered.groupby(["asset_group_name"])["总实收"].rank(ascending=False,
          method="first")
        pa_filtered["Ranking intervals"] = pa_filtered.apply((lambda x: calculate_range(x, pa_filtered, "Gross collectiion ranking")),
          axis=1)
        pa_filtered["催回率排名"] = pa_filtered.groupby("asset_group_name")["Individual Collection Rate"].rank(ascending=False,
          method="first")
        pa_filtered["催回率排名区间"] = pa_filtered.apply((lambda x: calculate_range(x, pa, "催回率排名")), axis=1)
        pa_filtered["日均实收排名"] = pa_filtered.groupby("asset_group_name")["日均实收"].rank(ascending=False, method="first")
        pa_filtered["日均实收排名区间"] = pa_filtered.apply((lambda x: calculate_range(x, pa,"日均实收排名")), axis=1)
        pa_filtered["Integrated Ranking"] = pa_filtered["日均实收排名"] * 0.5 + pa_filtered["催回率排名"] * 0.5
        pa_filtered["综合排名序列"] = pa_filtered.groupby("asset_group_name")["Integrated Ranking"].rank(ascending=True, method="first")
        pa_filtered["Integrated Ranking interval"] = pa_filtered.apply((lambda x: calculate_range1(x, pa_filtered)), axis=1)
        pa.loc[(pa_filtered.index, ["Gross collectiion ranking","Ranking intervals","Integrated Ranking",
                                    "Integrated Ranking interval","催回率排名","催回率排名区间","日均实收排名",
                                    "日均实收排名区间"])] = pa_filtered[[
         "Gross collectiion ranking","Ranking intervals","Integrated Ranking","Integrated Ranking interval",
         "催回率排名","催回率排名区间","日均实收排名","日均实收排名区间"]]
    except Exception as e:
        try:
            pass
        finally:
            e = None
            del e

    ce = pa[[
     "asset_group_name","office_name","Out/self","账龄","manager_user_no","manager_user_name","leader_user_no","leader_user_name","user_no",
     "组员","user_id","Newly enrolled Yes/ No",
     "min_date","最后工作日","删除时间","Include loss rate YES/NO","总实收","Gross collectiion ranking","Ranking intervals","新案分案本金",
     "Repaid principal adjust",
     "Individual Collection Rate","Average daily number of calls","Average daily talk time","总天数",
     "新人天数","老人天数","To Group First","首次晋升队列日期","催回率排名","催回率排名区间","日均实收","日均实收排名","日均实收排名区间","Integrated Ranking",
     "Integrated Ranking interval"]]
    ce.columns = [
     "Group","office_name","Out/self","账龄","Supervisor_no","Supervisor","leader_user_no","Team Leader","user_no",
     "Collection Executive","user_id",
     "Newly enrolled Yes/ No","Min Online Date","dimission_date","deletion_tate","Include loss rate YES/NO","Gross collection",
     "Gross collectiion ranking","Ranking intervals","Divided principal adjust",
     "Repaid principal adjust",
     "Individual Collection Rate","Average daily number of calls","Average daily talk time",
     "Online days","Number of days between rookies",
     "Number of days in a period of old age","To Group First","To Group date","Ranking of recall rates",
     "Ranking intervals of recall rates","average daily receipts",
     "Ranking of average daily receipts","Ranking intervals of average daily receipts",
     "Integrated Ranking","Integrated Ranking interval"]
    ce.replace([np.inf, -np.inf], (np.nan), inplace=True)

    #组长
    dfs["Repaid principal adjust"] = dfs["新案回款本金"] + dfs["新案展期费用"]
    dfr = dfs.loc[dfs["总天数"] > 7, :]
    g1 = dfs.groupby(["asset_group_name","office_name","账龄","manager_user_no","manager_user_name","leader_user_no","leader_user_name"], as_index=False)[('新案分案本金',
                                                                                                                                      'Repaid principal adjust',
                                                                                                                                      '总实收',
                                                                                                                                      '总天数')].sum()
    g2 = dfr.groupby(["asset_group_name","manager_user_no","manager_user_name","leader_user_no","leader_user_name"], as_index=False)[('新案分案本金',
                                                                                                                                      'Repaid principal adjust',
                                                                                                                                      '总实收',
                                                                                                                                      '总天数')].sum()
    g1["Team Collection Rate1"] = g1["Repaid principal adjust"] / g1["新案分案本金"]
    g1["Rank1"] = g1.groupby("asset_group_name")["Team Collection Rate1"].rank(ascending=False, method="first")
    g1["Ave.Gross collection"] = g1["总实收"] / g1["总天数"]
    g1["Rank2"] = g1.groupby("asset_group_name")["Ave.Gross collection"].rank(ascending=False, method="first")
    g1["Out/self"] = g1["asset_group_name"].apply(lambda x: "委外" if "os" in x.lower() else "自营")

    g2["Team Collection Rate2(Online days >7)"] = g2["Repaid principal adjust"] / g2["新案分案本金"]
    g2["Rank3"] = g2.groupby("asset_group_name")["Team Collection Rate2(Online days >7)"].rank(ascending=False, method="first")
    g2["Ave.Gross collection(Online days >7)"] = g2["总实收"] / g2["总天数"]
    g2["Rank4"] = g2.groupby("asset_group_name")["Ave.Gross collection(Online days >7)"].rank(ascending=False, method="first")
    tl = g1[["asset_group_name","office_name","Out/self","账龄","manager_user_no","manager_user_name","leader_user_no","leader_user_name",
     "新案分案本金","Repaid principal adjust","Team Collection Rate1",
     "Rank1","总实收","总天数","Ave.Gross collection","Rank2"]].merge((g2[["asset_group_name","manager_user_no","manager_user_name",
     "leader_user_no","leader_user_name","新案分案本金","Repaid principal adjust",
     "Team Collection Rate2(Online days >7)","Rank3",
     "总实收", "总天数",
     "Ave.Gross collection(Online days >7)","Rank4"]]),
      on=[
     "asset_group_name","manager_user_no","manager_user_name",
     "leader_user_no","leader_user_name"],
      how="left").merge(rs,on = ["asset_group_name","manager_user_no","manager_user_name","leader_user_no","leader_user_name"])
    tl.columns = ["Group","office_name","Out/self","账龄","Supervisor_no","Supervisor","leader_user_no","Team Leader","Divided principal adjust",
                  "Repaid principal adjust","Team Collection Rate1",
                  "Rank1","Gross collection","Online days","Ave.Gross collection","Rank2",
                  "Divided principal adjust(Online days >7)","Repaid principal adjust(Online days >7)",
                  "Team Collection Rate2(Online days >7)","Rank3",
                  "Gross collection(Online days >7)", "Online days(Online days >7)",
                  "Ave.Gross collection(Online days >7)","Rank4","总人数","在职人数","应出勤人数","出勤人数","离职人数","出勤率","离职率"]


    #主管
    dfm["Repaid principal adjust"] = dfm["新案回款本金"] + dfm["新案展期费用"]
    dfk = dfm.loc[dfm["总天数"] > 7, :]
    m1 = dfm.groupby(["asset_group_name", "manager_user_no", "manager_user_name"], as_index=False)[('新案分案本金',
                                                                                                    'Repaid principal adjust',
                                                                                                    '总天数')].sum()
    m2 = dfk.groupby(["asset_group_name", "manager_user_no", "manager_user_name"], as_index=False)[('新案分案本金',
                                                                                                    'Repaid principal adjust',
                                                                                                    '总天数')].sum()
    m1["Team Collection Rate1"] = m1["Repaid principal adjust"] / m1["新案分案本金"]
    m2["Team Collection Rate2"] = m2["Repaid principal adjust"] / m2["新案分案本金"]
    m1["Out/self"] = m1["asset_group_name"].apply(lambda x: "委外" if "os" in x.lower() else "自营")

    sp = m1[["asset_group_name","Out/self","manager_user_no", "manager_user_name", "Team Collection Rate1"]].merge(m2, on=[
     "asset_group_name", "manager_user_no", "manager_user_name"],
      how="left")
    sp = sp[["asset_group_name","Out/self","manager_user_no","manager_user_name","Team Collection Rate1",
     "Team Collection Rate2","新案分案本金","Repaid principal adjust","总天数"]]

    sp.columns = ["Group","Out/self","manager_user_no","Supervisor","Team Collection Rate1","Team Collection Rate2 >7",
     "Divided principal adjust >7",
     "Repaid principal adjust >7","Online days >7"]
    return ce, tl, sp