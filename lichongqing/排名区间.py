import pandas as pd

def calculate_range(row, df, rank_key):
    max_m = df[df["asset_group_name"] == row["asset_group_name"]][rank_key].max()
    if pd.isna(max_m):
        return None
    if row[rank_key] <= round(max_m * 0.05):
        return "Top5%"
    if row[rank_key] <= round(max_m * 0.25):
        return "5%-25%"
    if row[rank_key] <= round(max_m * 0.5):
        return "25%-50%"
    if row[rank_key] <= round(max_m * 0.7):
        return "50%-70%"
    if row[rank_key] <= round(max_m * 0.9):
        return "70%-90%"
    return "bottom10%"


def calculate_range1(row, df):
    max_m = df[df["asset_group_name"] == row["asset_group_name"]]["综合排名序列"].max()
    if pd.isna(max_m):
        return None
    if row["综合排名序列"] <= round(max_m * 0.2):
        return "Top20%"
    if row["综合排名序列"] <= round(max_m * 0.3):
        return "20%-30%"
    if row["综合排名序列"] <= round(max_m * 0.5):
        return "30%-50%"
    if row["综合排名序列"] <= round(max_m * 0.7):
        return "50%-70%"
    if row["综合排名序列"] <= round(max_m * 0.9):
        return "70%-90%"
    return "bottom10%"



def mxg实收区间(row,df,rank_key):
    max_m = df[df["asset_group_name"] == row["asset_group_name"]][rank_key].max()
    if pd.isna(max_m):
        return None
    if row[rank_key] <= round(max_m * 0.2):
        return "Top20%"
    if row[rank_key] <= round(max_m * 0.5):
        return "20%-50%"
    if row[rank_key] <= round(max_m * 0.7):
        return "50%-70%"
    else:return "bottom30%"

def mxg催回率(row,df,rank_key):
    max_m = df[df["asset_group_name"] == row["asset_group_name"]][rank_key].max()
    if pd.isna(max_m):
        return None
    if row[rank_key] <= round(max_m * 0.5):
        return "Top50%"
    else: return "50%-100%%"

def flb区间(row,df,rank_key):
    max_m = df[df["asset_group_name"] == row["asset_group_name"]][rank_key].max()
    if pd.isna(max_m):
        return None
    for i in range(1, 21):  # 从1到20
        if row[rank_key] <= round(max_m * (i / 20)):
            return f"{(i - 1) * 5}%-{i * 5}%"
    else: return None

def tg催回率(row, df, rank_key):
    max_m = df[df["asset_group_name"] == row["asset_group_name"]][rank_key].max()
    if pd.isna(max_m):
        return None
    if row[rank_key] <= round(max_m * 0.2):
        return "Top20%"
    elif row[rank_key] <= round(max_m * 0.5):
        return "20%-50%"
    else:
        return "50%-100%"

def tg实收区间(row, df, rank_key):
    max_m = df[df["asset_group_name"] == row["asset_group_name"]][rank_key].max()
    if pd.isna(max_m):
        return None
    if row[rank_key] <= round(max_m * 0.1):
        return "Top10%"
    elif row[rank_key] <= round(max_m * 0.5):
        return "10%-50%"
    elif row[rank_key] <= round(max_m * 0.7):
        return "50%-70%"
    else:
        return "bottom30%"

def 添加主管(first_day):
    sql = """
	select 正式主管,储备主管,正式主管中文名
    from fox_tmp.主管对应关系
    where 1=1 
    and 年 = year('{0}')
	and 月 = MONTH('{0}')
-- 		AND 年 = 2024
-- 		AND 月 = 10
    """.format(first_day)
    return sql
def 添加业务组(first_day):
    sql = """
	select 队列,业务组,账龄,新案
    from fox_tmp.组别信息
    where 1=1 
-- 		AND 年 = 2024
-- 		AND 月 = 10
    and 年 = year('{0}')
	and 月 = MONTH('{0}')
    """.format(first_day)
    return sql