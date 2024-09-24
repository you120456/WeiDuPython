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
