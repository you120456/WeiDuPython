import sc
import pandas as pd
import json
def 业务组(conn_ods):
    业务组sql="""
    select keyvalue_value from ods_fox_keyvalue where keyvalue_key='collect_report_config'
    """
    df = pd.read_sql(业务组sql,conn_ods)
    data=pd.DataFrame()
    df_json = pd.json_normalize(df['keyvalue_value'].apply(json.loads))
    for i in range(df_json.shape[1]):
        data = pd.concat([data, pd.json_normalize(df_json[i])[
            ["assetGroupName", "groupNewAssetDays", "startOverdueDays", "endOverdueDays","delFlag"]]])

    data.columns = ["组别", "newday", "startOverdueDays", "endOverdueDays","delFlag"]
    data = data[data["delFlag"] == True]
    data["账龄"] = data["startOverdueDays"].astype(str) + "-" + data["endOverdueDays"].astype(str)
    data.reset_index(inplace=True)
    return data
# print(业务组(sc.mxg()).to_excel("./ada.xlsx"))