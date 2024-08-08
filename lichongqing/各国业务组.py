# import sc
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
        data=pd.concat([data,pd.json_normalize(df_json[i])[["assetGroupName","groupNewAssetDays"]]])
    data.columns=["组别","newday"]
    return data
# 业务组(sc.bjst())