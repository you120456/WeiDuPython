import pandas as pd
def 中国帐龄实收(mg2):
    a = mg2[["asset_group_name","正式主管",	"正式主管中文名",	"manager_user_name","manager_user_no","leader_user_name","leader_user_no"
        ,"user_no","组员","大于61实收","小于61实收","D1分案本金","D1回款本金","D4分案本金","D4回款本金"]]
    a = a.query('asset_group_name in ["M2-TJ", "M2-YT", "A-1", "A-2"]')
    a.loc[a['asset_group_name'].isin(["M2-TJ", "M2-YT"]), ["D1分案本金", "D1回款本金", "D4分案本金", "D4回款本金"]] = None
    a.loc[a['asset_group_name'].isin(["A-1", "A-2"]), ["大于61实收", "小于61实收"]] = None
    return a