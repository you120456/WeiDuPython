import pandas as pd

def get_top_整体_data(员工数据_result):
    # Top25%排名放款率员工
    排名前25员工数据 = 员工数据_result[员工数据_result['申完率排名区间or放款率排名区间'].isin(['Top5%','5%-25%'])][['group','机构属性','work_set_up','director','user_id','assign_sum','申完量or放款量','申完率排名区间or放款率排名区间']]
    # Top50%排名放款率员工
    排名前50员工数据 = 员工数据_result[员工数据_result['申完率排名区间or放款率排名区间'].isin(['Top5%','5%-25%','25%-50%'])][['group','机构属性','work_set_up','director','user_id','assign_sum','申完量or放款量','申完率排名区间or放款率排名区间']]

    # 平均放款率
    # 内催/委外整体放款率
    员工数据_整体 = 员工数据_result[['group','机构属性','work_set_up','director','user_id','assign_sum','申完量or放款量']]
    整体平均指标 = 员工数据_整体.groupby(['group','机构属性','work_set_up']).agg(
        分案数 = ('assign_sum','sum'),
        审完放款数 = ('申完量or放款量','sum')
    ).reset_index()
    整体平均指标['平均(审完率|放款率)'] = round(整体平均指标['审完放款数'] / 整体平均指标['分案数'],4)

    # 主管整体放款率
    主管整体指标 = 员工数据_整体.groupby(['group','机构属性','director']).agg(
        分案数 = ('assign_sum','sum'),
        审完放款数 = ('申完量or放款量','sum')
    ).reset_index()
    主管整体指标['平均(审完率|放款率)'] = round(主管整体指标['审完放款数'] / 主管整体指标['分案数'],4)

    # 主管现场远程放款率
    主管现场远程指标 = 员工数据_整体.groupby(['group','机构属性','director','work_set_up']).agg(
        分案数 = ('assign_sum','sum'),
        审完放款数 = ('申完量or放款量','sum')
    ).reset_index()
    主管现场远程指标['平均(审完率|放款率)'] = round(主管现场远程指标['审完放款数'] / 主管现场远程指标['分案数'],4)


    # Top25%人员放款率
    排名前25整体指标 = 排名前25员工数据.groupby(['group','机构属性','work_set_up']).agg(
        分案数 = ('assign_sum','sum'),
        审完放款数 = ('申完量or放款量','sum')
    ).reset_index()
    排名前25整体指标['Top25%(审完率|放款率)'] = round(排名前25整体指标['审完放款数'] / 排名前25整体指标['分案数'],4)

    # 主管整体放款率
    排名前25主管指标 = 排名前25员工数据.groupby(['group','机构属性','director']).agg(
        分案数 = ('assign_sum','sum'),
        审完放款数 = ('申完量or放款量','sum')
    ).reset_index()
    排名前25主管指标['Top25%(审完率|放款率)'] = round(排名前25主管指标['审完放款数'] / 排名前25主管指标['分案数'],4)

    # 主管现场远程放款率
    排名前25主管现场远程指标 = 排名前25员工数据.groupby(['group','机构属性','director','work_set_up']).agg(
        分案数 = ('assign_sum','sum'),
        审完放款数 = ('申完量or放款量','sum')
    ).reset_index()
    排名前25主管现场远程指标['Top25%(审完率|放款率)'] = round(排名前25主管现场远程指标['审完放款数'] / 排名前25主管现场远程指标['分案数'],4)

    # Top50%人员放款率
    排名前50整体指标 = 排名前50员工数据.groupby(['group','机构属性','work_set_up']).agg(
        分案数 = ('assign_sum','sum'),
        审完放款数 = ('申完量or放款量','sum')
    ).reset_index()
    排名前50整体指标['Top50%(审完率|放款率)'] = round(排名前50整体指标['审完放款数'] / 排名前50整体指标['分案数'],4)

    # 主管整体放款率
    排名前50主管指标 = 排名前50员工数据.groupby(['group','机构属性','director']).agg(
        分案数 = ('assign_sum','sum'),
        审完放款数 = ('申完量or放款量','sum')
    ).reset_index()
    排名前50主管指标['Top50%(审完率|放款率)'] = round(排名前50主管指标['审完放款数'] / 排名前50主管指标['分案数'],4)

    # 主管现场远程放款率
    排名前50主管现场远程指标 = 排名前50员工数据.groupby(['group','机构属性','director','work_set_up']).agg(
        分案数 = ('assign_sum','sum'),
        审完放款数 = ('申完量or放款量','sum')
    ).reset_index()
    排名前50主管现场远程指标['Top50%(审完率|放款率)'] = round(排名前50主管现场远程指标['审完放款数'] / 排名前50主管现场远程指标['分案数'],4)

    Top_data = pd.concat([排名前50主管现场远程指标,排名前50主管指标,排名前50整体指标,排名前25主管现场远程指标,排名前25主管指标,排名前25整体指标,主管现场远程指标,主管整体指标,整体平均指标],ignore_index=True)
    Top_data = Top_data[['group','director','机构属性','work_set_up','Top50%(审完率|放款率)','Top25%(审完率|放款率)','平均(审完率|放款率)']]
    Top_data.columns = ['业务组','主管','机构属性','办公方式','Top50%(审完率|放款率)','Top25%(审完率|放款率)','平均(审完率|放款率)']
    Top_data = Top_data.sort_values(by=['办公方式', '机构属性', '主管', '业务组'], ascending=[False, False, False, False]) 

    # 主管业绩合并
    主管top_df_1 = pd.merge(主管现场远程指标,排名前25主管现场远程指标,on=(['group','机构属性','director','work_set_up']),how="left")
    主管top_df_2 = pd.merge(主管top_df_1,排名前50主管现场远程指标,on=(['group','机构属性','director','work_set_up']),how="left")
    # 主管top_df_2 = 主管top_df_2[['group','director','机构属性','work_set_up','Top50%(审完率|放款率)','Top25%(审完率|放款率)','平均(审完率|放款率)']]
    # 主管top_df_2.columns=['业务组','主管','机构属性','办公方式','Top50%(审完率|放款率)','Top25%(审完率|放款率)','平均(审完率|放款率)']

    # 业务组数据合并
    业务组top_df_1 = pd.merge(整体平均指标,排名前25整体指标,on=(['group','机构属性','work_set_up']),how='left')
    业务组top_df_2 = pd.merge(业务组top_df_1,排名前50整体指标,on=(['group','机构属性','work_set_up']),how='left')
    # 业务组top_df_2 = 业务组top_df_2[['group','机构属性','work_set_up','Top50%(审完率|放款率)','Top25%(审完率|放款率)','平均(审完率|放款率)']]
    # 业务组top_df_2.columns=['业务组','机构属性','办公方式','Top50%(审完率|放款率)','Top25%(审完率|放款率)','平均(审完率|放款率)']

    return 主管top_df_2,业务组top_df_2