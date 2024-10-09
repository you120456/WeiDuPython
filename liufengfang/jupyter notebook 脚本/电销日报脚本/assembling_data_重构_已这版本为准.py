import pandas as pd
import query_sql as sql
import numpy as np
from excel_beautify import beautify_excel
from send_email import send_bulk_emails_with_attachment


# 组长排名区间相关算法
# 组长排名区间算法
def calculation_interval_tl(row, column_name, target_column):
    max_Ranking = row[column_name]
    if row[target_column] <= round(max_Ranking * 0.2, 2):
        return "TOP 20%"
    elif row[target_column] <= round(max_Ranking * 0.4, 2):
        return "20%-40%"
    elif row[target_column] <= round(max_Ranking * 0.6, 2):
        return "40%-60%"
    elif row[target_column] <= round(max_Ranking * 0.8, 2):
        return "60%-80%"
    else:
        return "Bottom 20%"


# 组长排名区间算法
def calculation_interval_tl_v2(row, target_column):
    if row[target_column] == 1:
        return "20%-40%"
    elif row[target_column] == 2:
        return "40%-60%"
    elif row[target_column] == 3:
        return "60%-80%"


# 计算排名及排名区间
def get_ranking_range_tl(df):
    # 审完率排名
    df['申完率排名or放款率排名'] = df.groupby('group')['申完率or放款率'].rank(method='min', ascending=False)
    # 统计参与排名总人数
    df['参与排名总人数'] = df.groupby('group')['team_leader_no'].transform('count')
    # 计算排名区间
    df['申完率排名区间or放款率区间'] = df.apply(lambda x:
                                    calculation_interval_tl(x, '参与排名总人数', '申完率排名or放款率排名')
                                    if x['参与排名总人数'] > 3
                                    else calculation_interval_tl_v2(x, '申完率排名or放款率排名'), axis=1)

    return df.drop(columns=('参与排名总人数'))


# 组长排名区间相关算法


# 员工层相关算法函数
# 获取员工在各个组里最新的架构数据
def get_latest_organization_by_date(organization_df, group_col1, group_col2, value):
    """
    获取每个组按时间列最新的数据。
    参数:
    - organization_df: pandas DataFrame,包含要操作的数据
    - group_col1: str,分组列的名称
    - group_col2: str,分组列的名称
    - value: str,时间列的名称
    返回:
    - organization_latest_df: 包含每个组最新数据的行
    """
    organization_df[value] = pd.to_datetime(organization_df[value])
    latest_indices = organization_df.groupby([group_col1, group_col2])[value].idxmax()
    organization_latest_df = organization_df.loc[latest_indices]
    return organization_latest_df


# 员工排名区间算法
def calculation_interval(row, column_name, target_column):
    row[column_name]
    max_Ranking = row[column_name]
    if row[target_column] <= round(max_Ranking * 0.05, 2):
        return "Top5%"
    elif row[target_column] <= round(max_Ranking * 0.25, 2):
        return "5%-25%"
    elif row[target_column] <= round(max_Ranking * 0.5, 2):
        return "25%-50%"
    elif row[target_column] <= round(max_Ranking * 0.7, 2):
        return "50%-70%"
    elif row[target_column] <= round(max_Ranking * 0.9, 2):
        return "70%-90%"
    else:
        return "bottom10%"


# 计算排名及排名区间
def get_ranking_range(df):
    # 审完率排名
    df['申完率排名or放款率排名'] = df.groupby('group')['申完率or放款率'].rank(method='min', ascending=False)
    # 统计参与排名总人数
    df['参与排名总人数'] = df.groupby('group')['user_id'].transform('count')
    # 计算排名区间
    df['申完率排名区间or放款率区间'] = df.apply(lambda x: calculation_interval(x, '参与排名总人数', '申完率排名or放款率排名'), axis=1)
    return df


# 员工层相关算法函数
def get_data(start_day, end_day, loan_end_day, group, conn_link):
    # 数据源准备 
    print("开始查询数据源")
    print('分案开始时间：' + start_day, '分案结束时间：' + end_day, '放款结束时间：' + loan_end_day)
    # 每日分案对应的业绩数据
    performance_df = pd.read_sql(sql.performance_sql.format(start_day, end_day, loan_end_day), conn_link)
    # # 每日是否出勤
    attendance_df = pd.read_sql(sql.attendance_days_sql.format(start_day, end_day, loan_end_day), conn_link)
    # 每日过程数据
    process_df = pd.read_sql(sql.process_sql.format(start_day, end_day, loan_end_day), conn_link)
    # 离职日期查询
    dimission_date_df = pd.read_sql(sql.dimission_date_sql.format(end_day), conn_link)
    # 首次上线日期查询
    first_online_df = pd.read_sql(sql.first_online_sql.format(start_day, end_day, loan_end_day), conn_link)
    # 应出勤天数
    required_attendance_df = pd.read_sql(sql.required_attendance_days.format(start_day, end_day), conn_link)
    # 当月所有排班人员数据查询
    current_month_first_scheduled_df = pd.read_sql(sql.current_month_first_scheduled.format(start_day, end_day, group),
                                                   conn_link)
    # uo表每日架构
    organization_df = pd.read_sql(sql.organization_sql.format(start_day, end_day, group), conn_link)
    # 分案表每日架构
    assign_organization_df = pd.read_sql(sql.assign_organization.format(start_day, end_day), conn_link)
    # 工号及是否计入流失标志
    no_count_churn_df = pd.read_sql(sql.no_count_churn, conn_link)
    print("数据源查询完成开始拼装数据")
    return performance_df, attendance_df, process_df, dimission_date_df, first_online_df, required_attendance_df, current_month_first_scheduled_df, organization_df, assign_organization_df, no_count_churn_df


# 按架构清洗后的数据
def get_cleaning_data(performance_df, attendance_df, process_df, organization_df, first_online_df):
    # 每日架构匹配分案放款数据
    organization_df['uo_date'] = pd.to_datetime(organization_df['uo_date'])
    performance_df['assign_date'] = pd.to_datetime(performance_df['assign_date'])

    get_performance_df = pd.merge(
        organization_df,
        performance_df,
        left_on=['user_id', 'uo_date'],
        right_on=['user_id', 'assign_date'],
        how='left'
    ).drop(columns=['assign_date'])
    # 每日架构匹配每日过程数据
    process_df['call_date'] = pd.to_datetime(process_df['call_date'])
    get_process_df = pd.merge(
        organization_df,
        process_df,
        left_on=['user_id', 'uo_date'],
        right_on=['user_id', 'call_date'],
        how='left'
    ).drop(columns=['call_date'])
    # 每日架构匹配每日出勤数据
    attendance_df['work_day'] = pd.to_datetime(attendance_df['work_day'])
    get_attendance_df = pd.merge(
        organization_df,
        attendance_df,
        left_on=['user_id', 'uo_date'],
        right_on=['user_id', 'work_day'],
        how='left'
    ).drop(columns=['work_day'])
    # 匹配上线日期判断新老人上线天数
    first_online_df['first_online_day'] = pd.to_datetime(first_online_df['电销首次上线日期'])
    get_online_frist_df = first_online_df
    get_attendance_df = pd.merge(
        get_attendance_df,
        get_online_frist_df,
        left_on='user_id',
        right_on='坐席ID',
        how='left'
    ).drop(columns=['电销首次上线日期', '坐席ID'])
    get_attendance_df['newly_online'] = get_attendance_df['uo_date'] <= (
                get_attendance_df['first_online_day'] + pd.Timedelta(days=29))
    get_attendance_df['old_online'] = get_attendance_df['uo_date'] > (
                get_attendance_df['first_online_day'] + pd.Timedelta(days=29))
    return get_performance_df, get_process_df, get_attendance_df


def get_staff_result(get_performance_df, get_process_df, get_attendance_df, dimission_date_df, first_online_df,
                     required_attendance_df, organization_df, no_count_churn_df, end_day,
                     current_month_first_scheduled_df, if_exit_application):
    # 按时间获取最新同业务组员工架构
    latest_uo_df = get_latest_organization_by_date(organization_df, group_col1='group', group_col2='user_id',
                                                   value='uo_date')
    # 计算员工数据 start
    # 业绩数据架构替换为业务组内最新架构
    staff_performance = get_performance_df.drop(columns=['uo_date', 'director', 'team_leader', 'user_name'])
    staff_performance = pd.merge(staff_performance, latest_uo_df, left_on=['user_id', 'group'],
                                 right_on=['user_id', 'group'], how='left')

    staff_performance = staff_performance.groupby(['group', 'director', 'team_leader', 'user_id']).agg(
        assign_sum=('include_assign', 'sum'),
        loan_sum=('include_loan', 'sum'),
        application_sum=('include_application', 'sum')
    ).reset_index()
    # 每日过程数据替换为业务组内最新架构
    staff_process = get_process_df.drop(columns=['uo_date', 'director', 'team_leader', 'user_name'])
    staff_process = pd.merge(staff_process, latest_uo_df, left_on=['user_id', 'group'], right_on=['user_id', 'group'],
                             how='left')
    staff_process = staff_process.groupby(['group', 'director', 'team_leader', 'user_id']).agg(
        call_times_sum=('call_times', 'sum'),
        talk_duration_sum=('talk_duration', 'sum')
    ).reset_index()
    # 每日出勤替换为业务组内最新架构
    staff_attendance = get_attendance_df.drop(columns=['uo_date', 'director', 'team_leader', 'user_name'])
    staff_attendance = pd.merge(staff_attendance, latest_uo_df, left_on=['user_id', 'group'],
                                right_on=['user_id', 'group'], how='left')
    staff_attendance = staff_attendance.groupby(['group', 'director', 'team_leader', 'user_id']).agg(
        attendance_sum=('attendance_status', 'sum'),
        newly_sum=('attendance_status', lambda x: (staff_attendance.loc[x.index, 'newly_online'] & x == 1).sum()),
        old_sum=('attendance_status', lambda x: (staff_attendance.loc[x.index, 'old_online'] & x == 1).sum())
    ).reset_index()
    # 员工数据合并
    staff_uo_lastest = latest_uo_df.drop(columns=['uo_date'])
    # 取在排班表里存在排班的人员
    staff_uo_lastest = staff_uo_lastest[
        staff_uo_lastest['user_id'].isin(current_month_first_scheduled_df['user_id'].tolist())]
    合并业绩数据 = pd.merge(staff_uo_lastest, staff_performance.drop(columns=['team_leader', 'director']),
                      left_on=['user_id', 'group'], right_on=['user_id', 'group'], how='left')
    合并过程数据 = pd.merge(合并业绩数据, staff_process.drop(columns=['team_leader', 'director']), left_on=['user_id', 'group'],
                      right_on=['user_id', 'group'], how='left')
    合并出勤天数 = pd.merge(合并过程数据, staff_attendance.drop(columns=['team_leader', 'director']), left_on=['user_id', 'group'],
                      right_on=['user_id', 'group'], how='left')
    匹配上线日期 = pd.merge(合并出勤天数, first_online_df.drop(columns='电销首次上线日期'), left_on='user_id', right_on='坐席ID', how='left')
    匹配离职日期 = pd.merge(匹配上线日期, dimission_date_df, on='user_id', how='left')
    # 员工基础数据
    staff_base = 匹配离职日期.drop(columns='坐席ID')
    staff_base['dimission_date'] = pd.to_datetime(staff_base['dimission_date'])
    staff_base['first_online_day'] = pd.to_datetime(staff_base['first_online_day'])
    # 判断是否新人
    staff_base['是否新人'] = staff_base['first_online_day'].apply(lambda x: 'YES' if pd.isna(x) or (
                (x + pd.Timedelta(days=29)).date() >= pd.to_datetime(end_day).date()) else 'NO')
    # 日均外呼次数 日均通话时长
    staff_base['日均外呼次数'] = round(staff_base['call_times_sum'] / staff_base['attendance_sum'], 0)
    staff_base['日均通话时长'] = round(staff_base['talk_duration_sum'] / staff_base['attendance_sum'] / 60, 0)
    # 申完率 放款率

    # TODO 审完量放款量指标合并 (菲律宾特有)
    # if_exit_application 是否存在审完量 1：统计，0：不统计
    staff_base['申完量or放款量'] = np.where(staff_base['group'] == 'Telesales A', staff_base['application_sum'],
                                      staff_base['loan_sum']) if if_exit_application == 1 else staff_base['loan_sum']

    # staff_base['申完率'] = round(staff_base['application_sum'] / staff_base['assign_sum'],4)
    # staff_base['放款率'] = round(staff_base['loan_sum'] / staff_base['assign_sum'],4)
    staff_base['申完率or放款率'] = round(staff_base['申完量or放款量'] / staff_base['assign_sum'], 4)

    # 全部员工进行排名
    '''
    1.排除上线天数等于0的不进行排名
    2.排除离职日期在当前日期之前并且上线天数小于等于15天的不进行排名
    '''
    all_rankings_staff = staff_base[~((staff_base['attendance_sum'] == 0)
                                      | ((staff_base['attendance_sum'] <= 15)
                                         & (staff_base['dimission_date'].dt.date < pd.to_datetime(end_day).date())))]

    # 上线大于15天员工进行排名
    '''
    1.排除上线天数等于0的不进行排名
    2.排除离职日期在当前日期之前并且上线天数小于等于15天的不进行排名
    '''
    all_rankings_staff_15 = staff_base[(staff_base['attendance_sum'] > 15)]
    # 剔除离职且上线<=15的人员排名
    # 剔除未上线人员排名
    df_all = get_ranking_range(all_rankings_staff)
    df_all = df_all[['group', 'director', 'team_leader', 'user_id', '申完率排名or放款率排名', '申完率排名区间or放款率区间']]

    if not all_rankings_staff_15.empty:  # 判断是否有15天以上数据的df
        print("已存在15天以上坐席数据")
        # 上线大于15天的人员的排名
        df_outpace_15 = get_ranking_range(all_rankings_staff_15)
        df_outpace_15.rename(columns={
            '申完率排名or放款率排名': '申完率排名or放款率排名(大于15)',
            '申完率排名区间or放款率区间': '申完率排名区间or放款率区间(大于15)'
        }, inplace=True)
        # 选择特定列数据
        df_outpace_15 = df_outpace_15[
            ['group', 'director', 'team_leader', 'user_id', '申完率排名or放款率排名(大于15)', '申完率排名区间or放款率区间(大于15)']]
        #  员工基表数据合并全部排名数据
        df_3 = pd.merge(staff_base, df_all, left_on=['group', 'director', 'team_leader', 'user_id'],
                        right_on=['group', 'director', 'team_leader', 'user_id'], how='left')
        # 合并大于15天排名数据
        df_4 = pd.merge(df_3, df_outpace_15, left_on=['group', 'director', 'team_leader', 'user_id'],
                        right_on=['group', 'director', 'team_leader', 'user_id'], how='left')
        # 判断员工业绩是否达标
        df_4['业绩是否达标(大于15)'] = np.where(
            (df_4['attendance_sum'] > 15) & (df_4['申完率排名区间or放款率区间(大于15)'].isin(['Top5%', '5%-25%', '25%-50%'])), 'YES',
            'NO')
    else: # 没有15天以上数据的df
        #  员工基表数据合并全部排名数据
        df_4 = pd.merge(staff_base, df_all, left_on=['group', 'director', 'team_leader', 'user_id'],
                        right_on=['group', 'director', 'team_leader', 'user_id'], how='left')
        # 判断员工业绩是否达标
        df_4['业绩是否达标'] = np.where(df_4['申完率排名区间or放款率区间'].isin(['Top5%', '5%-25%', '25%-50%']), 'YES', 'NO')

    # 匹配员工工号及组长工号
    df_5 = pd.merge(df_4, no_count_churn_df.drop(columns=['name', 'count_churn']).rename(columns={'no': '员工工号'}),
                    on='user_id', how='left')
    df_5 = pd.merge(df_5, no_count_churn_df.drop(columns=['user_id', 'count_churn']).rename(columns={'no': '组长工号'}),
                    left_on='team_leader', right_on='name', how='left').drop(columns='name')
    df_5['应出勤天数'] = int(required_attendance_df['schedule_count'])
    员工数据_result = df_5
    return 员工数据_result
    # 员工数据计算 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ END ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


def get_tl_result(get_attendance_df, get_performance_df, no_count_churn_df, 员工数据_result):
    # 组长数据计算 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ START ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # 计算员工在组长名下的上线天数
    组长名下员工上线天数 = get_attendance_df.groupby(['group', 'director', 'team_leader', 'user_id']).agg(
        组长名下上线天数=('attendance_status', 'sum')
    ).reset_index()

    # 组长名下上线天数拼接到员工每日业绩里用于计算组长业绩
    # 这里取在本月有排班数据的员工
    df_performance = get_performance_df[get_performance_df['user_id'].isin(员工数据_result['user_id'].tolist())]
    组长每日业绩 = pd.merge(df_performance, 组长名下员工上线天数, left_on=['group', 'director', 'team_leader', 'user_id'],
                      right_on=['group', 'director', 'team_leader', 'user_id'], how='left')
    组长业绩 = 组长每日业绩.groupby(['group', 'director', 'team_leader']).agg(
        分案数=('include_assign', 'sum'),
        申完数=('include_application', 'sum'),
        放款数=('include_loan', 'sum'),
    ).reset_index()

    组长业绩_大于15天 = 组长每日业绩[组长每日业绩['组长名下上线天数'] > 15].groupby(['group', 'director', 'team_leader']).agg(
        分案数=('include_assign', 'sum'),
        申完数=('include_application', 'sum'),
        放款数=('include_loan', 'sum'),
    ).reset_index()
    # 统计组长离职人数及总人数统计
    组长离职人数统计 = 员工数据_result.groupby(['group', 'director', 'team_leader'])['dimission_date'].count().reset_index().rename(
        columns={'dimission_date': '离职人数'})
    组长总人数统计 = 员工数据_result.groupby(['group', 'director', 'team_leader'])['user_id'].count().reset_index().rename(
        columns={'user_id': '总人数'})
    # 达标人数统计
    组长达标人数统计 = 员工数据_result[员工数据_result['业绩是否达标'] == 'YES'].groupby(['group', 'director', 'team_leader'])[
        'user_id'].count().reset_index().rename(columns={'user_id': '达标人数'})
    组长业绩1 = pd.merge(组长业绩, 组长总人数统计, on=(['group', 'director', 'team_leader']), how='left')
    组长业绩2 = pd.merge(组长业绩1, 组长离职人数统计, on=(['group', 'director', 'team_leader']), how='left')
    组长业绩3 = pd.merge(组长业绩2, 组长达标人数统计, on=(['group', 'director', 'team_leader']), how='left')
    组长业绩4 = pd.merge(组长业绩3,
                     no_count_churn_df.drop(columns=['user_id', 'count_churn']).rename(columns={'name': 'team_leader'}),
                     on=('team_leader'), how='left').rename(columns={'no': 'team_leader_no'})
    组长业绩4['流失率'] = round(组长业绩4['离职人数'] / 组长业绩4['总人数'], 4)
    组长业绩4['应达标人数'] = 组长业绩4['总人数'] // 2
    组长业绩4['申完数or放款数'] = np.where(组长业绩4['group'] == 'Telesales A', 组长业绩4['申完数'], 组长业绩4['放款数'])
    组长业绩4['申完率or放款率'] = round(组长业绩4['申完数or放款数'] / 组长业绩4['分案数'], 4)
    组长业绩_result = get_ranking_range_tl(组长业绩4)
    return 组长业绩_result
    # 组长数据计算 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ END ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


def get_dr_result(get_attendance_df, get_performance_df, 员工数据_result, 组长业绩_result, no_count_churn_df):
    # 主管数据计算 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ START ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    # 计算员工在主管名下的上线天数
    主管名下员工上线天数 = get_attendance_df.groupby(['group', 'director', 'user_id']).agg(
        主管名下上线天数=('attendance_status', 'sum')
    ).reset_index()

    # 主管名下上线天数拼接到员工每日业绩里用于计算组长业绩
    # 这里取在本月有排班数据的员工
    df_performance = get_performance_df[get_performance_df['user_id'].isin(员工数据_result['user_id'].tolist())]
    主管每日业绩 = pd.merge(df_performance, 主管名下员工上线天数, left_on=['group', 'director', 'user_id'],
                      right_on=['group', 'director', 'user_id'], how='left')
    主管业绩 = 主管每日业绩.groupby(['group', 'director']).agg(
        分案数=('include_assign', 'sum'),
        # 申完数 = ('include_application','sum'),
        放款数=('include_loan', 'sum'),
    ).reset_index()

    主管业绩_大于15天 = 主管每日业绩[主管每日业绩['主管名下上线天数'] > 15].groupby(['group', 'director', 'team_leader']).agg(
        分案数=('include_assign', 'sum'),
        # 申完数 = ('include_application','sum'),
        放款数=('include_loan', 'sum'),
    ).reset_index()
    主管离职人数统计 = 员工数据_result.groupby(['group', 'director'])['dimission_date'].count().reset_index().rename(
        columns={'dimission_date': '离职人数'})
    主管总人数统计 = 员工数据_result.groupby(['group', 'director'])['user_id'].count().reset_index().rename(
        columns={'user_id': '总人数'})
    # 达标人数统计
    主管达标人数统计 = 员工数据_result[员工数据_result['业绩是否达标'] == 'YES'].groupby(['group', 'director'])[
        'user_id'].count().reset_index().rename(columns={'user_id': '达标人数'})
    主管带组数 = 组长业绩_result.groupby(['group', 'director'])['team_leader'].count().reset_index().rename(
        columns={'team_leader': '带组数'})
    主管业绩1 = pd.merge(主管业绩, 主管总人数统计, on=(['group', 'director']), how='left')
    主管业绩2 = pd.merge(主管业绩1, 主管离职人数统计, on=(['group', 'director']), how='left')
    主管业绩3 = pd.merge(主管业绩2, 主管达标人数统计, on=(['group', 'director']), how='left')
    主管业绩4 = pd.merge(主管业绩3,
                     no_count_churn_df.drop(columns=['user_id', 'count_churn']).rename(columns={'name': 'director'}),
                     on=('director'), how='left').rename(columns={'no': 'director_no'})
    主管业绩5 = pd.merge(主管业绩4, 主管带组数, on=(['group', 'director']), how='left')
    主管业绩5['流失率'] = round(主管业绩5['离职人数'] / 主管业绩4['总人数'], 4)
    主管业绩5['应达标人数'] = 主管业绩5['总人数'] // 2
    主管业绩5['放款率'] = round(主管业绩5['放款数'] / 主管业绩4['分案数'], 4)
    主管业绩_reult = 主管业绩5
    return 主管业绩_reult
    # 主管数据计算 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ END ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


def beautify_send(员工数据_result, 组长业绩_result, 主管业绩_reult, path, recipients, subject, email_body):
    # 数据排序排版 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ START ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    员工输出数据 = 员工数据_result[
        ['group', 'director', '组长工号', 'team_leader', '员工工号', 'user_name', '是否新人', 'first_online_day', 'dimission_date',
         '日均外呼次数'
            , '日均通话时长', '应出勤天数', 'attendance_sum', 'newly_sum', 'old_sum', 'assign_sum', '申完量or放款量', '申完率or放款率',
         '申完率排名or放款率排名', '申完率排名区间or放款率区间'
            , '业绩是否达标']]
    员工输出数据.columns = ['业务组', '主管', '组长工号', '组长', '坐席工号', '坐席', '是否新人', '上线日期', '离职日期', '日均外呼次数'
        , '日均通话时长', '应出勤天数', '总上线天数', '新人天数', '老人天数', '分案数', '申完量or放款量', '申完率or放款率', '申完率排名or放款率排名', '申完率排名区间or放款率区间'
        , '业绩是否达标']
    # 员工输出数据.columns = ['group', 'director', 'team_leader_no', 'team_leader', 'employee_no', 'user_name', 'is_new_employee', 
    #                      'first_online_day', 'dimission_date', 'avg_daily_calls', 'avg_daily_call_duration', 'expected_attendance_days', 
    #                      'attendance_sum', 'newly_sum', 'old_sum', 'application_or_loan_count', 
    #                      'application_or_loan_rate', 'application_or_loan_rate_rank', 'application_or_loan_rate_range', 
    #                      'is_performance_met']

    组长输出数据 = 组长业绩_result[
        ['group', 'director', 'team_leader_no', 'team_leader', '总人数', '应达标人数', '达标人数', '离职人数', '流失率', '分案数', '申完数or放款数',
         '申完率or放款率', '申完率排名or放款率排名', '申完率排名区间or放款率区间']]
    组长输出数据.columns = ['业务组', '主管', '组长工号', '组长', '坐席数', '应达标人数', '实际达标人数', '离职人数', '流失率', '分案数', '申完数or放款数', '申完率or放款率',
                      '申完率排名or放款率排名', '申完率排名区间or放款率区间']
    主管输出数据 = 主管业绩_reult[
        ['group', 'director_no', 'director', '总人数', '应达标人数', '达标人数', '离职人数', '流失率', '分案数', '放款数', '放款率', '带组数']]
    主管输出数据.columns = ['业务组', '主管工号', '主管', '坐席数', '应达标人数', '实际达标人数', '离职人数', '流失率', '分案数', '放款数', '放款率', '带组数']
    # 数据排序排版 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ END ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    ew = pd.ExcelWriter(path, engine='xlsxwriter')
    员工输出数据.to_excel(ew, sheet_name='坐席数据', index=False)
    组长输出数据.to_excel(ew, sheet_name='组长数据', index=False)
    主管输出数据.to_excel(ew, sheet_name='主管数据', index=False)
    ew.close()
    beautify_excel(path)
    # 发送邮件
    # recipients = ['liufengfang@weidu.ac.cn']
    send_bulk_emails_with_attachment(recipients, subject, email_body, path)
# if __name__ == "__main__":
# # 设置日报查询范围 查询日期初始化
# end_day = str(pd.Timestamp.now().date()-pd.Timedelta(days=1))+" 23:59:59"
# start_day = str((pd.Timestamp.now()- pd.Timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')) +' 00:00:00'
# loan_end_day = end_day

# # 数据保存路径
# path = './菲律宾电销/菲律宾电销日报({0}).xlsx'.format(pd.to_datetime(end_day).date())

# # 设置工作目录为脚本所在目录
# os.chdir(os.path.dirname(os.path.abspath(__file__)))
# # 读取配置文件获取电销业务组
# with open('config.yaml','r',encoding='UTF-8') as file:
#     config = yaml.safe_load(file)
# group = ",".join(f"'{cf}'" for cf in config['philippines']['group'])

# # 获取数据数据源
# performance_df,attendance_df,process_df,dimission_date_df,first_online_df,required_attendance_df,current_month_first_scheduled_df,organization_df,assign_organization_df,no_count_churn_df = get_data(start_day,end_day,loan_end_day,group)

# # 获取清洗架构后的数据
# get_performance_df,get_process_df,get_attendance_df = get_cleaning_data(performance_df,attendance_df,process_df,organization_df,first_online_df)

# #员工数据
# 员工数据_result = get_staff_result(get_performance_df,get_process_df,get_attendance_df,dimission_date_df,first_online_df,required_attendance_df,organization_df,no_count_churn_df,end_day,current_month_first_scheduled_df,if_exit_application=1)

# # 组长数据
# 组长业绩数据_result = get_tl_result(get_attendance_df,get_performance_df,no_count_churn_df,员工数据_result)

# # 主管业绩数据
# 主管业绩数据_result = get_dr_result(get_attendance_df,get_performance_df,员工数据_result,组长业绩数据_result,no_count_churn_df)

# # 最终美化发送邮件
# # 邮件接收人list
# # recipients = config['philippines']['email']
# recipients = ['liufengfang@weidu.ac.cn']
# # 邮件主题
# subject = '【菲律宾电销日报数据-{0}】'.format(pd.to_datetime(end_day).date())
# # 邮件内容
# body =r"""<!DOCTYPE html><html><head><style>.indented {margin-left: 20px;}</style></head><body><p>各位好！</p> <p>&nbsp;&nbsp;&nbsp;&nbsp;附件是菲律宾电销日报数据，请查收！谢谢！</p></body></html>"""
# beautify_send(员工数据_result,组长业绩数据_result,主管业绩数据_result,path,recipients,subject,body)
