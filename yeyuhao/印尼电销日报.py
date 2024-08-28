#!/usr/bin/env python
# coding: utf-8
import numpy as np
import pandas as pd
import mysql.connector
import datetime
import warnings
import excelFormat
import config

warnings.filterwarnings("ignore")
def userKpi(assign_start,assign_end,loan_end):
    first_online_day_sql = '''
                    select 
                    user_no.员工ID,
                    user_no.工号,
                    user_no.工作方式,
                    user_no.离职日期,
                    frist_day.上线日期
                    from
                    (SELECT 
                    su.id as '员工ID',
                    su.`no` as '工号',
                    se.work_set_up as '工作方式',
                    se.dimission_date as '离职日期'
                    FROM ods_fox_sys_user su 
                    left join ods_fox_sys_user_extend  se
                    on su.id = se.user_id
                    ) user_no
                    left join 
                    (SELECT
                                            tad.user_id as '员工ID',
                                            MIN( work_day ) `上线日期`
                                        FROM
                                            ods_fox_tel_sale_attendance_dtl tad
                                            -- 各个电销国家对应电销队列
                                        GROUP BY
                                            tad.user_id) frist_day
                                            on user_no.员工ID = frist_day.员工ID                       
    '''
    user_no_df = config.indonesia_bd_engine_read(first_online_day_sql) # 获取员工远程职场工号及最早上线日信息

    uo_sql = '''
    
        SELECT pt_date as '日期',
                        SUBSTRING_INDEX(uo.asset_group_name,',',-1) `业务组`,
                        SUBSTRING_INDEX(SUBSTRING_INDEX(uo.parent_user_names, ',', 2), ',', -1) `主管`,
                        SUBSTRING_INDEX(SUBSTRING_INDEX(uo.parent_user_names, ',', 3), ',', -1) `组长`,
                        SUBSTRING_INDEX(SUBSTRING_INDEX(uo.parent_user_names, ',', 4), ',', -1) `组员`,
                        SUBSTRING_INDEX(SUBSTRING_INDEX(parent_user_ids, 'DPTU_', -1), ',', 1) `主管ID`,
                        SUBSTRING_INDEX(SUBSTRING_INDEX(parent_user_ids, 'GRU_', -1), ',', 1) `组长ID`,
                        uo.user_id as '员工ID'
                FROM
                        fox_tel_dw.dwd_fox_user_organization_df uo 
                WHERE
                        uo.pt_date>="{0}"
                        and uo.pt_date<"{1}"
                        and uo.user_id in (
                                            SELECT
                                            distinct(tad.user_id) as '员工ID' 
                                            FROM
                                                ods_fox_tel_sale_attendance_dtl tad                   
                                            WHERE
                                                (tad.work_day >= '{0}' AND tad.work_day < '{1}' )
                                                and tad.attendance_status = 1
                                                and asset_group_name  like '%Telesales%'
                                            )
    '''.format(str(assign_start)[0:10], str(assign_end)[0:10])
    uo_df = config.indonesia_bd_engine_read(uo_sql) # 员工每日架构信息

    day_user_df = pd.merge(uo_df,user_no_df, on=['员工ID'], how='left')       # 员工每日信息


    user_dtl_sql = '''
                    SELECT
                        tad.user_id as '员工ID' ,
                        tad.work_day as '日期',
                        tad.attendance_status  as '上线天数'
                    FROM
                        ods_fox_tel_sale_attendance_dtl tad                   
                    WHERE
                        (tad.work_day >= '{0}' AND tad.work_day < '{1}' )
                        and tad.attendance_status = 1
                        and asset_group_name  like '%Telesales%'
    '''.format(str(assign_start)[0:10], str(assign_end)[0:10])
    user_dtl_df = config.indonesia_bd_engine_read(user_dtl_sql)   # 员工每日上线明细


    user_call_sql = '''
                SELECT
                    call_detail.dunner_id as 员工ID,
                    date(call_detail.dial_time) AS 日期,
                    SUM( call_detail.talk_duration )  as "通话时长", 
                    COUNT(IF( call_detail.talk_duration > 0, 1, NULL )) as "通话次数"
                FROM
                -- 通话明细 start-- 
                    (
                    SELECT
                        tche.dunner_id,
                        tch.dial_time,
                        tch.talk_duration 
                    FROM
                        ods_fox_tel_sale_call_history tch
                        LEFT JOIN ods_fox_tel_sale_call_history_extend tche ON tch.id = tche.source_id 
                    WHERE
                    ( tch.dial_time >= '{0}' AND tch.dial_time < '{1}' )) `call_detail` 
                    -- 通话明细 end-- 
                GROUP BY
                    call_detail.dunner_id,date(call_detail.dial_time)
    '''.format(str(assign_start)[0:10], str(assign_end)[0:10])
    user_call_df = config.indonesia_bd_engine_read(user_call_sql)   # 员工每日通时通次


    user_case_sql = '''
                    SELECT
                        DATE( assign_create ) as '日期',
                        a.assign_sys_user_id as '员工ID',
                        COUNT(
                        IF
                            ((
                                    untml_reason IS NULL 
                                    OR untml_reason NOT IN ( '短假撤案', '黑名单上报', 'black_report_withdrawal_case', 'short_holiday_withdrawal_case' )) 
                            AND
                            IF
                                ( untml_reason IN ( '长假撤案', '稽核下线', '离职撤案', 'long_vacation_withdrawal_case', 'audit_offline_withdrawal_case', 'resign_withdrawal_case', 'RESIGN_AND_WITHDRAW_THE_CASE' ) AND `follow_status` IS NULL, 0, 1 ),
                                1,
                            NULL 
                            )) AS '分案数',
                        COUNT( a.mission_log_id ) '放款数' 
                    FROM
                        (
                        SELECT
                            tml.asset_type_group_name,
                            tml.director_id,
                            tml.leader_id,
                            tml.assign_sys_user_name,
                            tml.assign_sys_user_id,
                            tml.id,
                            tml.create_at `assign_create`,
                            untml.reason `untml_reason`,
                            untml.create_at `untml_create`,
                            tst.customer_type,
                            follow.follow_status,
                            tt.mission_log_id 
                        FROM
                            ods_fox_tel_sale_mission_log tml
                            LEFT JOIN ods_fox_tel_sale_mission_log untml ON tml.id = untml.assign_mission_log_id 
                            AND untml.operator = 'unassign' 
                            AND ( untml.create_at >= '{0}' AND untml.create_at < '{1}' )
                            LEFT JOIN ods_fox_tel_sale_task tst ON tml.task_number = tst.task_number
                            LEFT JOIN (
                            SELECT DISTINCT
                                tcr.mission_log_id,
                                1 `follow_status` 
                            FROM
                                ods_fox_tel_sale_contact_record tcr 
                            WHERE
                                ( tcr.create_at >= '{0}' AND tcr.create_at < '{1}' ) 
                                AND tcr.call_uuid <> '' 
                            ) follow ON tml.id = follow.mission_log_id
                            LEFT JOIN ods_fox_tel_sale_loan_task tt ON tml.id = tt.mission_log_id 
                            AND tt.cancel_loan_time IS NULL 
                            AND ( tt.on_loan_time >= '{0}' AND tt.on_loan_time < '{2}' ) 
                        WHERE
                            tml.operator = 'assign' 
                            AND ( tml.create_at >= '{0}' AND tml.create_at < '{1}' ) 
                        ) a 
                    GROUP BY
                        DATE( assign_create ),
                        a.assign_sys_user_id
    '''.format(str(assign_start)[0:10], str(assign_end)[0:10], str(loan_end)[0:10])

    user_case_df = config.indonesia_bd_engine_read(user_case_sql)   # 员工每日通时通次


    day_user_df['日期'] = pd.to_datetime(day_user_df['日期'])
    day_user_df['上线日期'] = pd.to_datetime(day_user_df['上线日期'])
    day_user_df['离职日期'] = pd.to_datetime(day_user_df['离职日期'])
    user_dtl_df['日期'] = pd.to_datetime(user_dtl_df['日期'])
    user_call_df['日期'] = pd.to_datetime(user_call_df['日期'])
    user_case_df['日期'] = pd.to_datetime(user_case_df['日期'])
    day_user_df = pd.merge(day_user_df,user_dtl_df, on=['员工ID','日期'], how='left')
    day_user_df = pd.merge(day_user_df,user_call_df, on=['员工ID','日期'], how='left')
    day_user_df = pd.merge(day_user_df,user_case_df, on=['员工ID','日期'], how='left')
    day_user_df.loc[( day_user_df['日期']-day_user_df['上线日期']).dt.days < 30, '新人上线天数'] = day_user_df['上线天数']
    day_user_df.loc[( day_user_df['日期']-day_user_df['上线日期']).dt.days >= 30, '老人上线天数'] = day_user_df['上线天数']
    day_user_df = day_user_df [(day_user_df['离职日期'].isnull()) | ((( day_user_df['日期']-day_user_df['离职日期']).dt.days <= 0) & (day_user_df['离职日期'].notnull()))] # 离职人员业绩固定按离职日期
    day_user_df.loc[((( (assign_end - datetime.timedelta(1))-day_user_df['离职日期']).dt.days <= 0) & (day_user_df['离职日期'].notnull())), '离职日期'] = np.NaN
    new_user_df = day_user_df.sort_values(by=['日期', '员工ID','业务组'], ascending=False)
    new_user_df = new_user_df.drop_duplicates(subset=['业务组', '员工ID'], keep='first')
    user_df = day_user_df.groupby(by=['业务组', '员工ID'],as_index=False).agg({"上线天数": "sum", "通话时长": "sum", '通话次数': "sum",
                                                                          '分案数': "sum",'放款数': "sum",'新人上线天数': "sum",'老人上线天数': "sum"})
    user_df['日均通时'] = round(user_df['通话时长']/(user_df['上线天数']*60), 0)
    user_df['日均通次'] = round(user_df['通话次数']/(user_df['上线天数']), 0)
    user_df['放款率'] = round(user_df['放款数']/(user_df['分案数']), 4)


    user_df = pd.merge(new_user_df[['业务组','主管','组长','组员','主管ID','组长ID','员工ID','工号','工作方式','离职日期','上线日期']],user_df, on=['员工ID','业务组'], how='left')

    user_df = user_df [(user_df['离职日期'].isnull()) | (( user_df['上线天数']> 15) & (user_df['离职日期'].notnull()))] # 离职人员业绩固定按离职日期
    user_df['放款数排名'] = user_df.groupby(['业务组'])['放款数'].rank(ascending=False,method='min')

    def calculate_range1(row):
        max_m = user_df[user_df['业务组'] == row['业务组']]["放款数排名"].max()
        if row["放款数排名"] <= round(max_m * 0.05):
            return "Top5%"
        elif row["放款数排名"] <= round(max_m * 0.25):
            return "5%-25%"
        elif row["放款数排名"] <= round(max_m * 0.5):
            return "25%-50%"
        elif row["放款数排名"] <= round(max_m * 0.7):
            return "50%-70%"
        elif row["放款数排名"] <= round(max_m * 0.9):
            return "70%-90%"
        else:
            return "bottom10%"
    user_df["放款数排名区间"] = user_df.apply(calculate_range1, axis=1)

    user_df.loc[( assign_end- datetime.timedelta(1)-user_df['上线日期']).dt.days >= 30, '是否新人'] = 'NO'
    user_df.loc[( assign_end- datetime.timedelta(1)-user_df['上线日期']).dt.days < 30, '是否新人'] = 'YES'

    no_sql = '''
    SELECT 
                    su.id as 'ID',
                    su.`no` as '人员工号'
                    FROM ods_fox_sys_user su 
    
    '''
    no_df = config.indonesia_bd_engine_read(no_sql) # 员工每日架构信息

    user_df = pd.merge(user_df, no_df, left_on='主管ID',right_on='ID',how='left')
    user_df = user_df.rename(columns={"人员工号": "主管工号"})
    del user_df['ID']
    user_df = pd.merge(user_df, no_df, left_on='组长ID',right_on='ID',how='left')
    user_df = user_df.rename(columns={"人员工号": "组长工号"})
    del user_df['ID']

    user_df['应出勤天数'] = 0

    user_df['离职日期'] = user_df['离职日期'].dt.date
    user_df['上线日期'] = user_df['上线日期'].dt.date
    top_rank_range = ['5%-25%', 'Top5%', '25%-50%']
    bottom_rank_range = ['50%-70%', '70%-90%', 'bottom10%']

    user_df.loc[ user_df['放款数排名区间'].isin(top_rank_range), '是否达标'] = 'YES'
    user_df.loc[ user_df['放款数排名区间'].isin(bottom_rank_range), '是否达标'] = 'NO'
    user_df = user_df[user_df['上线天数'] > 0]

    user_df = user_df[['业务组', '主管', '组长工号', '组长', '工号', '组员', '是否新人', '上线日期', '离职日期', '日均通次', '日均通时', '应出勤天数', '上线天数', '新人上线天数', '老人上线天数', '分案数', '放款数', '放款率', '放款数排名', '放款数排名区间', '是否达标', '工作方式']]
    # day_user_df.to_excel(r'D:\DailyReport\印尼报表\电销\自动化报表\wave电销日报明细{}.xlsx'.format(str( assign_end- datetime.timedelta(1))[0:10]), index=False)
    # excelFormat.beautify_excel(r'D:\DailyReport\印尼报表\电销\自动化报表\wave电销日报明细{}.xlsx'.format(str( assign_end- datetime.timedelta(1))[0:10]))
    user_df.to_excel(r'D:\DailyReport\印尼报表\电销\自动化报表\wave电销日报{}.xlsx'.format(str( assign_end- datetime.timedelta(1))[0:10]), index=False)
    excelFormat.beautify_excel(r'D:\DailyReport\印尼报表\电销\自动化报表\wave电销日报{}.xlsx'.format(str( assign_end- datetime.timedelta(1))[0:10]))




print("印尼电销自动化,开始运行：", datetime.datetime.now())
now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
assign_start = now - datetime.timedelta(7)
assign_end = now - datetime.timedelta(0)
assign_start = now.replace(year=2024, month=7, day=26)  # 数据开始日期
assign_end = now.replace(year=2024, month=7, day=31)  # 分案截止日期+1day
loan_end = assign_end + datetime.timedelta(0) # 放款截止日期+1day
for i in range(31):
    end_time = assign_end + datetime.timedelta(i)
    loan_end = end_time + datetime.timedelta(0)
    userKpi(assign_start,end_time,loan_end)
print("结束运行", datetime.datetime.now())

