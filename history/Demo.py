#!/usr/bin/env python
# coding: utf-8


import config
import pymysql
import mysql.connector
import datetime
import pandas as pd
import warnings
from sshtunnel import SSHTunnelForwarder

warnings.filterwarnings("ignore")
print("'脚本6 稽核数据：稽核日报'\n开始执行!", datetime.datetime.now())
now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
time_end = now - datetime.timedelta(0)
time_start = now - datetime.timedelta(1)
# month_start = time_start.replace(day=1)
time_start = now.replace(year=2024, month=6, day=15) # 开始日期
time_end = now.replace(year=2024, month=6, day=16) # 数据截止日期
month_start = time_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
print('数据时间范围{}--->{}'.format(str(time_start),
                              str(time_end)))

# ### 巴基斯坦数据

# =============================================================================
# 字段计算
# =============================================================================
# 巴基斯坦稽核架构
sql_uo_Pakistan = '''
                SELECT
                    SUBSTRING_INDEX( uo.`asset_group_name`, ',',- 1 ) '组别',
                    uo.office_name,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(uo.`parent_user_names`,',',2),',',-1) '主管',
                    cc.TL_NO,
                    uo.parent_user_name '组长',
                    b.co_NO,
                    uo.`name`,
                    uo.user_id,
                    c.dimission_date,
                    d.online_day 
                FROM
                    arcticfox.`user_organization` uo
                    LEFT JOIN ( SELECT DISTINCT NAME, NO AS TL_NO FROM sys_user ) AS cc ON uo.parent_user_name = cc.
                    NAME LEFT JOIN ( SELECT DISTINCT id, NO AS co_NO FROM sys_user ) AS b ON uo.user_id = b.id
                    LEFT JOIN ( SELECT DISTINCT user_id, dimission_date 
                    FROM sys_user_extend ) AS c ON uo.user_id = c.user_id
                    LEFT JOIN ( SELECT DISTINCT user_id, min( online_day ) online_day 
                    FROM online_days WHERE online_day_flag = 1 GROUP BY user_id ) AS d ON uo.user_id = d.user_id
    '''

uo_Pakistan_df = config.pakistan_fox_engine_read(sql_uo_Pakistan)

# 入催户数
sql_input_Pakistan = '''
                    SELECT
                    date( cr.`create_at` ) date,
                    `asset_from_app`,
                    count( DISTINCT da.debtor_id ) 户数 
                FROM
                    `collect_recovery` cr
                    JOIN asset a ON cr.asset_id = a.asset_id
                    JOIN debtor_asset da ON a.asset_id = da.asset_id 
                WHERE
                    cr.`batch_num` IS NULL 
                    AND cr.`late_days` = 1 
                    AND cr.`create_at` >= '{}'
                    AND cr.`create_at` < '{}'
                GROUP BY
                    1,2
    '''.format(str(time_start), str(time_end))

input_Pakistan_df = config.pakistan_fox_engine_read(sql_input_Pakistan)

# 工单信息
sql_order_Pakistan = '''
                    SELECT 
                    "巴基斯坦" as 国家,
                    wo.create_at as "创建日期",
                    wo.update_at as "最近更新时间",
                    if(wo.create_at <>wo.update_at, 
                    if(wo.status =2,
                    timestampdiff(
                            DAY,
                        DATE ( wo.create_at ),
                        DATE ( wo.update_at )),
                    timestampdiff(
                            DAY,
                        DATE ( wo.update_at ),
                        DATE ( now() ))),
                    timestampdiff(
                            DAY,
                        DATE ( wo.create_at),
                        DATE ( now() )))
                    相差天数,
                    asset_item_number as "资产编号",
                    debtor_name as "债务人姓名",
                    handle_sys_user_name as '处理人姓名',
                    CASE
                      WHEN type = 0 THEN '协商还款'
                        WHEN type = 1 THEN '协商还款(加急)'
                        WHEN type = 2 THEN '投诉'
                        WHEN type = 3 THEN '投诉倾向'
                      ELSE type 
                    END AS 进件类型,
                    CASE
                      WHEN wo.status  = 0 THEN '创建'
                        WHEN wo.status  = 1 THEN '处理中'
                        WHEN wo.status  = 2 THEN '已完成'
                        ELSE '未知'
                    END AS 状态,
                    content as "内容"
                    FROM `work_order` wo 
                    where wo.create_at >=  '{}'
                    and wo.create_at <  '{}'
    '''.format(str(month_start), str(time_end))

order_Pakistan_df = config.pakistan_fox_engine_read(sql_order_Pakistan)
# 通时计算
sql_call_Pakistan = '''
                SELECT
                se.dunner_id as user_id,
                se.dunner_name,
                se.dunner_asset_group_name,
                se.dunner_leader_name,
                # se.dunner_manager_name,
                ch.call_at,
                ch.code_debtor_phone_number 债务人电话,
                ch.enc_debtor_phone_number 密文,
                se.`asset_item_number`,
                se.debtor_relationship,
                ch.call_time 通话时长,
                ch.dial_time 振铃时长 
            FROM
                call_history ch
                LEFT JOIN call_history_extend se ON ch.id = se.source_id 
            WHERE
                ch.`call_at` >=  '{}'
                and ch.`call_at`<'{}'
                AND ch.call_channel = 1 -- 呼出
                '''.format(str(time_start), str(time_end))
call_Pakistan_df = config.pakistan_audit_engine_read(sql_call_Pakistan)

call_Pakistan_df = pd.merge(call_Pakistan_df, uo_Pakistan_df[['user_id', '主管']], how="left", on='user_id')
del call_Pakistan_df['user_id']
out_path = r"C:\自动化数据存放\稽核\日报汇总" + "\\"
writer_Pakistan = pd.ExcelWriter(out_path + "稽核数据巴基斯坦{}.xlsx".format(str(time_start)[0:10]), engine='xlsxwriter')
uo_Pakistan_df.to_excel(writer_Pakistan, sheet_name='巴基斯坦稽核架构', index=False)
input_Pakistan_df.to_excel(writer_Pakistan, sheet_name='巴基斯坦稽核入催', index=False)
order_Pakistan_df.to_excel(writer_Pakistan, sheet_name='巴基斯坦本月工单明细', index=False)
call_Pakistan_df.to_excel(writer_Pakistan, sheet_name='巴基斯坦稽核通时', index=False)
writer_Pakistan.save()  # 此语句不可少，否则本地文件未保存

# ### 菲律宾数据

# =============================================================================
# 字段计算
# =============================================================================
# 菲律宾核架构
sql_uo_Philippines = '''
                SELECT
                    SUBSTRING_INDEX( uo.`asset_group_name`, ',',- 1 ) '组别',
                    uo.office_name,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(uo.`parent_user_names`,',',2),',',-1) '主管',
                    cc.TL_NO,
                    uo.parent_user_name '组长',
                    b.co_NO,
                    uo.`name`,
                    uo.user_id,
                    c.dimission_date,
                    d.online_day 
                FROM
                    arcticfox.`user_organization` uo
                    LEFT JOIN ( SELECT DISTINCT NAME, NO AS TL_NO FROM sys_user ) AS cc ON uo.parent_user_name = cc.
                    NAME LEFT JOIN ( SELECT DISTINCT id, NO AS co_NO FROM sys_user ) AS b ON uo.user_id = b.id
                    LEFT JOIN ( SELECT DISTINCT user_id, dimission_date 
                    FROM sys_user_extend ) AS c ON uo.user_id = c.user_id
                    LEFT JOIN ( SELECT DISTINCT user_id, min( online_day ) online_day 
                    FROM online_days WHERE online_day_flag = 1 GROUP BY user_id ) AS d ON uo.user_id = d.user_id
    '''

uo_Philippines_df = config.philippines_fox_engine_read(sql_uo_Philippines)

# 入催户数
sql_input_Philippines = '''
                    SELECT
                    date( cr.`create_at` ) date,
                    `asset_from_app`,
                    count( DISTINCT da.debtor_id ) 户数 
                FROM
                    `collect_recovery` cr
                    JOIN asset a ON cr.asset_id = a.asset_id
                    JOIN debtor_asset da ON a.asset_id = da.asset_id 
                WHERE
                    cr.`batch_num` IS NULL 
                    AND cr.`late_days` = 1 
                    AND cr.`create_at` >= '{}'
                    AND cr.`create_at` < '{}'
                GROUP BY
                    1,2
    '''.format(str(time_start), str(time_end))

input_Philippines_df = config.philippines_fox_engine_read(sql_input_Philippines)

# 工单信息
sql_order_Philippines = '''
                    SELECT 
                    "菲律宾" as 国家,
                    wo.create_at as "创建日期",
                    wo.update_at as "最近更新时间",
                    if(wo.create_at <>wo.update_at, 
                    if(wo.status =2,
                    timestampdiff(
                            DAY,
                        DATE ( wo.create_at ),
                        DATE ( wo.update_at )),
                    timestampdiff(
                            DAY,
                        DATE ( wo.update_at ),
                        DATE ( now() ))),
                    timestampdiff(
                            DAY,
                        DATE ( wo.create_at),
                        DATE ( now() )))
                    相差天数,
                    asset_item_number as "资产编号",
                    debtor_name as "债务人姓名",
                    handle_sys_user_name as '处理人姓名',
                    CASE
                      WHEN type = 0 THEN '协商还款'
                        WHEN type = 1 THEN '协商还款(加急)'
                        WHEN type = 2 THEN '投诉'
                        WHEN type = 3 THEN '投诉倾向'
                      ELSE type 
                    END AS 进件类型,
                    CASE
                      WHEN wo.status  = 0 THEN '创建'
                        WHEN wo.status  = 1 THEN '处理中'
                        WHEN wo.status  = 2 THEN '已完成'
                        ELSE '未知'
                    END AS 状态,
                    content as "内容"
                    FROM `work_order` wo 
                    where wo.create_at >=  '{}'
                    and wo.create_at <  '{}'
    '''.format(str(month_start), str(time_end))

order_Philippines_df = config.philippines_fox_engine_read(sql_order_Philippines)
# 通时计算
sql_call_Philippines = '''
                SELECT
                 se.dunner_id as user_id,
                se.dunner_name,
                se.dunner_asset_group_name,
                se.dunner_leader_name,
                # se.dunner_manager_name,
                ch.call_at,
                ch.code_debtor_phone_number 债务人电话,
                ch.enc_debtor_phone_number 密文,
                se.`asset_item_number`,
                se.debtor_relationship,
                ch.call_time 通话时长,
                ch.dial_time 振铃时长 
            FROM
                call_history ch
                LEFT JOIN call_history_extend se ON ch.id = se.source_id 
            WHERE
                ch.`call_at` >=  '{}'
                and ch.`call_at`<'{}'
                AND ch.call_channel = 1 -- 呼出
                '''.format(str(time_start), str(time_end))
call_Philippines_df = config.philippines_audit_engine_read(sql_call_Philippines)

call_Philippines_df = pd.merge(call_Philippines_df, uo_Philippines_df[['user_id', '主管']], how="left", on='user_id')
del call_Philippines_df['user_id']

# 电销通话明细
call_Philippines_tmk_sql = '''SELECT
                            tche.asset_type_group_name `分案队列名称`,
                            tche.leader_name `组长`,
                            tche.dunner_name `坐席`,
                            tch.dial_time `拨打时间`,
                            tche.enc_customer_phone `客户电话`,
                            tcr.task_number `任务编号`,
                            tch.talk_duration `通话时长`,
                            tch.dial_duration `振铃时长` 
                        FROM
                            tel_sale_call_history tch
                            LEFT JOIN tel_sale_call_history_extend tche ON tch.id = tche.source_id
                            LEFT JOIN tel_sale_contact_record tcr ON tch.call_uuid = tcr.call_uuid 
                        WHERE
                            tch.dial_time >= '{0}' 
                            and tch.dial_time <'{1}' 
                               '''.format(str(time_start), str(time_end))
call_Philippines_tmk_df = config.philippines_fox_engine_read(call_Philippines_tmk_sql)

writer_Philippines = pd.ExcelWriter(out_path + "稽核数据菲律宾{}.xlsx".format(str(time_start)[0:10]), engine='xlsxwriter')
uo_Philippines_df.to_excel(writer_Philippines, sheet_name='菲律宾稽核架构', index=False)
input_Philippines_df.to_excel(writer_Philippines, sheet_name='菲律宾稽核入催', index=False)
order_Philippines_df.to_excel(writer_Philippines, sheet_name='菲律宾本月工单明细', index=False)
call_Philippines_df.to_excel(writer_Philippines, sheet_name='菲律宾稽核通时', index=False)
call_Philippines_tmk_df.to_excel(writer_Philippines, sheet_name='菲律宾电销通话明细', index=False)
writer_Philippines.save()  # 此语句不可少，否则本地文件未保存

# ### 泰国数据

# =============================================================================
# 字段计算
# =============================================================================
# 稽核架构
sql_uo_Thailand = '''
                SELECT
                    SUBSTRING_INDEX( uo.`asset_group_name`, ',',- 1 ) '组别',
                    uo.office_name,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(uo.`parent_user_names`,',',2),',',-1) '主管',
                    cc.TL_NO,
                    uo.parent_user_name '组长',
                    b.co_NO,
                    uo.`name`,
                    uo.user_id,
                    c.dimission_date,
                    d.online_day 
                FROM
                    arcticfox.`user_organization` uo
                    LEFT JOIN ( SELECT DISTINCT NAME, NO AS TL_NO FROM sys_user ) AS cc ON uo.parent_user_name = cc.
                    NAME LEFT JOIN ( SELECT DISTINCT id, NO AS co_NO FROM sys_user ) AS b ON uo.user_id = b.id
                    LEFT JOIN ( SELECT DISTINCT user_id, dimission_date 
                    FROM sys_user_extend ) AS c ON uo.user_id = c.user_id
                    LEFT JOIN ( SELECT DISTINCT user_id, min( online_day ) online_day 
                    FROM online_days WHERE online_day_flag = 1 GROUP BY user_id ) AS d ON uo.user_id = d.user_id
    '''

uo_Thailand_df = config.thailand_fox_engine_read(sql_uo_Thailand)

# 稽核架构
sql_small_eyes_Thailand = '''
                    SELECT
                        t.NAME AS '催员',
                        t.NO AS '工号',
                        aa.日期,
                        aa.号码 ,
                        aa.债务人ID,
                        aa.点击时间,
                        da.asset_item_number as '资产编号'

                    FROM
                        (
                        SELECT
                            DATE( create_at ) AS "日期",
                            create_at AS '点击时间',
                            create_user_id,
                            REPLACE ( SUBSTRING_INDEX( SUBSTRING_INDEX( content, ",", 1 ), ":",- 1 ), '"', '' )  AS "号码",
                            REPLACE ( SUBSTRING_INDEX( SUBSTRING_INDEX( content, ",",- 1 ), ":",- 1 ), '}}', '' )  AS '债务人ID' 
                        FROM
                            oper_business_log 
                        WHERE
                            1 = 1 
                            AND type = 'SHOW_DEBTOR_PHONE' 
                            AND create_at >= '{0}' 
                                    AND create_at < '{1}' 
                        ) aa
                        LEFT JOIN sys_user t ON aa.create_user_id = t.id
                        LEFT JOIN `debtor_asset` da  ON aa.债务人ID= da.debtor_id
'''.format(str(month_start), str(time_end))

small_eyes_Thailand_df = config.thailand_fox_engine_read(sql_small_eyes_Thailand)

# 入催户数
sql_input_Thailand = '''
                    SELECT
                    date( cr.`create_at` ) date,
                    `asset_from_app`,
                    count( DISTINCT da.debtor_id ) 户数 
                FROM
                    `collect_recovery` cr
                    JOIN asset a ON cr.asset_id = a.asset_id
                    JOIN debtor_asset da ON a.asset_id = da.asset_id 
                WHERE
                    cr.`batch_num` IS NULL 
                    AND cr.`late_days` = 1 
                    AND cr.`create_at` >= '{}'
                    AND cr.`create_at` < '{}'
                GROUP BY
                    1,2
    '''.format(str(time_start), str(time_end))

input_Thailand_df = config.thailand_fox_engine_read(sql_input_Thailand)

# 工单信息
sql_order_Thailand = '''
                    SELECT 
                    "泰国" as 国家,
                    wo.create_at as "创建日期",
                    wo.update_at as "最近更新时间",
                    if(wo.create_at <>wo.update_at, 
                    if(wo.status =2,
                    timestampdiff(
                            DAY,
                        DATE ( wo.create_at ),
                        DATE ( wo.update_at )),
                    timestampdiff(
                            DAY,
                        DATE ( wo.update_at ),
                        DATE ( now() ))),
                    timestampdiff(
                            DAY,
                        DATE ( wo.create_at),
                        DATE ( now() )))
                    相差天数,
                    asset_item_number as "资产编号",
                    debtor_name as "债务人姓名",
                    handle_sys_user_name as '处理人姓名',
                    CASE
                      WHEN type = 0 THEN '协商还款'
                        WHEN type = 1 THEN '协商还款(加急)'
                        WHEN type = 2 THEN '投诉'
                        WHEN type = 3 THEN '投诉倾向'
                      ELSE type 
                    END AS 进件类型,
                    CASE
                      WHEN wo.status  = 0 THEN '创建'
                        WHEN wo.status  = 1 THEN '处理中'
                        WHEN wo.status  = 2 THEN '已完成'
                        ELSE '未知'
                    END AS 状态,
                    content as "内容"
                    FROM `work_order` wo 
                    where wo.create_at >=  '{}'
                    and wo.create_at <  '{}'
    '''.format(str(month_start), str(time_end))

order_Thailand_df = config.thailand_fox_engine_read(sql_order_Thailand)
# 通时计算
sql_call_Thailand = '''
                SELECT
                se.dunner_id as user_id,
                se.dunner_name,
                se.dunner_asset_group_name,
                se.dunner_leader_name,
                # se.dunner_manager_name,
                ch.call_at,
                SUBSTRING_INDEX(call_recording_url,'/',-1) '录音地址',	
                ch.code_debtor_phone_number 债务人电话,
                ch.enc_debtor_phone_number 密文,
                se.`asset_item_number`,
                se.debtor_relationship,
                ch.call_time 通话时长,
                ch.dial_time 振铃时长 
            FROM
                call_history ch
                LEFT JOIN call_history_extend se ON ch.id = se.source_id 
            WHERE
                ch.`call_at` >=  '{}'
                and ch.`call_at`<'{}'
                AND ch.call_channel = 1 -- 呼出
                '''.format(str(time_start), str(time_end))
call_Thailand_df = config.thailand_audit_engine_read(sql_call_Thailand)

call_Thailand_df = pd.merge(call_Thailand_df, uo_Thailand_df[['user_id', '主管']], how="left", on='user_id')
del call_Thailand_df['user_id']



# 电销通话明细
call_Thailand_tmk_sql = '''SELECT
                            tche.asset_type_group_name `分案队列名称`,
                            tche.leader_name `组长`,
                            tche.dunner_name `坐席`,
                            tch.dial_time `拨打时间`,
                            tche.enc_customer_phone `客户电话`,
                            tcr.task_number `任务编号`,
                            tch.talk_duration `通话时长`,
                            tch.dial_duration `振铃时长` 
                        FROM
                            tel_sale_call_history tch
                            LEFT JOIN tel_sale_call_history_extend tche ON tch.id = tche.source_id
                            LEFT JOIN tel_sale_contact_record tcr ON tch.call_uuid = tcr.call_uuid 
                        WHERE
                            tch.dial_time >= '{0}' 
                            and tch.dial_time <'{1}' 
                               '''.format(str(time_start), str(time_end))
call_Thailand_tmk_df = config.thailand_fox_engine_read(call_Thailand_tmk_sql)

#
# call_Thailand_tmk = '''
#                         SELECT
#                         ug.user_group_name `电销分案队列`,
#                         co.user_name `坐席`,
#                         FROM_UNIXTIME( tp.task_process_dialing_at, '%Y-%m-%d %H:%i:%s' ) `拨打时间`,
#                         t.task_id `任务编号`,
#                         tp.task_process_duration `通话时长`,
#                     IF
#                         ( tp.task_process_duration > 0, tp.task_process_establish_at - tp.task_process_dialing_at, tp.task_process_release_at - tp.task_process_dialing_at ) `振铃时长`
#                     FROM
#                         task_process tp
#                         LEFT JOIN task t ON tp.task_process_task_id = t.task_id
#                         LEFT JOIN task_business tb ON t.task_id = tb.task_business_task_id
#                         LEFT JOIN `user` co ON t.task_flow_user_id = co.user_id
#                         LEFT JOIN user_group ug ON co.user_user_group_id = ug.user_group_id
#                     WHERE
#                         FROM_UNIXTIME( tp.task_process_dialing_at, '%Y-%m-%d' ) = '{}'
#                 '''.format(str(time_start)[0:10])
# call_Thailand_tmk_df = config.thailand_tmk_engine_read(call_Thailand_tmk)

writer_Thailand = pd.ExcelWriter(out_path + "稽核数据泰国{}.xlsx".format(str(time_start)[0:10]), engine='xlsxwriter')
uo_Thailand_df.to_excel(writer_Thailand, sheet_name='泰国稽核架构', index=False)
input_Thailand_df.to_excel(writer_Thailand, sheet_name='泰国稽核入催', index=False)
order_Thailand_df.to_excel(writer_Thailand, sheet_name='泰国本月工单明细', index=False)
call_Thailand_df.to_excel(writer_Thailand, sheet_name='泰国稽核通时', index=False)
small_eyes_Thailand_df.to_excel(writer_Thailand, sheet_name='泰国小眼睛点击明细', index=False)
call_Thailand_tmk_df.to_excel(writer_Thailand, sheet_name='泰国电销通话明细', index=False)
writer_Thailand.save()  # 此语句不可少，否则本地文件未保存

### 印度尼西亚

# 稽核架构
sql_uo_indonesia = '''
                SELECT
                    SUBSTRING_INDEX( uo.`asset_group_name`, ',',- 1 ) '组别',
                    uo.office_name,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(uo.`parent_user_names`,',',2),',',-1) '主管',
                    cc.TL_NO,
                    uo.parent_user_name '组长',
                    b.co_NO,
                    uo.`name`,
                    uo.user_id,
                    c.dimission_date,
                    d.online_day 
                FROM
                    arcticfox.`user_organization` uo
                    LEFT JOIN ( SELECT DISTINCT NAME, NO AS TL_NO FROM sys_user ) AS cc ON uo.parent_user_name = cc.
                    NAME LEFT JOIN ( SELECT DISTINCT id, NO AS co_NO FROM sys_user ) AS b ON uo.user_id = b.id
                    LEFT JOIN ( SELECT DISTINCT user_id, dimission_date 
                    FROM sys_user_extend ) AS c ON uo.user_id = c.user_id
                    LEFT JOIN ( SELECT DISTINCT user_id, min( online_day ) online_day 
                    FROM online_days WHERE online_day_flag = 1 GROUP BY user_id ) AS d ON uo.user_id = d.user_id
    '''
uo_indonesia_df = config.indonesia_fox_engine_read(sql_uo_indonesia)

# 入催户数
sql_input_indonesia = '''
                    SELECT
                    date( cr.`create_at` ) date,
                    `asset_from_app`,
                    count( DISTINCT da.debtor_id ) 户数 
                FROM
                    `collect_recovery` cr
                    JOIN asset a ON cr.asset_id = a.asset_id
                    JOIN debtor_asset da ON a.asset_id = da.asset_id 
                WHERE
                    cr.`batch_num` IS NULL 
                    AND cr.`late_days` = 1 
                    AND cr.`create_at` >= '{}'
                    AND cr.`create_at` < '{}'
                GROUP BY
                    1,2
    '''.format(str(time_start), str(time_end))

input_indonesia_df = config.indonesia_fox_engine_read(sql_input_indonesia)

# 工单信息
sql_order_indonesia = '''
                    SELECT 
                    "印尼" as 国家,
                    wo.create_at as "创建日期",
                    wo.update_at as "最近更新时间",
                    if(wo.create_at <>wo.update_at, 
                    if(wo.status =2,
                    timestampdiff(
                            DAY,
                        DATE ( wo.create_at ),
                        DATE ( wo.update_at )),
                    timestampdiff(
                            DAY,
                        DATE ( wo.update_at ),
                        DATE ( now() ))),
                    timestampdiff(
                            DAY,
                        DATE ( wo.create_at),
                        DATE ( now() )))
                    相差天数,
                    asset_item_number as "资产编号",
                    debtor_name as "债务人姓名",
                    handle_sys_user_name as '处理人姓名',
                    CASE
                      WHEN type = 0 THEN '协商还款'
                        WHEN type = 1 THEN '协商还款(加急)'
                        WHEN type = 2 THEN '投诉'
                        WHEN type = 3 THEN '投诉倾向'
                      ELSE type 
                    END AS 进件类型,
                    CASE
                      WHEN wo.status  = 0 THEN '创建'
                        WHEN wo.status  = 1 THEN '处理中'
                        WHEN wo.status  = 2 THEN '已完成'
                        ELSE '未知'
                    END AS 状态,
                    content as "内容"
                    FROM `work_order` wo 
                    where wo.create_at >=  '{}'
                    and wo.create_at <  '{}'
    '''.format(str(month_start), str(time_end))

order_indonesia_df = config.indonesia_fox_engine_read(sql_order_indonesia)

# 通时计算
sql_call_indonesia = '''
                SELECT
                se.dunner_id as user_id,
                se.dunner_name,
                se.dunner_asset_group_name,
                se.dunner_leader_name,
                # se.dunner_manager_name,
                ch.call_at,
                ch.code_debtor_phone_number 债务人电话,
                ch.enc_debtor_phone_number 密文,
                se.`asset_item_number`,
                se.debtor_relationship,
                ch.call_time 通话时长,
                ch.dial_time 振铃时长 
            FROM
                call_history ch
                LEFT JOIN call_history_extend se ON ch.id = se.source_id 
            WHERE
                ch.`call_at` >=  '{}'
                and ch.`call_at`<'{}'
                AND ch.call_channel = 1 -- 呼出
                '''.format(str(time_start), str(time_end))
call_indonesia_df = config.indonesia_audit_engine_read(sql_call_indonesia)

call_indonesia_df = pd.merge(call_indonesia_df, uo_indonesia_df[['user_id', '主管']], how="left", on='user_id')
del call_indonesia_df['user_id']

# 电销通话明细
call_indonesia_tmk_sql = '''SELECT
                            tche.asset_type_group_name `分案队列名称`,
                            tche.leader_name `组长`,
                            tche.dunner_name `坐席`,
                            tch.dial_time `拨打时间`,
                            tche.enc_customer_phone `客户电话`,
                            tcr.task_number `任务编号`,
                            tch.talk_duration `通话时长`,
                            tch.dial_duration `振铃时长` 
                        FROM
                            tel_sale_call_history tch
                            LEFT JOIN tel_sale_call_history_extend tche ON tch.id = tche.source_id
                            LEFT JOIN tel_sale_contact_record tcr ON tch.call_uuid = tcr.call_uuid 
                        WHERE
                            tch.dial_time >= '{0}' 
                            and tch.dial_time <'{1}' 
                               '''.format(str(time_start), str(time_end))
call_indonesia_tmk_df = config.indonesia_fox_engine_read(call_indonesia_tmk_sql)

tel_indonesia_wa_sql = '''SELECT 
                            su.name 催员,
                            su.no,
                            date(t.create_at) 点击日期 ,
                            t.create_at 点击时间 ,
                            t.title type,
                            t.content task_number
                            FROM
                        oper_business_log t,
                        sys_user su 
                WHERE
                        t.create_user_id = su.id 
                        AND t.create_at >= '{0}' 
                        AND t.create_at < '{1}' 
                        AND t.title LIKE '%TEL_SALE_WHATSAPP_LINK%'
                               '''.format(str(month_start), str(time_end))
tel_indonesia_wa_df = config.indonesia_fox_engine_read(tel_indonesia_wa_sql)

indonesia_wa_sql = '''SELECT 
                    su.name 催员,
                    DATE(t.create_date) 日期,
                    t.create_date 点击时间 ,
                    debtor_id 债务人ID ,
                    enc_phone as 手机号密文 ,
                     (CASE                             
                            WHEN REPLACE ( JSON_EXTRACT( t.params, '$.relationType' ), '"', '' ) = 'emergency' THEN
                            'other' 
                             WHEN REPLACE ( JSON_EXTRACT( t.params, '$.relationType' ), '"', '' ) = 'third_party' THEN
                             'other' 
                              WHEN REPLACE ( JSON_EXTRACT( t.params, '$.relationType' ), '"', '' ) = 'self' THEN
                              'self' ELSE 'TEL' 
                                            END 
                                            ) 类型
                    FROM `whatsapp_session_log` t,
                         sys_user su 
                                    WHERE
                    t.dunner_id = su.id 
                     and status =1
                                            AND t.create_date >='{0}'
                                            AND t.create_date < '{1}' 
                               '''.format(str(month_start), str(time_end))
indonesia_wa_df = config.indonesia_fox_engine_read(indonesia_wa_sql)

writer_indonesia = pd.ExcelWriter(out_path + "稽核数据印尼{}.xlsx".format(str(time_start)[0:10]), engine='xlsxwriter')
uo_indonesia_df.to_excel(writer_indonesia, sheet_name='印尼稽核架构', index=False)
input_indonesia_df.to_excel(writer_indonesia, sheet_name='印尼稽核入催', index=False)
order_indonesia_df.to_excel(writer_indonesia, sheet_name='印尼本月工单明细', index=False)
call_indonesia_df.to_excel(writer_indonesia, sheet_name='印尼稽核通时', index=False)
call_indonesia_tmk_df.to_excel(writer_indonesia, sheet_name='印尼电销通话明细', index=False)
tel_indonesia_wa_df.to_excel(writer_indonesia, sheet_name='印尼电销WA明细', index=False)
indonesia_wa_df.to_excel(writer_indonesia, sheet_name='印尼电催WA明细', index=False)
writer_indonesia.save()  # 此语句不可少，否则本地文件未保存

# =============================================================================
# 邮件发送
# =============================================================================

import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart

error = '\n异常内容：'

##### 配置区  #####
mail_host = 'smtp.exmail.qq.com'
mail_port = '465'  # Linux平台上面发
receiver_lst = ['yeyuhao@weidu.ac.cn', 'huxiaoqing@weidu.ac.cn', 'yuzheng@weidu.ac.cn', 'lichi@weidu.ac.cn',
                'zhangjinyou@weidu.ac.cn', 'lijingyi@weidu.ac.cn', 'xiejinhui@weidu.ac.cn']

# 发件人邮箱账号
login_sender = 'shwd_operation@weidu.ac.cn'
# 发件人邮箱授权码而不是邮箱密码，授权码由邮箱官网可设置生成
login_pass = config.login_pass
# 邮箱文本内容

mail_msg = '''
    时间范围{}--->{}。
    \n数据请见附件！
    '''.format(str(time_start)[:10], str(time_end - datetime.timedelta(1))[:10])
# 发送者
sendName = "数据组"
# 接收者
resName = ""
# 邮箱正文标题
title = "稽核日报印巴:" + str(time_start)[:10]

######### end  ##########

msg = MIMEMultipart()
msg['From'] = formataddr([sendName, login_sender])
# 邮件的标题
msg['Subject'] = title
msg.attach(MIMEText(mail_msg, 'html', 'utf-8'))  # 添加正文


att3 = MIMEText(open(out_path + "稽核数据巴基斯坦{}.xlsx".format(str(time_start)[0:10]), 'rb').read(), 'base64',
                'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att3["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att3.add_header("Content-Disposition", "attachment", filename=("gbk", "", "稽核数据巴基斯坦.xlsx"))
# att1["Content-Disposition"] = 'attachment;filename="data.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att3)  # 将附件添加到邮件内容当中

att4 = MIMEText(open(out_path + "稽核数据印尼{}.xlsx".format(str(time_start)[0:10]), 'rb').read(), 'base64',
                'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att4["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att4.add_header("Content-Disposition", "attachment", filename=("gbk", "", "稽核数据印尼.xlsx"))
# att1["Content-Disposition"] = 'attachment;filename="data.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att4)  # 将附件添加到邮件内容当中
# 服务器
server = smtplib.SMTP_SSL(mail_host, mail_port)
server.login(login_sender, login_pass)
server.sendmail(login_sender, receiver_lst, msg.as_string())
print("已发送到:\n" + ';\n'.join(receiver_lst) + "的邮箱中！")
server.quit()




##### 配置区  #####
mail_host = 'smtp.exmail.qq.com'
mail_port = '465'  # Linux平台上面发
receiver_lst = ['yeyuhao@weidu.ac.cn', 'huxiaoqing@weidu.ac.cn', 'yuzheng@weidu.ac.cn', 'lichi@weidu.ac.cn',
                'zhangjinyou@weidu.ac.cn', 'lijingyi@weidu.ac.cn', 'xiejinhui@weidu.ac.cn']

# 发件人邮箱账号
login_sender = 'shwd_operation@weidu.ac.cn'
# 发件人邮箱授权码而不是邮箱密码，授权码由邮箱官网可设置生成
login_pass = config.login_pass
# 邮箱文本内容

mail_msg = '''
    时间范围{}--->{}。
    \n数据请见附件！
    '''.format(str(time_start)[:10], str(time_end - datetime.timedelta(1))[:10])
# 发送者
sendName = "数据组"
# 接收者
resName = ""
# 邮箱正文标题
title = "稽核日报泰菲:" + str(time_start)[:10]

######### end  ##########

msg = MIMEMultipart()
msg['From'] = formataddr([sendName, login_sender])
# 邮件的标题
msg['Subject'] = title
msg.attach(MIMEText(mail_msg, 'html', 'utf-8'))  # 添加正文

att1 = MIMEText(open(out_path + "稽核数据泰国{}.xlsx".format(str(time_start)[0:10]), 'rb').read(), 'base64',
                'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att1["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att1.add_header("Content-Disposition", "attachment", filename=("gbk", "", "稽核数据泰国.xlsx"))
# att1["Content-Disposition"] = 'attachment;filename="data.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att1)  # 将附件添加到邮件内容当中

att2 = MIMEText(open(out_path + "稽核数据菲律宾{}.xlsx".format(str(time_start)[0:10]), 'rb').read(), 'base64',
                'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att2["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att2.add_header("Content-Disposition", "attachment", filename=("gbk", "", "稽核数据菲律宾.xlsx"))
# att1["Content-Disposition"] = 'attachment;filename="data.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att2)  # 将附件添加到邮件内容当中

# 服务器
server = smtplib.SMTP_SSL(mail_host, mail_port)
server.login(login_sender, login_pass)
server.sendmail(login_sender, receiver_lst, msg.as_string())
print("已发送到:\n" + ';\n'.join(receiver_lst) + "的邮箱中！")
server.quit()

# =============================================================================
# 清空内存
# =============================================================================

import gc


def clean_variables():
    variables = list(globals().keys()).copy()
    cannot_delete = ['gc']
    for key in variables:
        try:
            if (key[:1] != '_') and (key not in cannot_delete):
                globals().pop(key)  # 删除变量
                gc.collect()  # 清理内存
        except:
            pass


clean_variables()  # sys.getsizeof(combine)