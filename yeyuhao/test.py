#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import datetime
import warnings
import excelFormat
import config

warnings.filterwarnings("ignore")
print("开始运行：", datetime.datetime.now())
time_start = datetime.datetime.now().replace(year=2024, month=1, day=1)  # 数据开始日期
# time_end = datetime.datetime.now().replace(year=2024, month=1, day=2)  # 数据截止日期+1day
debtor_sql = '''select debtor_id , asset_item_number from `ods_fox_debtor_asset` where create_at>'2023-05-01 00:00:00' '''
debtor_id_df = config.chinese_bd_engine_read(debtor_sql)

def finish(time_start):
    df = pd.read_excel(
        r"C:\Users\user\Documents\WXWork\1688854640449737\Cache\File\2024-08\长银24年1至7月逾期名单\长银24年1至7月逾期名单\快牛KUN_20240101-20240731\快牛{}.xlsx".format(
            str(time_start)[0:10]))
    df = df[df['逾期天数'] <= 3]
    df = df[['统计日期', '外部订单号']]
    df.rename(columns={"外部订单号": "asset_item_number"}, inplace=True)
    df = pd.merge(df, debtor_id_df, how='left', on='asset_item_number')
    call_sql = '''
            SELECT
                a.`通话时间`,
                a.`被叫号码密文`,
                a.`通话类型`,
                b.`债务人`,
                user_no.`员工工号`,
                leader_no.`组长工号`,
                b.debtor_id 
            FROM
                (
                SELECT
                    call_at AS '通话时间',
                    id,
                    dunner_id AS '催员ID',
                    '呼出' AS '通话类型',
                    enc_debtor_phone_number AS '被叫号码密文' 
                FROM
                    ods_audit_call_history 
                WHERE
                    call_channel = 1 
                    AND create_at >= '{0}' 
                    AND create_at < '{1}' ) a 
                LEFT JOIN 
                    ( SELECT source_id, debtor_id, code_debtor_name AS '债务人', dunner_leader_id AS '组长ID' FROM ods_audit_call_history_extend WHERE 
                    create_at >= '{0}' 
                    AND create_at < '{1}'
                    and debtor_relationship = 'self'
                ) b ON a.id = b.source_id
                LEFT JOIN ( SELECT id AS '催员ID', NO AS '员工工号' FROM ods_fox_sys_user ) user_no ON a.`催员ID` = user_no.`催员ID`
                LEFT JOIN ( SELECT id AS '催员ID', NO AS '组长工号' FROM ods_fox_sys_user ) leader_no ON b.`组长ID` = leader_no.`催员ID`
    '''.format(str(time_start)[0:10], str(time_start + datetime.timedelta(1))[0:10])
    call_df = config.chinese_bd_engine_read(call_sql)
    df = pd.merge(df, call_df, how='inner', on='debtor_id')
    df = df.groupby('debtor_id').head(3).reset_index(drop=True)
    return df
df = finish(time_start)

for i in range(1,31):
    df1=finish(time_start + datetime.timedelta(i))
    df = pd.concat([df, df1])



df = df[['通话时间', '员工工号', '组长工号', '被叫号码密文', '通话类型', '债务人']]
df.to_excel( r"C:\Users\user\Documents\WXWork\1688854640449737\Cache\File\2024-08\长银24年1至7月逾期名单\长银24年1至7月逾期名单\快牛KUN_20240101-20240731\2.xlsx", index=False)
excelFormat.beautify_excel(r"C:\Users\user\Documents\WXWork\1688854640449737\Cache\File\2024-08\长银24年1至7月逾期名单\长银24年1至7月逾期名单\快牛KUN_20240101-20240731\2.xlsx")