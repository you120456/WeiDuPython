#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import mysql.connector
import datetime
import warnings
import config

warnings.filterwarnings("ignore")
print("开始运行：", datetime.datetime.now())
now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

time_start = now.replace(year=2024, month=4, day=19)  # 数据开始日期
time_end = now.replace(year=2024, month=4, day=26, hour=0, minute=0, second=0, microsecond=0)  # 数据截止日期+1day
# time_end = time_end.replace(hour=9, minute=0, second=0, microsecond=0)
month_start = time_start.replace(day=1)  # 本月初开始日期

print('周报数据时间范围{}--->{}'.format(str(time_start)[:10],
                                str(time_end - datetime.timedelta(1))[:10]))
# =============================================================================
# 数据导入
# # =============================================================================
sql = '''
        SELECT
                DATE( ch.call_at ) AS '拨打日期',
                HOUR(ch.call_at) '拨打时段',
                ch.dunner_id "催员ID",
                phone_line AS '线路号',
                ce.debtor_relationship AS '债务人关系',
                ch.call_at '拨打时间',
                ch.enc_debtor_phone_number '拨打号码',
                IF( ch.call_time > 0, 1, 0 )AS '是否接通' ,
                ch.call_time  as '通话时长'   ,
                ch.dial_time as '振铃时长'   
        FROM
                call_history ch
                JOIN call_history_extend ce ON ch.id = ce.source_id 
        WHERE
                ch.call_at >= '{0}'
                and ch.call_at < '{1}'
                AND ch.call_channel = 1 
         '''.format(str(time_start), str(time_end))
finish_df = config.chinese_audit_engine_read(sql)

china_uo_sql = '''
            SELECT
            DATE(stat_time) 拨打日期,
            SUBSTRING_INDEX(uo.`asset_group_name`,',',-1) '组别',
            uo.`name` 催员,
            uo.user_id 催员ID
            FROM  `bi_user_organization` uo
            LEFT JOIN sys_user a on uo.user_id=a.id
            WHERE uo.`asset_group_name` IS NOT NULL
            and stat_time >= '{0}'
'''.format(str(time_start))

china_uo_df = config.chinese_bi_engine_read(china_uo_sql)


finish_df = pd.merge(finish_df, china_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
call_data = finish_df.groupby(by=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count",
                                                                                               "是否接通": "sum", "通话时长": "sum", '振铃时长': "sum"})
call_data = call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
call_data['接通率'] = round(call_data['接通次数'] / call_data['拨打次数'], 4)
finish_df = finish_df.sort_values(by=['拨打时间'])
finish_df = finish_df.drop_duplicates(subset=['拨打日期','线路号', '债务人关系', '拨打号码'], keep='first')
finish_df['国家'] = '中国'
finish_df['类别'] = '电催'
finish_df = finish_df.groupby(by=['国家', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count",
                                                                  "是否接通": "sum"})
finish_df = finish_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
finish_df['首拨接通率'] = round(finish_df['接通号码数'] / finish_df['拨打号码数'], 4)
finish_df = pd.merge(finish_df, call_data, on=['拨打日期', '组别', '拨打时段',  '线路号', '债务人关系'], how='left')


uo_sql = '''
SELECT
            uo.work_day 拨打日期,
            uo.asset_group_name '组别',
            uo.`user_name` 催员,
            uo.user_id 催员ID
            FROM  `collect_attendance_dtl` uo
            WHERE uo.asset_group_name IS NOT NULL
            and work_day >= '{0}'
'''.format(str(time_start))


philippines_df = config.philippines_audit_engine_read(sql)
philippines_uo_df = config.philippines_fox_engine_read(uo_sql)
philippines_df = pd.merge(philippines_df, philippines_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
philippines_call_data = philippines_df.groupby(by=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count","是否接通": "sum", "通话时长": "sum"
                                                                                                                , '振铃时长': "sum"})
philippines_call_data = philippines_call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
philippines_call_data['接通率'] = round(philippines_call_data['接通次数'] / philippines_call_data['拨打次数'], 4)
philippines_df = philippines_df.sort_values(by=['拨打时间'])
philippines_df = philippines_df.drop_duplicates(subset=['拨打日期','线路号', '债务人关系', '拨打号码'], keep='first')
philippines_df['国家'] = '菲律宾'
philippines_df['类别'] = '电催'
philippines_df = philippines_df.groupby(by=['国家', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum"})
philippines_df = philippines_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
philippines_df['首拨接通率'] = round(philippines_df['接通号码数'] / philippines_df['拨打号码数'], 4)
philippines_df = pd.merge(philippines_df, philippines_call_data, on=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], how='left')
finish_df = pd.concat([finish_df, philippines_df])


thailand_df = config.thailand_audit_engine_read(sql)
thailand_uo_df = config.thailand_fox_engine_read(uo_sql)
thailand_df = pd.merge(thailand_df, thailand_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
thailand_call_data = thailand_df.groupby(by=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count","是否接通": "sum", "通话时长": "sum", '振铃时长': "sum"})
thailand_call_data = thailand_call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
thailand_call_data['接通率'] = round(thailand_call_data['接通次数'] / thailand_call_data['拨打次数'], 4)

thailand_df = thailand_df.sort_values(by=['拨打时间'])
thailand_df = thailand_df.drop_duplicates(subset=['拨打日期','线路号', '债务人关系', '拨打号码'], keep='first')
thailand_df['国家'] = '泰国'
thailand_df['类别'] = '电催'
thailand_df = thailand_df.groupby(by=['国家', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum"})
thailand_df = thailand_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
thailand_df['首拨接通率'] = round(thailand_df['接通号码数'] / thailand_df['拨打号码数'], 4)
thailand_df = pd.merge(thailand_df, thailand_call_data, on=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], how='left')
finish_df = pd.concat([finish_df, thailand_df])


mexico_df = config.mexico_audit_engine_read(sql)
mexico_uo_df = config.mexico_fox_engine_read(uo_sql)
mexico_df = pd.merge(mexico_df, mexico_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
mexico_call_data = mexico_df.groupby(by=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count","是否接通": "sum", "通话时长": "sum", '振铃时长': "sum"})
mexico_call_data = mexico_call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
mexico_call_data['接通率'] = round(mexico_call_data['接通次数'] / mexico_call_data['拨打次数'], 4)

mexico_df = mexico_df.sort_values(by=['拨打时间'])
mexico_df = mexico_df.drop_duplicates(subset=['拨打日期','线路号', '债务人关系', '拨打号码'], keep='first')
mexico_df['国家'] = '墨西哥'
mexico_df['类别'] = '电催'
mexico_df = mexico_df.groupby(by=['国家', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum"})
mexico_df = mexico_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
mexico_df['首拨接通率'] = round(mexico_df['接通号码数'] / mexico_df['拨打号码数'], 4)
mexico_df = pd.merge(mexico_df, mexico_call_data, on=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], how='left')
finish_df = pd.concat([finish_df, mexico_df])

indonesia_df = config.indonesia_audit_engine_read(sql)
indonesia_uo_df = config.indonesia_fox_engine_read(uo_sql)
indonesia_df = pd.merge(indonesia_df, indonesia_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
indonesia_call_data = indonesia_df.groupby(by=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count","是否接通": "sum", "通话时长": "sum", '振铃时长': "sum"})
indonesia_call_data = indonesia_call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
indonesia_call_data['接通率'] = round(indonesia_call_data['接通次数'] / indonesia_call_data['拨打次数'], 4)

indonesia_df = indonesia_df.sort_values(by=['拨打时间'])
indonesia_df = indonesia_df.drop_duplicates(subset=['拨打日期','线路号', '债务人关系', '拨打号码'], keep='first')
indonesia_df['国家'] = '印尼'
indonesia_df['类别'] = '电催'
indonesia_df = indonesia_df.groupby(by=['国家', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum"})
indonesia_df = indonesia_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
indonesia_df['首拨接通率'] = round(indonesia_df['接通号码数'] / indonesia_df['拨打号码数'], 4)
indonesia_df = pd.merge(indonesia_df, indonesia_call_data, on=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], how='left')
finish_df = pd.concat([finish_df, indonesia_df])

pakistan_df = config.pakistan_audit_engine_read(sql)
pakistan_uo_df = config.pakistan_fox_engine_read(uo_sql)
pakistan_df = pd.merge(pakistan_df, pakistan_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
pakistan_call_data = pakistan_df.groupby(by=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count","是否接通": "sum", "通话时长": "sum", '振铃时长': "sum"})
pakistan_call_data = pakistan_call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
pakistan_call_data['接通率'] = round(pakistan_call_data['接通次数'] / pakistan_call_data['拨打次数'], 4)

pakistan_df = pakistan_df.sort_values(by=['拨打时间'])
pakistan_df = pakistan_df.drop_duplicates(subset=['拨打日期','线路号', '债务人关系', '拨打号码'], keep='first')
pakistan_df['国家'] = '巴基斯坦'
pakistan_df['类别'] = '电催'
pakistan_df = pakistan_df.groupby(by=['国家', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum"})
pakistan_df = pakistan_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
pakistan_df['首拨接通率'] = round(pakistan_df['接通号码数'] / pakistan_df['拨打号码数'], 4)
pakistan_df = pd.merge(pakistan_df, pakistan_call_data, on=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], how='left')
finish_df = pd.concat([finish_df, pakistan_df])

################
# 电销
################
tel_sql = '''
        SELECT
                DATE( dial_time) AS '拨打日期',
                HOUR(dial_time) '拨打时段',
                phone_line AS '线路号',
                'self' AS '债务人关系',
                dial_time '拨打时间',
                callee_phone_number '拨打号码',
                IF( talk_duration > 0, 1, 0 )AS '是否接通'     ,
                tche.dunner_id as "催员ID",
                ch.dial_duration  '振铃时长' ,
                ch.talk_duration    '通话时长'            
            FROM
            tel_sale_call_history ch
            LEFT JOIN tel_sale_call_history_extend tche ON ch.id = tche.source_id 
        WHERE
                ch.dial_time >= '{0}'
                and dial_time < '{1}'
         '''.format(str(time_start), str(time_end))
tel_uo_sql = '''
SELECT
            uo.work_day 拨打日期,
            uo.asset_group_name '组别',
            uo.`user_name` 催员,
            uo.user_id 催员ID
            FROM  `collect_attendance_dtl` uo
            WHERE uo.asset_group_name IS NOT NULL
            and work_day >= '{0}'
'''.format(str(time_start))

tel_philippines_df = config.philippines_fox_engine_read(tel_sql)
tel_philippines_uo_df = config.philippines_fox_engine_read(tel_uo_sql)
tel_philippines_df = pd.merge(tel_philippines_df, tel_philippines_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
tel_philippines_call_data = tel_philippines_df.groupby(by=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count","是否接通": "sum", "通话时长": "sum", '振铃时长': "sum"})
tel_philippines_call_data = tel_philippines_call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
tel_philippines_call_data['接通率'] = round(tel_philippines_call_data['接通次数'] / tel_philippines_call_data['拨打次数'], 4)
tel_philippines_df = tel_philippines_df.sort_values(by=['拨打时间'])
tel_philippines_df = tel_philippines_df.drop_duplicates(subset=['拨打日期','线路号', '债务人关系', '拨打号码'], keep='first')
tel_philippines_df['国家'] = '菲律宾'
tel_philippines_df['类别'] = '电销'
tel_philippines_df = tel_philippines_df.groupby(by=['国家', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum"})
tel_philippines_df = tel_philippines_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
tel_philippines_df['首拨接通率'] = round(tel_philippines_df['接通号码数'] / tel_philippines_df['拨打号码数'], 4)
tel_philippines_df = pd.merge(tel_philippines_df, tel_philippines_call_data, on=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], how='left')
finish_df = pd.concat([finish_df, tel_philippines_df])

tel_mexico_df = config.mexico_fox_engine_read(tel_sql)
tel_mexico_uo_df = config.mexico_fox_engine_read(tel_uo_sql)
tel_mexico_df = pd.merge(tel_mexico_df, tel_mexico_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
tel_mexico_call_data = tel_mexico_df.groupby(by=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count","是否接通": "sum", "通话时长": "sum", '振铃时长': "sum"})
tel_mexico_call_data = tel_mexico_call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
tel_mexico_call_data['接通率'] = round(tel_mexico_call_data['接通次数'] / tel_mexico_call_data['拨打次数'], 4)
tel_mexico_df = tel_mexico_df.sort_values(by=['拨打时间'])
tel_mexico_df = tel_mexico_df.drop_duplicates(subset=['拨打日期','线路号', '债务人关系', '拨打号码'], keep='first')
tel_mexico_df['国家'] = '墨西哥'
tel_mexico_df['类别'] = '电销'
tel_mexico_df = tel_mexico_df.groupby(by=['国家', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum"})
tel_mexico_df = tel_mexico_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
tel_mexico_df['首拨接通率'] = round(tel_mexico_df['接通号码数'] / tel_mexico_df['拨打号码数'], 4)
tel_mexico_df = pd.merge(tel_mexico_df, tel_mexico_call_data, on=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], how='left')
finish_df = pd.concat([finish_df, tel_mexico_df])


### 泰国电销
tel_thailand_df = config.thailand_fox_engine_read(tel_sql)
tel_thailand_uo_df = config.thailand_fox_engine_read(tel_uo_sql)
tel_thailand_df = pd.merge(tel_thailand_df, tel_thailand_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
tel_thailand_call_data = tel_thailand_df.groupby(by=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count","是否接通": "sum", "通话时长": "sum", '振铃时长': "sum"})
tel_thailand_call_data = tel_thailand_call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
tel_thailand_call_data['接通率'] = round(tel_thailand_call_data['接通次数'] / tel_thailand_call_data['拨打次数'], 4)
tel_thailand_df = tel_thailand_df.sort_values(by=['拨打时间'])
tel_thailand_df = tel_thailand_df.drop_duplicates(subset=['拨打日期','线路号', '债务人关系', '拨打号码'], keep='first')
tel_thailand_df['国家'] = '泰国'
tel_thailand_df['类别'] = '电销'
tel_thailand_df = tel_thailand_df.groupby(by=['国家', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum"})
tel_thailand_df = tel_thailand_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
tel_thailand_df['首拨接通率'] = round(tel_thailand_df['接通号码数'] / tel_thailand_df['拨打号码数'], 4)
tel_thailand_df = pd.merge(tel_thailand_df, tel_thailand_call_data, on=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], how='left')
finish_df = pd.concat([finish_df, tel_thailand_df])

################
# 印尼电销电销区不分自营委外
################
# tel_indonesia_df = config.indonesia_fox_engine_read(tel_sql)
# tel_indonesia_uo_df = config.indonesia_fox_engine_read(tel_uo_sql)
# tel_indonesia_df = pd.merge(tel_indonesia_df, tel_indonesia_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
# tel_indonesia_call_data = tel_indonesia_df.groupby(by=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count","是否接通": "sum", "通话时长": "sum"})
# tel_indonesia_call_data = tel_indonesia_call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
# tel_indonesia_call_data['接通率'] = round(tel_indonesia_call_data['接通次数'] / tel_indonesia_call_data['拨打次数'], 4)
# tel_indonesia_df = tel_indonesia_df.sort_values(by=['拨打时间'])
# tel_indonesia_df = tel_indonesia_df.drop_duplicates(subset=['拨打日期','线路号', '债务人关系', '拨打号码'], keep='first')
# tel_indonesia_df['国家'] = '印尼'
# tel_indonesia_df['类别'] = '电销'
# tel_indonesia_df = tel_indonesia_df.groupby(by=['国家', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum"})
# tel_indonesia_df = tel_indonesia_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
# tel_indonesia_df['首拨接通率'] = round(tel_indonesia_df['接通号码数'] / tel_indonesia_df['拨打号码数'], 4)
# tel_indonesia_df = pd.merge(tel_indonesia_df, tel_indonesia_call_data, on=['组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], how='left')
# finish_df = pd.concat([finish_df, tel_indonesia_df])


################################
# 印尼电销电销区分自营委外
################################
tel_indonesia_sql = '''
        SELECT
                DATE( dial_time) AS '拨打日期',
                HOUR(dial_time) '拨打时段',
                phone_line AS '线路号',
                'self' AS '债务人关系',
                dial_time '拨打时间',
                callee_phone_number '拨打号码',
                IF( talk_duration > 0, 1, 0 )AS '是否接通'     ,
                tche.dunner_id as "催员ID",
                ch.talk_duration    '通话时长' ,
                 ch.dial_duration  '振铃时长' ,
				IF(suy.role_id ='9bd3e2a1768a4c1cbc79af4e99f59447' ,"委外","自营") as "类型"
            FROM
            tel_sale_call_history ch
            LEFT JOIN tel_sale_call_history_extend tche ON ch.id = tche.source_id 
LEFT JOIN ( SELECT * FROM `sys_user_role` WHERE role_id = '9bd3e2a1768a4c1cbc79af4e99f59447' ) suy ON tche.dunner_id = suy.user_id
        WHERE
                ch.dial_time >= '{}'
                and dial_time < '{}'
         '''.format(str(time_start), str(time_end))
tel_indonesia_uo_sql = '''
SELECT
            uo.work_day 拨打日期,
            uo.asset_group_name '组别',
            uo.`user_name` 催员,
            uo.user_id 催员ID
            FROM  `collect_attendance_dtl` uo
            WHERE uo.asset_group_name IS NOT NULL
            and work_day >= '{0}'
'''.format(str(time_start))



tel_indonesia_df = config.indonesia_fox_engine_read(tel_indonesia_sql)
tel_indonesia_uo_df = config.indonesia_fox_engine_read(tel_indonesia_uo_sql)
tel_indonesia_df = pd.merge(tel_indonesia_df, tel_indonesia_uo_df[['拨打日期', '组别', '催员ID']], on=['拨打日期',  '催员ID'], how='left')
tel_indonesia_call_data = tel_indonesia_df.groupby(by=['类型', '组别', '拨打日期', '拨打时段',  '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum", "通话时长": "sum", '振铃时长': "sum"})
tel_indonesia_call_data = tel_indonesia_call_data.rename(columns={"拨打号码": "拨打次数", "是否接通": "接通次数"})
tel_indonesia_call_data['接通率'] = round(tel_indonesia_call_data['接通次数'] / tel_indonesia_call_data['拨打次数'], 4)
tel_indonesia_df = tel_indonesia_df.sort_values(by=['拨打时间'])
tel_indonesia_df = tel_indonesia_df.drop_duplicates(subset=['拨打日期', '线路号', '债务人关系', '拨打号码'], keep='first')
tel_indonesia_df['国家'] = '印尼'
tel_indonesia_df['类别'] = '电销'
tel_indonesia_df = tel_indonesia_df.groupby(by=['国家', '类型', '组别', '拨打日期', '拨打时段', '类别', '线路号', '债务人关系'], as_index=False).agg({'拨打号码': "count", "是否接通": "sum"})
tel_indonesia_df = tel_indonesia_df.rename(columns={"拨打号码": "拨打号码数", "是否接通": "接通号码数"})
tel_indonesia_df['首拨接通率'] = round(tel_indonesia_df['接通号码数'] / tel_indonesia_df['拨打号码数'], 4)
tel_indonesia_df = pd.merge(tel_indonesia_df, tel_indonesia_call_data, on=['组别', '类型', '拨打日期', '拨打时段',  '线路号', '债务人关系'], how='left')
finish_df = pd.concat([finish_df, tel_indonesia_df])



finish_df.to_excel(r'E:\自动化数据存放\IT\IT分小时过程数据.xlsx',index=False)


print (finish_df)