#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
a='因不可抗力因素温和催收,正常撤案,冻结债务人,冻结'
b='撤销分案-23652,稽核异常撤案,风险上报转客维,稽核投诉倾向撤案,转客维'
c='分案不均,拨打受限组内互换案件,分案前已结清,恢复案件撤案,分案不均补案,存在逾期案件时撤d-2案件'
d='5,7'
e='4,6'
f='5,7'


# In[2]:


#encoding=utf-8

import pandas as pd
import mysql.connector
import pymysql
import os
from sshtunnel import SSHTunnelForwarder
from dateutil.relativedelta import relativedelta
from datetime import date,timedelta,datetime
from functools import reduce
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.header import Header
from os.path import basename
from smtplib import SMTP_SSL
from email import encoders

# 自动修改月初日期
today = date.today()
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
if today.day <= 26:
    first_day=(today.replace(day = 26) - relativedelta(months=1)).strftime("%Y-%m-%d")
else: first_day = today.replace(day = 26).strftime("%Y-%m-%d")

# #tg
# server1 = SSHTunnelForwarder(
#     ('8.219.0.11',22),  # 这里写入B 跳板机IP、端口
#     ssh_username='lichongqing',  # 跳板机 用户名
#     ssh_password='lichongqing',  # 跳板机 密码
#     ssh_pkey=r'D:\id_rsa_li',
#     remote_bind_address=('192.168.30.47', 9030),  # 这里写入 C数据库的 IP、端口号
# )
# server1.start()
# conn_ods = pymysql.connect(
#     host='127.0.0.1',       #只能写 127.0.0.1，这是固定的，不可更改
#     port=server1.local_bind_port,
#     user='u_lichongqing',      #C数据库 用户名
#     password='V7rnmeQqa9eh',   #C数据库 密码
#     db='fox_ods',       #填写需要连接的数据库名
#     charset='utf8',
# )


# In[4]:


#巴基斯坦数仓
server1 = SSHTunnelForwarder(
    ('161.117.0.173',22),  # 这里写入B 跳板机IP、端口
    ssh_username='liufengfang',  # 跳板机 用户名
    ssh_password='liufengfang',  # 跳板机 密码
    ssh_pkey=r'E:\4.24稽核扣分\id_rsa_liu',
    remote_bind_address=('192.168.57.82', 9030),  # 这里写入 C数据库的 IP、端口号
)
server1.start()
conn_ods = pymysql.connect(
    host='127.0.0.1',       #只能写 127.0.0.1，这是固定的，不可更改
    port=server1.local_bind_port,
    user='u_lichongqing',      #C数据库 用户名
    password='9ismQzzgFtas',   #C数据库 密码
    db='fox_ods',       #填写需要连接的数据库名
    charset='utf8',
)


# sql,需替换组别，逾期天数
架构表="""
SELECT pt_date,
                SUBSTRING_INDEX(uo.asset_group_name,',',-1) `asset_group_name`,
                SUBSTRING_INDEX(SUBSTRING_INDEX(uo.parent_user_names, ',', 2), ',', -1) `manager_user_name`,
                SUBSTRING_INDEX(SUBSTRING_INDEX(uo.parent_user_names, ',', 3), ',', -1) `leader_user_name`,
                SUBSTRING_INDEX(SUBSTRING_INDEX(uo.parent_user_names, ',', 4), ',', -1) `组员`,
                uo.user_id,
                uo.parent_user_id `leader_user_id`
        FROM
                fox_dw.dwd_fox_user_organization_df uo 
        WHERE
                uo.pt_date BETWEEN "{0}" AND "{1}"
AND SUBSTRING_INDEX(uo.asset_group_name,',',-1) in (
"A New Group",
"PreRemind",
"A Mix Group",
"B1 Group",
"B2 Group"
)
""".format(first_day +" 00:00:00",yesterday+" 23:59:59")
df = pd.read_sql(架构表,conn_ods)


分案回款="""
SELECT
DATE(detail.`分案时间`) `分案日期`,
detail.`业务组`,
detail.`主管`,
detail.`组长`,
detail.`催员ID`,
COUNT(DISTINCT detail.`债务人ID`) `派发户数`,
COUNT(DISTINCT IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1,detail.`债务人ID`,NULL)) `新案派发户数`,
SUM(IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1,detail.`分案本金`,0)) `新案分案本金`,
SUM(IF(detail.`是否计入新案回款本金` = 1,detail.`期次表催回本金`,0)) `新案回款本金`,
SUM(IF(detail.`是否计入新案展期` = 1,detail.`期次表展期费用`,0)) `新案展期费用`,
SUM(IF(detail.`是否计入实收` = 1,detail.`期次表催回总金额`,0)) `总实收`
FROM
(SELECT
ml.mission_group_name `业务组`,
ml.director_name `主管`,
ml.group_leader_name `组长`,
ml.assigned_sys_user_id `催员ID`,
ml.mission_log_asset_id `资产ID`,
asset.asset_item_number `资产编号`,
ml.debtor_id `债务人ID`,
ml.mission_log_id `分案id`,
ml.assign_asset_late_days `分案时资产逾期天数`,
ml.assign_principal_amount * 0.01 `分案本金`,
ml.mission_log_create_at `分案时间`,
unml.mission_log_create_at `撤案时间`,
IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) `撤案理由`,
(CASE cad.attendance_status_flag
WHEN 1 THEN '上线'
WHEN 3 THEN '下线'
WHEN 4 THEN '短假'
WHEN 5 THEN '长假'
WHEN 6 THEN '矿工'
WHEN 7 THEN '稽核下线'
ELSE '其它状态'
END) `分案时催员状态`,
cad.attendance_status_flag `分案时催员状态编号`,
(CASE cadcr.attendance_status_flag
WHEN 1 THEN '上线'
WHEN 3 THEN '下线'
WHEN 4 THEN '短假'
WHEN 5 THEN '长假'
WHEN 6 THEN '矿工'
WHEN 7 THEN '稽核下线'
ELSE '其它状态'
END) `回款时催员状态`,
cadcr.attendance_status_flag `回款时催员状态编号`,
IFNULL(cr.repaid_principal_amount * 0.01,0) `回款表回款本金`,
IFNULL(cr.delay_amount * 0.01,0) `回款表展期费用`,
cr.repay_date `回款表回款时间`,
cr.create_at `回款表创建时间`,
IFNULL(apr.delay_amount * 0.01,0) `期次表展期费用`,
IFNULL(apr.repaid_principal_amount * 0.01,0) `期次表催回本金`,
IFNULL(apr.repaid_fee_amount * 0.01,0) `期次表催回手续费`,
IFNULL(apr.repaid_penalty_amount * 0.01,0) `期次表催回罚息`,
IFNULL(apr.repaid_interest_amount * 0.01,0) `期次表催回利息`,
IFNULL(apr.repaid_total_amount * 0.01,0) `期次表催回总金额`,
cr.batch_num `还款批次号`,
apr.repaid_period `还款期次`,
ml.overdue_period `分案期次`,
cr.overdue_flag `是否逾期`,
ROW_NUMBER() OVER(PARTITION BY ml.mission_log_id) `分案ID序列`,

-- 分案时催员长假、稽核下线不计入
((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{3}"),0)) AND
-- -- 分案时催员短假、旷工并且当天所有撤案不计入
(NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{4}"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
-- 当天分案当天展期不计入
(NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
-- 因为撤案原因不计入
(NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
-- 因为撤案原因并且没有回款不计入
(NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) AND
-- 因为撤案原因 当天撤案 并且 当天没有回款 不计入
(NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{1}") 
AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at) AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0))) `是否计入分案本金`,


-- 分案时催员长假、稽核下线不计入
((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{3}"),0)) AND
-- -- 分案时催员短假、旷工并且当天所有撤案不计入
(NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{4}"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
-- 当天分案当天展期不计入
(NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
-- 因为撤案原因不计入
(NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
-- 因为撤案原因并且没有回款不计入
(NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) AND
-- 因为撤案原因 当天撤案 并且 当天没有回款 不计入
(NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{1}") 
AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at) AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) AND
-- 各业务组新案判断
(IF((ml.mission_group_name LIKE 'Pre%' AND ml.assign_asset_late_days = -2)
OR (ml.mission_group_name LIKE 'A%' AND ml.assign_asset_late_days = 1)
OR (ml.mission_group_name LIKE 'B1%' AND ml.assign_asset_late_days IN (1,8))
OR (ml.mission_group_name LIKE 'B2%' AND ml.assign_asset_late_days IN (1,15)),1,0))) `是否计入新案分案本金`,


-- 分案时催员长假、稽核下线不计入
((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{3}"),0)) AND
-- -- 分案时催员短假、旷工并且当天所有撤案不计入
(NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{4}"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
-- 因为撤案原因不计入
(NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
-- 因为撤案原因并且没有回款不计入
(NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
-- 长假、稽核下线当天回款不计入
AND (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"{5}"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
-- 预提醒只统计当期回款本金，其他业务组统计逾期回款
(IF(LOCATE('PreRemind',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0,1,
IF(cr.overdue_flag = 1 AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0 ,1,0))) ) `是否计入回款本金`,


-- 分案时催员长假、稽核下线不计入
((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{3}"),0)) AND
-- -- 分案时催员短假、旷工并且当天所有撤案不计入
(NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{4}"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
-- 因为撤案原因不计入
(NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
-- 因为撤案原因并且没有回款不计入
(NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
-- 长假、稽核下线当天回款不计入
AND (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"{5}"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
-- 预提醒只统计当期回款本金，其他业务组统计逾期回款
(IF(LOCATE('PreRemind',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0,1,
IF(cr.overdue_flag = 1 AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0 ,1,0))) AND
-- 各业务组新案判断
(IF((ml.mission_group_name LIKE 'Pre%' AND ml.assign_asset_late_days = -2)
OR (ml.mission_group_name LIKE 'A%' AND ml.assign_asset_late_days = 1)
OR (ml.mission_group_name LIKE 'B1%' AND ml.assign_asset_late_days IN (1,8))
OR (ml.mission_group_name LIKE 'B2%' AND ml.assign_asset_late_days IN (1,15)),1,0))) `是否计入新案回款本金`,


-- 分案时催员长假、稽核下线不计入
((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{3}"),0)) AND
-- 当天分案当天展期不计入(展期无回款本金，不用判断)
(NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
-- 长假、稽核下线当天回款不计入
(NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"{5}"),1) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
-- 展期金额不为空
(IF(LOCATE('PreRemind',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.delay_amount,0) <> 0,1,
IF(LOCATE('PreRemind',ml.mission_group_name) = 0 AND IFNULL(apr.delay_amount,0) <> 0,1,0)))) `是否计入展期`,


-- 分案时催员长假、稽核下线不计入
((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{3}"),0)) AND
-- 当天分案当天展期不计入(展期无回款本金，不用判断)
(NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
-- 长假、稽核下线当天回款不计入
 (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"{5}"),1) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
-- 展期金额不为空
 (IF(LOCATE('PreRemind',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.delay_amount,0) <> 0,1,
IF(LOCATE('PreRemind',ml.mission_group_name) = 0 AND IFNULL(apr.delay_amount,0) <> 0,1,0))) AND
 -- 各业务组新案判断
(IF((ml.mission_group_name LIKE 'Pre%' AND ml.assign_asset_late_days = -2)
OR (ml.mission_group_name LIKE 'A%' AND ml.assign_asset_late_days = 1)
OR (ml.mission_group_name LIKE 'B1%' AND ml.assign_asset_late_days IN (1,8))
OR (ml.mission_group_name LIKE 'B2%' AND ml.assign_asset_late_days IN (1,15)),1,0)))  `是否计入新案展期`,


  -- 分案时催员长假、稽核下线不计入
((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{3}"),0)) AND
-- -- 分案时催员短假、旷工并且当天所有撤案不计入
(NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"{4}"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
-- 因为撤案原因不计入
(NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
-- 因为撤案原因并且没有回款不计入
(NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
  -- 长假、稽核下线当天回款不计入
AND NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"{5}"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) `是否计入实收`
FROM
ods_fox_mission_log ml
LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
LEFT JOIN ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id 
AND unml.mission_log_operator = 'unassign' 
AND ( unml.mission_log_create_at >= "{6}" AND unml.mission_log_create_at <= "{7}" ) 
LEFT JOIN ods_fox_collect_attendance_dtl cad ON ml.assigned_sys_user_id = cad.user_id
AND DATE(ml.mission_log_create_at) = cad.work_day
LEFT JOIN ods_fox_collect_recovery cr ON ml.mission_log_id = cr.mission_log_id
AND ( cr.create_at >= "{6}" AND cr.create_at <= "{7}" ) 
AND cr.batch_num IS NOT NULL
LEFT JOIN ods_fox_asset_period_recovery apr ON cr.batch_num = apr.batch_num
LEFT JOIN ods_fox_collect_attendance_dtl cadcr ON cr.sys_user_id = cadcr.user_id
AND DATE(cr.create_at) = cadcr.work_day
LEFT JOIN ods_fox_asset asset ON ml.mission_log_asset_id = asset.asset_id
WHERE
ml.mission_log_operator = 'assign'
AND (ml.mission_log_create_at >= "{6}" AND ml.mission_log_create_at <= "{7}")
AND ml.mission_group_name NOT IN ('PHCC','IVR','WIZ-IVR','cultivate')) detail
GROUP BY
DATE(detail.`分案时间`),
detail.`业务组`,
detail.`主管`,
detail.`组长`,
detail.`催员ID`
ORDER BY
detail.`催员ID`,
DATE(detail.`分案时间`)
""".format(a,b,c,d,e,f,first_day +" 06:00:00",yesterday+" 23:59:59")
dfa= pd.read_sql(分案回款,conn_ods)


离职时间 = """
SELECT
    sue.user_id,
    su.`name`,
    su.del_date '删除时间',
    su.NO,
    sue.dimission_date '最后工作日',-- 业绩截至日期
    sue.dimission_reason -- uo.NAME,
-- SUBSTRING_INDEX(uo.`asset_group_name`,',',-1) '组别',
-- SUBSTRING_INDEX(SUBSTRING_INDEX(uo.`parent_user_names`,',',2),',',-1) '主管',
-- uo.parent_user_name '组长'

FROM
    `ods_fox_sys_user_extend` sue
    JOIN ods_fox_sys_user su ON sue.user_id = su.id -- JOIN user_organization uo ON sue.user_id = uo.user_id

WHERE
    sue.dimission_date IS NOT NULL 
    AND su.del_flag = 1
    AND su.del_date >= '2000-05-10'
ORDER BY
    su.del_date,
    sue.dimission_date
"""
dfb = pd.read_sql(离职时间,conn_ods)


新老天数 ="""
SELECT
uo.user_id,
DATE ( work_day ),
uo.NAME,
CASE

WHEN DATEDIFF( DATE ( work_day ), DATE ( date_new ) ) >= 30 THEN
'NO' ELSE 'YES' 
END AS if_new,
o.attendance_status AS days 
FROM
ods_fox_collect_attendance_dtl AS o
LEFT JOIN (
SELECT
a.user_id,
CASE

WHEN a_date IS NULL THEN
b_date 
WHEN b_date > a_date 
AND a_date IS NOT NULL THEN
a_date 
WHEN b_date IS NULL THEN
a_date 
END AS date_new 
FROM
(
SELECT
user_id,
min(
DATE ( online_day )) AS a_date 
FROM
ods_fox_online_days 
WHERE
online_day_flag = 1 
AND DATE ( online_day ) <= '2023-02-25' 
GROUP BY
user_id 
) AS a
LEFT JOIN (
SELECT
user_id,
min(
DATE ( work_day )) AS b_date 
FROM
ods_fox_collect_attendance_dtl 
WHERE
DATE ( work_day ) >= '2023-02-26' 
AND DATE ( work_day ) <= DATE ( curdate()- 1 ) 
AND attendance_status = 1 
GROUP BY
user_id 
) AS b ON a.user_id = b.user_id UNION
SELECT
bb.* 
FROM
(
SELECT
user_id,
min(
DATE ( work_day )) AS b_date 
FROM
ods_fox_collect_attendance_dtl 
WHERE
DATE ( work_day ) >= '2023-02-26' 
AND DATE ( work_day ) <= DATE ( curdate()- 1 ) 
AND attendance_status = 1 
GROUP BY
user_id 
) AS bb
LEFT JOIN (
SELECT
user_id,
min(
DATE ( online_day )) AS a_date 
FROM
ods_fox_online_days 
WHERE
online_day_flag = 1 
AND DATE ( online_day ) <= '2023-02-25' 
GROUP BY
user_id 
) AS aa ON bb.user_id = aa.user_id 
WHERE
aa.user_id IS NULL 
) AS t1 ON o.user_id = t1.user_id
LEFT JOIN ods_fox_user_organization uo ON o.user_id = uo.user_id -- 修改这里的时间区间就可以了，其他的不用修改

WHERE
DATE ( work_day ) >= "{0}" -- DATE_ADD(CURDATE(),INTERVAL - DAY(CURDATE()) + 1 DAY)   -- 月初第一天

AND DATE(work_day) <= '{1}'
""".format(first_day,yesterday)
dfc = pd.read_sql(新老天数,conn_ods)

最早上线日期 = """
 select a.user_id, 
 case when  a_date is null then b_date  
 when b_date > a_date and a_date is not null  then a_date  
 when b_date is null then a_date 
 end as min_date
 from 
 ( 
 select user_id ,  min(date(online_day)) as a_date
 from ods_fox_online_days 
 where online_day_flag = 1 
 and date(online_day) <= '2023-02-25' -- 不动
  group by user_id
 ) as a
 left join 
 ( 
 select user_id ,  min(date(work_day))   as b_date 
 from ods_fox_collect_attendance_dtl 
 where date(work_day) >= '2023-02-26'  -- 不动
 and date(work_day) <= date( curdate()- 1 )
 and attendance_status = 1 
 group by user_id
 ) as b 
 on a.user_id = b.user_id 

union


select bb.*
from
 ( 
 select user_id ,  min(date(work_day))   as b_date 
 from ods_fox_collect_attendance_dtl 
 where date(work_day) >= '2023-02-26'  -- 不动
 and date(work_day) <= date( curdate()- 1 )
 and attendance_status = 1 
 group by user_id
 ) as bb

left join 

 ( 
 select user_id ,  min(date(online_day)) as a_date
 from ods_fox_online_days 
 where online_day_flag = 1 
 and date(online_day) <= '2023-02-25'  -- 不动
  group by user_id
 ) as aa
 on bb.user_id = aa.user_id
where aa.user_id is null
"""
dfd = pd.read_sql(最早上线日期,conn_ods)

外呼 = """
SELECT
se.dunner_id id,
date(ch.`call_at`) 日期,
COUNT(ch.id) 外呼次数,
SUM(ch.call_time) 通时
FROM ods_audit_call_history ch
LEFT JOIN ods_audit_call_history_extend se
ON ch.id=se.source_id
WHERE ch.`call_at`>='{0}'
AND date(ch.`call_at`)<=date( curdate()- 1 )
AND ch.call_channel=1
-- and ch.call_time=0
GROUP BY 1,2
""".format(first_day+" 00:00:00")
dfe = pd.read_sql(外呼,conn_ods)

# b组时间
b时间 = """
SELECT
        uo.`name` as '催员名',
        a.assigned_sys_user_id as "user_id",
        a.首次晋升B队列日期 
FROM
        (
        SELECT
                assigned_sys_user_id,
                date(min( mission_log_assigned_date )) AS "首次晋升B队列日期" 
        FROM
                ods_fox_mission_log 
        WHERE
                mission_group_name LIKE "B%"
        GROUP BY
                1 
        ORDER BY
                date(
                min( mission_log_assigned_date )) desc
                ) a
        LEFT JOIN ods_fox_user_organization uo ON a.assigned_sys_user_id = uo.user_id
"""
dff = pd.read_sql(b时间,conn_ods)


#合并表格
df1 = df.merge(dfa,left_on = ["pt_date","user_id"],right_on=["分案日期","催员ID"],how="left")
df2 = df1.merge(dfc,left_on = ["pt_date","user_id"],right_on=["date(work_day)","user_id"],how="left")
df3 = df2.merge(dfe,left_on = ["pt_date","user_id"],right_on=["日期","id"],how="left")
df4=df3.groupby(["asset_group_name","manager_user_name","leader_user_name","组员","user_id"],as_index= False).agg(新案分案本金 = ("新案分案本金",'sum'),
                                                                                                  新案回款本金 =("新案回款本金","sum"),
                                                                                                  新案展期费用=("新案展期费用","sum"),
                                                                                                  总实收=("总实收","sum"),
                                                                                                  外呼次数=("外呼次数","sum"),
                                                                                                  通时=("通时","sum"),
                                                                                                  新人天数 = ("days", lambda x: x.sum() if (df3["if_new"] == "YES").any() else 0),
                                                                                                  老人天数 = ("days", lambda x: x.sum() if (df3["if_new"] == "NO").any() else 0),
                                                                                                  总天数 = ("days","sum")
                                            
                                                                                                 )


#添加进入b组时间，如有其它高账龄组，需要修改sql，并在下方列别添加对应组别。
df5 = df4.merge(dfb,on="user_id",how = "left").merge(dfd,on="user_id",how = "left").merge(dff,on="user_id",how = "left")
df5.loc[~df5["asset_group_name"].isin(["B1 Group", "B2 Group"]), "首次晋升B队列日期"] = None
df6 = df5[(df5["删除时间"]>=datetime.strptime(first_day,'%Y-%m-%d').date()) | df5["删除时间"].isnull()]


df6["Repaid principal adjust"]=df6['新案回款本金']+df6['新案展期费用']
df6["Individual Collection Rate"]=(df6['新案回款本金']+df6['新案展期费用'])/df6['新案分案本金']

df6["Newly enrolled Yes/ No"]= (date.today() - timedelta(days=1)-df6["min_date"]).dt.days.apply(lambda x: 'YES' if x < 30 else 'NO')
df6["Gross collectiion ranking"]=df6.groupby(["asset_group_name"])["总实收"].rank(ascending=False,method='first')

def calculate_range1(row):
    max_m = df6[df6['asset_group_name'] == row['asset_group_name']]["Gross collectiion ranking"].max()
    if row["Gross collectiion ranking"] <= round(max_m * 0.05):
        return "Top5%"
    elif row["Gross collectiion ranking"] <= round(max_m * 0.25):
        return "5%-25%"
    elif row["Gross collectiion ranking"] <= round(max_m * 0.5):
        return "25%-50%"
    elif row["Gross collectiion ranking"] <= round(max_m * 0.7):
        return "50%-70%"
    elif row["Gross collectiion ranking"] <= round(max_m * 0.9):
        return "70%-90%"
    else:
        return "bottom10%"
df6["Ranking intervals"] = df6.apply(calculate_range1, axis=1)


df6["Average daily number of calls"]=df6["外呼次数"]/df6["总天数"]
df6["Average daily talk time"]=df6["通时"]/df6["总天数"]/60
df6["To B Group First"]=df6["首次晋升B队列日期"].apply(lambda x: "YES" if pd.notna(x) and x >= pd.to_datetime(first_day) else ("NO" if pd.notna(x) else None))


df6["催回率排名"] = df6.groupby("asset_group_name")["Individual Collection Rate"].rank(ascending=False, method='first')
df6["日均实收"] = df6["总实收"]/df6["总天数"]
df6.loc[(df6["总实收"] == 0)|(df6["总天数"] == 0),"日均实收"] = None
df6["日均实收排名"]=df6.groupby("asset_group_name")["日均实收"].rank(ascending=False,method='first')
df6["Integrated Ranking"] = (df6["日均实收排名"]*0.5+df6["催回率排名"]*0.5)
df6["综合排名序列"] = df6.groupby("asset_group_name")["Integrated Ranking"].rank(ascending=True, method='first')
def calculate_range11(row):
    max_m = df6[df6['asset_group_name'] == row['asset_group_name']]["综合排名序列"].max()
    if row["综合排名序列"] <= round(max_m * 0.2):
        return "Top20%"
    elif row["综合排名序列"] <= round(max_m * 0.3):
        return "20%-30%"    
    elif row["综合排名序列"] <= round(max_m * 0.5):
        return "30%-50%"
    elif row["综合排名序列"] <= round(max_m * 0.7):
        return "50%-70%"
    elif row["综合排名序列"] <= round(max_m * 0.9):
        return "70%-90%"
    else:
        return "bottom10%"
df6["Integrated Ranking interval"] = df6.apply(calculate_range11, axis=1)


#上线大于7天重新排名
df6_filtered = df6[df6['总天数'] > 7]
try:
    df6_filtered['Gross collectiion ranking'] = df6_filtered.groupby(["asset_group_name"])["总实收"].rank(ascending=False,method='first')
    def calculate_range2(row):
        max_m = df6_filtered[df6_filtered['asset_group_name'] == row['asset_group_name']]["Gross collectiion ranking"].max()
        if row["Gross collectiion ranking"] <= round(max_m * 0.05):
            return "Top5%"
        elif row["Gross collectiion ranking"] <= round(max_m * 0.25):
            return "5%-25%"
        elif row["Gross collectiion ranking"] <= round(max_m * 0.5):
            return "25%-50%"
        elif row["Gross collectiion ranking"] <= round(max_m * 0.7):
            return "50%-70%"
        elif row["Gross collectiion ranking"] <= round(max_m * 0.9):
            return "70%-90%"
        else:
            return "bottom10%"

    df6_filtered["Ranking intervals"] = df6_filtered.apply(calculate_range2, axis=1)
    df6_filtered["催回率排名"] = df6_filtered.groupby("asset_group_name")["Individual Collection Rate"].rank(ascending=False, method='first')
    df6_filtered["日均实收排名"]=df6_filtered.groupby("asset_group_name")["日均实收"].rank(ascending=False,method='first')
    df6_filtered["Integrated Ranking"] = (df6_filtered["日均实收排名"]*0.5+df6_filtered["催回率排名"]*0.5)
    df6_filtered["综合排名序列"] = df6_filtered.groupby("asset_group_name")["Integrated Ranking"].rank(ascending=True, method='first')
    def calculate_range12(row):
        max_m = df6_filtered[df6_filtered['asset_group_name'] == row['asset_group_name']]["综合排名序列"].max()
        if row["综合排名序列"] <= round(max_m * 0.2):
            return "Top20%"
        elif row["综合排名序列"] <= round(max_m * 0.3):
            return "20%-30%"
        elif row["综合排名序列"] <= round(max_m * 0.5):
            return "30%-50%"
        elif row["综合排名序列"] <= round(max_m * 0.7):
            return "50%-70%"
        elif row["综合排名序列"] <= round(max_m * 0.9):
            return "70%-90%"
        else:
            return "bottom10%"
    df6_filtered["Integrated Ranking interval"] = df6.apply(calculate_range12, axis=1)
    df6.loc[df6_filtered.index, ['Gross collectiion ranking','Ranking intervals','Integrated Ranking','Integrated Ranking interval']] = df6_filtered[['Gross collectiion ranking','Ranking intervals','Integrated Ranking','Integrated Ranking interval']]

except Exception as e:
    pass


df7 = df6[["asset_group_name","manager_user_name","leader_user_name","组员","Newly enrolled Yes/ No",
"min_date","最后工作日","删除时间","总实收","Gross collectiion ranking","Ranking intervals","新案分案本金","Repaid principal adjust",
    "Individual Collection Rate","Average daily number of calls","Average daily talk time","总天数",
     "新人天数","老人天数","To B Group First","首次晋升B队列日期",'Integrated Ranking','Integrated Ranking interval']]


df8=df7.loc[df7["总天数"]>7,:]
g1 = df7.groupby(["asset_group_name","manager_user_name","leader_user_name"],as_index=False)["新案分案本金","Repaid principal adjust","总实收","总天数"].sum()
g2 = df8.groupby(["asset_group_name","manager_user_name","leader_user_name"],as_index=False)["新案分案本金","Repaid principal adjust","总实收","总天数"].sum()
g1["Team Collection Rate1"] = g1["Repaid principal adjust"]/g1["新案分案本金"]
g1["Rank1"] = g1.groupby("asset_group_name")["Team Collection Rate1"].rank(ascending=True, method='first')
g2["Team Collection Rate2(Online days >7)"] = g2["Repaid principal adjust"]/g2["新案分案本金"]
g2["Rank2"] = g2.groupby("asset_group_name")["Team Collection Rate2(Online days >7)"].rank(ascending=True, method='first')
g2["Ave.Gross collection"] = g2["总实收"]/g2["总天数"]
g2["Rank3"] = g2.groupby("asset_group_name")["Ave.Gross collection"].rank(ascending=True, method='first')


df9 = g1[['asset_group_name', 'manager_user_name', 'leader_user_name','Team Collection Rate1',
       'Rank1']].merge(g2[['asset_group_name', 'manager_user_name', 'leader_user_name','Team Collection Rate2(Online days >7)', 'Rank2',
       'Ave.Gross collection', 'Rank3']],on=['asset_group_name', 'manager_user_name', 'leader_user_name'],how="left")


m1 = df7.groupby(["asset_group_name","manager_user_name"],as_index=False)["新案分案本金","Repaid principal adjust","总天数"].sum()
m2 = df8.groupby(["asset_group_name","manager_user_name"],as_index=False)["新案分案本金","Repaid principal adjust","总天数"].sum()
m1["Team Collection Rate1"] = m1["Repaid principal adjust"]/m1["新案分案本金"]
m2["Team Collection Rate2"] = m2["Repaid principal adjust"]/m2["新案分案本金"]


df10 = m1[['asset_group_name', 'manager_user_name','Team Collection Rate1']].merge(m2,on=['asset_group_name', 'manager_user_name'],how = "left")

df7.columns = ['Group','Supervisor','Team Leader','Collection Executive',
               'Newly enrolled Yes/ No','Min Online Date','dimission_date','deletion_tate','Gross collection',
              'Gross collectiion ranking','Ranking intervals','Divided principal adjust','Repaid principal adjust',
               'Individual Collection Rate','Average daily number of calls','Average daily talk time',
               'Online days','Number of days between rookies',
               'Number of days in a period of old age','To B Group First','To B Group date',
               'Integrated Ranking','Integrated Ranking interval']


df9.columns=['Group','Supervisor','Team Leader', 'Team Collection Rate1', 'Rank1',
       'Team Collection Rate2(Online days >7)', 'Rank2',
       'Ave.Gross collection', 'Rank3']

df10.columns=['Group','Supervisor','Team Collection Rate1','Divided principal adjust >7',
              'Repaid principal adjust >7','Online days >7','Team Collection Rate2 >7']



writer = pd.ExcelWriter('./{0}{1}自动日报.xlsx'.format(yesterday,"巴基斯坦"))
df7.to_excel(writer, sheet_name='ce', index=False)
df9.to_excel(writer, sheet_name='tl', index=False)
df10.to_excel(writer, sheet_name='sp', index=False)
writer.close()





df_email = pd.read_csv('E:\邮箱.txt', sep='=',header=None,encoding='gbk')
df_email.columns = ['key', 'value']
email_variable = df_email.loc[df_email['key'] == '邮箱', 'value'].values[0]
email_password = df_email.loc[df_email['key'] == '密码', 'value'].values[0]


def send_email(to_email,email_variable,email_password):
# 邮件主题
    mail_title = '{0}{1}自动日报.xlsx'.format(yesterday,"巴基斯坦")
    sender_email = email_variable
    sender_password = email_password
    recipient_email = to_email
    attachment_path1 = './{0}{1}自动日报.xlsx'.format(yesterday,"巴基斯坦")
    # 邮件正文
    mail_content = """
    <!DOCTYPE html>
    <html>
    <head>
      <style>
        .indented {
          margin-left: 20px; /* 设置缩进的距离，可以根据需要调整 */
        }
      </style>
    </head>
    <body>
      <p>你好，附件是日报数据,请查收附件。
      </p>
    </body>
    </html>
    """

    # 创建邮件对象
    msg = MIMEMultipart()
    msg["Subject"] = Header(mail_title, 'utf-8')
    msg["From"] = sender_email
    msg["To"] = recipient_email

    # 添加邮件正文
    msg.attach(MIMEText(mail_content, 'html'))

    # 读取附件内容
    with open(attachment_path1, 'rb') as file1:
        attachment_data1 = file1.read()
    attachment1 = MIMEApplication(attachment_data1)
    attachment1["Content-Type"] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    # attachment1["Content-Disposition"] = f'attachment;filename=("gbk", "", "稽核数据菲律宾.xlsx")}"'
    attachment1.add_header("Content-Disposition", "attachment", filename=("gbk", "", '{0}{1}自动日报.xlsx'.format(yesterday,"巴基斯坦")))
    # 添加附件到邮件
    msg.attach(attachment1)
    # 发送邮件
    # smtp_server = "smtp.exmail.qq.com"
    smtp_server = "smtp.exmail.qq.com"
    smtp_port = 465

    with SMTP_SSL(smtp_server, smtp_port) as server:
        server.login(sender_email, sender_password)
        server.send_message(msg)
mail_list = ['lichongqing@weidu.ac.cn',"wangxiang3@weidu.ac.cn",
             "zhangjinyou@weidu.ac.cn","luoping@weidu.ac.cn","chenxiaomin@weidu.ac.cn"]
# mail_list = ["lichongqing@weidu.ac.cn"]
for i in mail_list:
    send_email(i,email_variable,email_password)



