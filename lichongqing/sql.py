def 架构表(first_day, yesterday, group):
    架构表 = """
    SELECT pt_date,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 2 ), ',', - 1 ) `manager_user_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 3 ), ',', - 1 ) `leader_user_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 4 ), ',', - 1 ) `组员`,
            uo.user_id,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE uo.user_id = su.id) `user_no`,
            (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.user_id = su.id) `组员`,
            -- parent_user_name,
            uo.parent_user_id `leader_user_id`,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE uo.parent_user_id = su.id) `leader_user_no`,
            (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.parent_user_id = su.id) `leader_user_name`,
            manager_user_id,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE uo.manager_user_id = su.id) `manager_user_no`,
            manager_user_name
            -- (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.manager_user_id = su.id) `manager_user_name`
            FROM
                  fox_dw.dwd_fox_user_organization_df uo
                --   fox_tmp.dwd_fox_user_organization_df uo

            WHERE
                    uo.pt_date>="{0}"
                    and uo.pt_date<="{1}"
                    and LENGTH(parent_user_names) - LENGTH(REPLACE(parent_user_names, ',', '')) = 3
				AND SUBSTRING_INDEX(uo.asset_group_name,',',-1) in (SELECT distinct(asset_group_name) FROM `ods_fox_collect_attendance_dtl`
						where work_day >="{0}" and work_day <="{1}" and asset_group_name not like "Telesales%")
    """.format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return 架构表
# 中国架构
def 中国架构表(first_day, yesterday, group):
    架构表 = """
    SELECT pt_date,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 2 ), ',', - 1 ) `manager_user_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 3 ), ',', - 1 ) `leader_user_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 4 ), ',', - 1 ) `组员`,
            uo.user_id,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE uo.user_id = su.id) `user_no`,
            (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.user_id = su.id) `组员`,
            -- parent_user_name,
            uo.parent_user_id `leader_user_id`,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE uo.parent_user_id = su.id) `leader_user_no`,
            (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.parent_user_id = su.id) `leader_user_name`,
            manager_user_id,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE uo.manager_user_id = su.id) `manager_user_no`,
            manager_user_name
            -- (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.manager_user_id = su.id) `manager_user_name`
            FROM
                  -- fox_dw.dwd_fox_user_organization_df uo
                  fox_tmp.dwd_fox_user_organization_df uo

            WHERE
                    uo.pt_date>="{0}"
                    and uo.pt_date<="{1}"
                    and LENGTH(parent_user_names) - LENGTH(REPLACE(parent_user_names, ',', '')) = 3
				AND SUBSTRING_INDEX(uo.asset_group_name,',',-1) in (SELECT distinct(asset_group_name) FROM `ods_fox_collect_attendance_dtl`
						where work_day >="{0}" and work_day <="{1}" and asset_group_name not like "Telesales%")
    """.format(first_day + " 00:00:00", yesterday + " 23:59:59", group)
    return 架构表

def 分案回款(a,b,c,first_day,yesterday,新案):
    分案回款="""
    SELECT
    DATE(detail.`分案时间`) `分案日期`,
    -- detail.`业务组`,
    -- detail.`主管`,
    -- detail.`组长`,
    detail.`催员ID`,
    COUNT(DISTINCT detail.`债务人ID`) `债务人数`,
	COUNT(DISTINCT IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1,detail.`债务人ID`,NULL)) `新案债务人数`,
	COUNT(DISTINCT IF(detail.`期次表催回总金额`>0,detail.`债务人ID`,NULL)) `催回债务人数`,
	COUNT(DISTINCT IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1 and detail.`期次表催回总金额`>0,detail.`债务人ID`,NULL)) `新案催回债务人数`,
	
	COUNT(DISTINCT detail.`资产ID`) `资产数`,
	COUNT(DISTINCT if(day(分案时间)=1,null,detail.`资产ID`)) `去第一天资产数`, -- 第一天分案清空
	SUM(IF(detail.`分案ID序列` = 1 AND detail.`是否计入分案本金` = 1 and day(分案时间)!=1,detail.`分案本金`,0)) `去第一天分案本金`,
	COUNT(DISTINCT IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1,detail.`资产ID`,NULL)) `新案资产数`,
	COUNT(DISTINCT IF(detail.`期次表催回总金额`>0,detail.`资产ID`,NULL)) `催回资产数`,
	COUNT(DISTINCT IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1 and detail.`期次表催回总金额`>0,detail.`资产ID`,NULL)) `新案催回资产数`,
	
    SUM(IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1,detail.`分案本金`,0)) `新案分案本金`,
    SUM(IF(detail.`是否计入新案回款本金` = 1,detail.`期次表催回本金`,0)) `新案回款本金`,
    SUM(IF(detail.`是否计入新案展期` = 1,detail.`期次表展期费用`,0)) `新案展期费用`,
    SUM(IF(detail.`是否计入实收` = 1,detail.`期次表催回总金额`,0)) `总实收`,
    SUM(IF(detail.`分案ID序列` = 1 AND detail.`是否计入分案本金` = 1,detail.`分案本金`,0)) `分案本金`,
    SUM(IF(detail.`是否计入回款本金` = 1,detail.`期次表催回本金`,0)) `回款本金`,
    0 as `展期费用`
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
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
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
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
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
    (IF({5},1,0))) `是否计入新案分案本金`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
    -- 因为撤案原因不计入
    (NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
    -- 因为撤案原因并且没有回款不计入
    (NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
    -- 长假、稽核下线当天回款不计入
    AND (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
    -- 预提醒只统计当期回款本金，其他业务组统计逾期回款
    (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0,1,
    IF(cr.overdue_flag = 1 AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0 ,1,0))) ) `是否计入回款本金`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
    -- 因为撤案原因不计入
    (NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
    -- 因为撤案原因并且没有回款不计入
    (NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
    -- 长假、稽核下线当天回款不计入
    AND (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
    -- 预提醒只统计当期回款本金，其他业务组统计逾期回款
    (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0,1,
    IF(cr.overdue_flag = 1 AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0 ,1,0))) AND
    -- 各业务组新案判断
    (IF({5},1,0))) `是否计入新案回款本金`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- 当天分案当天展期不计入(展期无回款本金，不用判断)
    (NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
    -- 长假、稽核下线当天回款不计入
    (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),1) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
    -- 展期金额不为空
    (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.delay_amount,0) <> 0,1,
    IF(LOCATE('P',ml.mission_group_name) = 0 AND IFNULL(apr.delay_amount,0) <> 0,1,0)))) `是否计入展期`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- 当天分案当天展期不计入(展期无回款本金，不用判断)
    (NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
    -- 长假、稽核下线当天回款不计入
     (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),1) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
    -- 展期金额不为空
     (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.delay_amount,0) <> 0,1,
    IF(LOCATE('P',ml.mission_group_name) = 0 AND IFNULL(apr.delay_amount,0) <> 0,1,0))) AND
     -- 各业务组新案判断
    (IF({5},1,0)))  `是否计入新案展期`,
    
    
      -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
    -- 因为撤案原因不计入
    (NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
    -- 因为撤案原因并且没有回款不计入
    (NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
      -- 长假、稽核下线当天回款不计入
    AND NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) `是否计入实收`
    FROM
    ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
    LEFT JOIN ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id 
    AND unml.mission_log_operator = 'unassign' 
    AND ( unml.mission_log_create_at >= "{3}" AND unml.mission_log_create_at <= "{4}" ) 
    LEFT JOIN ods_fox_collect_attendance_dtl cad ON ml.assigned_sys_user_id = cad.user_id
    AND DATE(ml.mission_log_create_at) = cad.work_day
    LEFT JOIN ods_fox_collect_recovery cr ON ml.mission_log_id = cr.mission_log_id
    AND ( cr.create_at >= "{3}" AND cr.create_at <= "{4}" ) 
    AND cr.batch_num IS NOT NULL
    LEFT JOIN ods_fox_asset_period_recovery apr ON cr.batch_num = apr.batch_num
    LEFT JOIN ods_fox_collect_attendance_dtl cadcr ON cr.sys_user_id = cadcr.user_id
    AND DATE(cr.create_at) = cadcr.work_day
    LEFT JOIN ods_fox_asset asset ON ml.mission_log_asset_id = asset.asset_id
    WHERE
    ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{3}" AND ml.mission_log_create_at <= "{4}")
    AND ml.mission_group_name NOT IN ('PHCC','IVR','WIZ-IVR','cultivate')) detail
    GROUP BY
    DATE(detail.`分案时间`),
    -- detail.`业务组`,
    -- detail.`主管`,
    -- detail.`组长`,
    detail.`催员ID`
    ORDER BY
    detail.`催员ID`,
    DATE(detail.`分案时间`)
    """.format(a,b,c,first_day,yesterday,新案)
    return 分案回款

def 离职时间(yesterday):
    离职时间 = """
        SELECT
            sue.user_id,
            su.`name`,
            su.del_date '删除时间',
            su.NO,
            IF( sue.dimission_date IS NULL,su.del_date, sue.dimission_date ) '最后工作日',-- 业绩截至日期
            sue.dimission_reason, -- uo.NAME
            case when count_churn = 1 then "YES" when count_churn = 0 then "NO"  end as "Include loss rate YES/NO"
        FROM
            `ods_fox_sys_user_extend` sue
            JOIN ods_fox_sys_user su ON sue.user_id = su.id -- JOIN user_organization uo ON sue.user_id = uo.user_id
        WHERE
            -- sue.dimission_date IS NOT NULL 
            su.del_flag = 1
            and su.del_date >= '2000-05-10'
        having date(最后工作日) <= '{}'
        ORDER BY
            su.del_date,
            sue.dimission_date
    """.format(yesterday)
    return 离职时间

#####20241105新增 预估日均入催量###############################
def 预估日均入催量(first_day,yesterday):
    预估日均入催量="""
    select group_type,
    sum(group_value) as group_value,
    round(sum(group_value)/count(distinct calendar_date),0) as mean_group_value
    from fox_tmp.temp_zmj_20141030
    where calendar_date>"{0}"
    and calendar_date<="{1}"
    group by group_type
""".format(first_day,yesterday)
    return 预估日均入催量

#####20241105新增 承载监控日人均预估（预估日人均新案分案量 预估日人均在手案件量） ##############################
def 承载监控日人均预估(yesterday):
    承载监控日人均预估="""
   select
   "{0}" as  dte,business_group,
   round(estimated_daily_reminder_debt/plan_human_resources,0) as renjun_estimated_daily_reminder_debt,
   plan_preman_online_asset as renjun_plan_preman_online_asset
   from fox_tmp.temp_load_bearing_monitor_for
""".format(yesterday)
    return 承载监控日人均预估


####20241111 非国内  印尼 巴基斯坦 泰国 墨西哥
def 案件量监控1(first_day,yesterday):
    案件量监控1 = """
    select
a0.`业务组`,a0.`月累计日均资产数`,a1.mean_group_value as `预估日均入催量`
from 
(
select 
`业务组2` as `业务组`,
`账龄`,
`进入账龄第一天`,
count(distinct `资产ID`) as `月累计资产数`,
count(distinct `分案日期`) as `月累计天数`,
round(count(distinct `资产ID`)/count(distinct `分案日期`),0) as `月累计日均资产数`

from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
            ml.mission_log_id `分案id`,
            ml.mission_group_name `业务组`,
			zb.`业务组` as `业务组2`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            ml.assigned_sys_user_id `催员ID`,
            date(ml.mission_log_create_at) `分案日期`,
            date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,
            ml.debtor_id `债务人ID`,
			ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
            IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
            展期费,
            总实收,
            mlur.mission_log_unassign_reason,
			if(mlur.mission_log_unassign_reason in( "分案不均","UNEVEN_WITHDRAW_CASE"),1,0) 是否分案不均
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	AND unml.mission_log_operator = 'unassign' 
    AND (unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" ) 
    LEFT JOIN 
    (SELECT
            mission_log_id,
        MAX(repay_date) 还款日期,
        SUM( delay_amount)/100 展期费,
        SUM( repaid_principal_amount )/100 催回本金,
            sum(repaid_total_amount)/100 总实收
    FROM
        fox_ods.ods_fox_collect_recovery 
    WHERE
        repay_date >= "{0}"
        AND repay_date <= "{1}"
        and repaid_total_amount >0
        AND batch_num IS NOT NULL
    GROUP BY
        mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
		left join 
		(select `年`,`月`,`队列`,`业务组`,`账龄`,
		case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
		from fox_tmp.`组别信息`
		where `年`=year("{0}") and `月`=month("{0}")
		) zb
		on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列` and 
		zb.`进入账龄第一天`=ml.assign_asset_late_days and zb.`进入账龄第一天`=ml.assign_debtor_late_days)

		
    WHERE ml.mission_log_operator = 'assign'
    -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")

)a
where `是否分案不均`=0
and `账龄` is not null
group by `账龄`,
`业务组2`,
`进入账龄第一天`
)a0
left join
(
select group_type,
sum(group_value) as group_value,
round(sum(group_value)/count(distinct calendar_date),0) as mean_group_value
from fox_tmp.temp_zmj_20141030
where calendar_date>="{0}"
and calendar_date<="{1}"
group by group_type
order by group_type
)a1
on (a0.`业务组`=a1.group_type)
order by a0.`账龄`
    """.format(first_day,yesterday+" 23:59:59")
    return 案件量监控1


####20241111 国内
def 案件量监控2(first_day, yesterday):
    案件量监控2 = """
    select
a0.`业务组`,a0.`月累计日均债务人数`,a1.mean_group_value as `预估日均入催量`
from
(
select 
`业务组2` as `业务组`,
`账龄`,
`进入账龄第一天`,
-- count(distinct `资产ID`) as `月累计资产数`,
count(distinct `债务人ID`) as `月累计债务人数`,
count(distinct `分案日期`) as `月累计天数`,
round(count(distinct `债务人ID`)/count(distinct `分案日期`),0) as `月累计日均债务人数`

-- round(count(distinct `资产ID`)/count(distinct `分案日期`),0) as `月累计日均资产数`

from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
            ml.mission_log_id `分案id`,
            ml.mission_group_name `业务组`,
			zb.`业务组` as `业务组2`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            ml.assigned_sys_user_id `催员ID`,
            date(ml.mission_log_create_at) `分案日期`,
            date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,
            ml.debtor_id `债务人ID`,
			ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
            IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in ("分案不均","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
    
   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
    AND unml.mission_log_operator = 'unassign' 
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" ) 
    LEFT JOIN 
    (SELECT
            mission_log_id,
        MAX(repay_date) 还款日期,
        SUM( delay_amount)/100 展期费,
        SUM( repaid_principal_amount )/100 催回本金,
            sum(repaid_total_amount)/100 总实收
    FROM
        fox_ods.ods_fox_collect_recovery 
    WHERE
        repay_date >= "{0}"
        AND repay_date <= "{1}"
        and repaid_total_amount >0
        AND batch_num IS NOT NULL
    GROUP BY
        mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
		left join 
		(select `年`,`月`,`队列`,`业务组`,`账龄`,
		case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
		from fox_tmp.`组别信息`
		where `年`=year("{0}") and `月`=month("{0}")
		) zb
		on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列` and 
		zb.`进入账龄第一天`=ml.assign_asset_late_days 
		 and zb.`进入账龄第一天`=ml.assign_debtor_late_days
		)

		
    WHERE ml.mission_log_operator = 'assign'
   -- and not ((mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION")) and (date(ml.mission_log_create_at) = date(unml.mission_log_create_at)))
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
)a
where `是否分案不均`=0
 -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
and `账龄` is not null
group by `账龄`,
`业务组2`,
`进入账龄第一天`
)a0
left join
(
select group_type,
sum(group_value) as group_value,
round(sum(group_value)/count(distinct calendar_date),0) as mean_group_value
from fox_tmp.temp_zmj_20141030
where calendar_date>="{0}"
and calendar_date<="{1}"
group by group_type
order by group_type
)a1
on (a0.`业务组`=a1.group_type)
order by a0.`账龄`
    """.format(first_day, yesterday+" 23:59:59")
    return 案件量监控2


####20241112 国wai
def 分案监控1(first_day, yesterday):
    分案监控1 = """

    with aa as(

    select
     substr('{1}',1,10) as `统计日期`,
    `业务组2` as `业务组`,
    mission_group_name as asset_group_name,
    d.`正式主管中文名` as `管理层`,
    `催员ID`,
    `账龄`,
    `进入账龄第一天`,
    count(distinct `资产ID`) as `月累计资产数`,
    count(distinct case when work_day is not null then `资产ID` else null end) as `月累计资产数_上线`,
    count(distinct case when work_day is not null then concat(`催员ID`,work_day) else null end) as `上线天人`,
    round(count(distinct `资产ID`)/count(distinct concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,

    round(count(distinct case when work_day is not null then `资产ID` else null end)/count(distinct case when work_day is not null then concat(`催员ID`,work_day) else null end),0) as `日人均新案分案量_上线`,


    sum(`分案本金`) as `分案本金`,
    sum(case when work_day is not null then `分案本金` else 0 end) as `分案本金_上线`,

    round(sum(`分案本金`)/count(distinct concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`,

    round(sum(case when work_day is not null then `分案本金` else 0 end)/count(distinct case when work_day is not null then concat(`催员ID`,work_day) else null end),0) as `日人均新案分案金额_上线`

    from
    (
    select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`, sum(分案本金) 分案本金
    from
    (
    SELECT      zb.`账龄`,
                zb.`进入账龄第一天`,
               -- ml.mission_log_id `分案id`,
                ml.mission_group_name,
                zb.`业务组` as `业务组2`,
                -- SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
                ml.director_name `主管`,
                ml.group_leader_name `组长`,
                ml.assigned_sys_user_id `催员ID`,
                date(ml.mission_log_create_at) `分案日期`,
                date(unml.mission_log_create_at) `撤案日期`,
                ml.mission_log_asset_id `资产ID`,
                ml.assign_asset_late_days as `逾期天数`,
                ml.assign_principal_amount * 0.01 `分案本金`,
                IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
                展期费,
                总实收,
                if(mlur.mission_log_unassign_reason in ("分案不均","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均
        from ods_fox_mission_log ml
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
        LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
        AND unml.mission_log_operator = 'unassign' 
        AND ( unml.mission_log_create_at >= '{0}' AND unml.mission_log_create_at <= '{1}' ) 
        LEFT JOIN 
        (SELECT
                mission_log_id,
            MAX(repay_date) 还款日期,
        --                 cr.assigned_date,
        --     SUM(repaid_principal_amount),
            SUM( delay_amount)/100 展期费,
            SUM( repaid_principal_amount )/100 催回本金,
                sum(repaid_total_amount)/100 总实收
        FROM
            fox_ods.ods_fox_collect_recovery 
        WHERE
            repay_date >= '{0}'
            AND repay_date <= '{1}'
            and repaid_total_amount >0
            AND batch_num IS NOT NULL
        GROUP BY
            mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
                    left join 
                    (select `年`,`月`,`队列`,`业务组`,`账龄`,
                    case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
                    from fox_tmp.`组别信息`
                    where `年`=year('{0}') and `月`=month('{0}')
                    ) zb
                    on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列` and 
                    zb.`进入账龄第一天`=ml.assign_asset_late_days and zb.`进入账龄第一天`=ml.assign_debtor_late_days)

        WHERE ml.mission_log_operator = 'assign'
       -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
        AND (ml.mission_log_create_at >= '{0}' AND ml.mission_log_create_at <= '{1}')
                    )aa
    WHERE `是否分案不均` = 0
    group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`
    )a
    left join 
    (select user_id,work_day 
      from 
    fox_ods.ods_fox_collect_attendance_dtl
    where attendance_status=1
    group by user_id,work_day
    )cad
       on (a.`催员ID`=cad.user_id and cad.work_day = substr(a.`分案日期`,1,10))
    left join
    fox_tmp.`主管对应关系` d
    on (year(a.`分案日期`)=d.`年` and month(a.`分案日期`)=d.`月` and `主管`=`储备主管`)
    where 1 = 1
    and `账龄` is not null

    group by `业务组2`,
    mission_group_name,
    `账龄`,
    `进入账龄第一天`,d.`正式主管中文名`,`催员ID`, substr('{1}',1,10)
    order by `业务组2`
    )

    select 
    `统计日期`,
    `业务组`,
     asset_group_name,
     `管理层`,
    round(min(日人均新案分案量)-avg(日人均新案分案量),2) as 最小日均分案数_平均,
    round(max(日人均新案分案量)-avg(日人均新案分案量),2) as 最大日均分案数_平均,
    -- round(avg(日人均新案分案量),2) as 日均新案分案量_mean,

    round(min(日人均新案分案金额)-avg(日人均新案分案金额),2) as 最小日均分案金额_平均,
    round(max(日人均新案分案金额)-avg(日人均新案分案金额),2) as 最大日均分案金额_平均,
    -- round(avg(日人均新案分案金额),2) as 日人均新案分案金额_mean,

    round(min(日人均新案分案量_上线)-avg(日人均新案分案量_上线),2) as 每日上线最小日均分案量_平均,
    round(max(日人均新案分案量_上线)-avg(日人均新案分案量_上线),2) as 每日上线最大日均分案量_平均,
    -- round(avg(日人均新案分案量_上线),2) as 日均新案分案量_mean_上线,

    round(min(日人均新案分案金额_上线)-avg(日人均新案分案金额_上线),2) as 每日上线最小日均分案金额_平均,
    round(max(日人均新案分案金额_上线)-avg(日人均新案分案金额_上线),2) as 每日上线最大日均分案金额_平均
    -- round(avg(日人均新案分案金额_上线),2) as 日人均新案分案金额_mean_上线


    from aa
    where `管理层` is not null
    group by `业务组`,
     asset_group_name,
     `管理层`,`统计日期`
    """.format(first_day, yesterday + " 23:59:59")
    return 分案监控1


####20241112 国内
def 分案监控2(first_day, yesterday):
    分案监控2 = """
        
    with aa as(
    
    select
     substr('{1}',1,10) as `统计日期`,
    `业务组2` as `业务组`,
    mission_group_name as asset_group_name,
    d.`正式主管中文名` as `管理层`,
    `催员ID`,
    `账龄`,
    `进入账龄第一天`,
    count(distinct `债务人ID`) as `月累计资产数`,
    count(distinct case when work_day is not null then `债务人ID` else null end) as `月累计资产数_上线`,
    count(distinct case when work_day is not null then concat(`催员ID`,work_day) else null end) as `上线天人`,
    round(count(distinct `债务人ID`)/count(distinct concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
    
    round(count(distinct case when work_day is not null then `债务人ID` else null end)/count(distinct case when work_day is not null then concat(`催员ID`,work_day) else null end),0) as `日人均新案分案量_上线`,
    
    
    sum(`分案本金`) as `分案本金`,
    sum(case when work_day is not null then `分案本金` else 0 end) as `分案本金_上线`,
    
    round(sum(`分案本金`)/count(distinct concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`,
    
    round(sum(case when work_day is not null then `分案本金` else 0 end)/count(distinct case when work_day is not null then concat(`催员ID`,work_day) else null end),0) as `日人均新案分案金额_上线`
    
    from
    (
    select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`主管`,债务人ID, sum(分案本金) 分案本金
    from
    (
    SELECT      zb.`账龄`,
                zb.`进入账龄第一天`,
               -- ml.mission_log_id `分案id`,
                ml.mission_group_name,
                zb.`业务组` as `业务组2`,
                -- SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
                ml.director_name `主管`,
                ml.group_leader_name `组长`,
                ml.assigned_sys_user_id `催员ID`,
                date(ml.mission_log_create_at) `分案日期`,
                date(unml.mission_log_create_at) `撤案日期`,
                ml.mission_log_asset_id `资产ID`,
                ml.debtor_id `债务人ID`,
                ml.assign_asset_late_days as `逾期天数`,
                ml.assign_principal_amount * 0.01 `分案本金`,
                IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
                展期费,
                总实收,
                if(mlur.mission_log_unassign_reason in ("分案不均","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均
        from ods_fox_mission_log ml
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
       LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id AND unml.mission_log_operator = 'unassign' 
        AND ( unml.mission_log_create_at >= '{0}' AND unml.mission_log_create_at <= '{1}' ) 
        LEFT JOIN 
        (SELECT
                mission_log_id,
            MAX(repay_date) 还款日期,
        --                 cr.assigned_date,
        --     SUM(repaid_principal_amount),
            SUM( delay_amount)/100 展期费,
            SUM( repaid_principal_amount )/100 催回本金,
                sum(repaid_total_amount)/100 总实收
        FROM
            fox_ods.ods_fox_collect_recovery 
        WHERE
            repay_date >= '{0}'
            AND repay_date <= '{1}'
            and repaid_total_amount >0
            AND batch_num IS NOT NULL
        GROUP BY
            mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
                    left join 
                    (select `年`,`月`,`队列`,`业务组`,`账龄`,
                    case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
                    from fox_tmp.`组别信息`
                    where `年`=year('{0}') and `月`=month('{0}')
                    ) zb
                    on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列` and 
                    zb.`进入账龄第一天`=ml.assign_asset_late_days and zb.`进入账龄第一天`=ml.assign_debtor_late_days)
        
        WHERE ml.mission_log_operator = 'assign'
        AND (ml.mission_log_create_at >= '{0}' AND ml.mission_log_create_at <= '{1}')
        -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
                    )aa
    WHERE `是否分案不均` = 0
    group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`债务人ID`,`主管`
    )a
    left join 
    (select user_id,work_day 
      from 
    fox_ods.ods_fox_collect_attendance_dtl
    where attendance_status=1
    group by user_id,work_day
    )cad
       on (a.`催员ID`=cad.user_id and cad.work_day = substr(a.`分案日期`,1,10))
    left join
    fox_tmp.`主管对应关系` d
    
    on (year(a.`分案日期`)=d.`年` and month(a.`分案日期`)=d.`月` and `主管`=`储备主管`)
    where 1 = 1
    and `账龄` is not null
    
    group by `业务组2`,
    mission_group_name,
    `账龄`,
    `进入账龄第一天`,d.`正式主管中文名`,`催员ID`, substr('{1}',1,10)
    order by `业务组2`
    )
    
    select 
    `统计日期`,
    `业务组`,
     asset_group_name,
     `管理层`,
    round(min(日人均新案分案量)-avg(日人均新案分案量),2) as 最小日均分案数_平均,
    round(max(日人均新案分案量)-avg(日人均新案分案量),2) as 最大日均分案数_平均,
    -- round(avg(日人均新案分案量),2) as 日均新案分案量_mean,
    
    round(min(日人均新案分案金额)-avg(日人均新案分案金额),2) as 最小日均分案金额_平均,
    round(max(日人均新案分案金额)-avg(日人均新案分案金额),2) as 最大日均分案金额_平均,
    -- round(avg(日人均新案分案金额),2) as 日人均新案分案金额_mean,
    
    round(min(日人均新案分案量_上线)-avg(日人均新案分案量_上线),2) as 每日上线最小日均分案量_平均,
    round(max(日人均新案分案量_上线)-avg(日人均新案分案量_上线),2) as 每日上线最大日均分案量_平均,
    -- round(avg(日人均新案分案量_上线),2) as 日均新案分案量_mean_上线,
    
    round(min(日人均新案分案金额_上线)-avg(日人均新案分案金额_上线),2) as 每日上线最小日均分案金额_平均,
    round(max(日人均新案分案金额_上线)-avg(日人均新案分案金额_上线),2) as 每日上线最大日均分案金额_平均
    -- round(avg(日人均新案分案金额_上线),2) as 日人均新案分案金额_mean_上线
    
    
    from aa
    where `管理层` is not null
    group by `业务组`,
     asset_group_name,
     `管理层`,`统计日期`
    """.format(first_day, yesterday + " 23:59:59")
    return 分案监控2


####20241112 国wai
def 承载监控1(first_day, yesterday):
    承载监控1 = """
    select
    `业务组2` as `业务组`,
    mission_group_name as asset_group_name,
    d.`正式主管中文名` as `管理层`,
    renjun_estimated_daily_reminder_debt as 预估日人均新案分案量,
    round(count(distinct `资产ID`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
    round(sum(`分案本金`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`,
    count(distinct `资产ID`) as `日人均新案分案量_分子`,
    count(distinct  concat(`催员ID`,`分案日期`)) as `日人均新案分案量_分母`,
    sum(`分案本金`) as `日人均新案分案金额_分子`,
    count(distinct  concat(`催员ID`,`分案日期`)) as `日人均新案分案金额_分母`
    -- renjun_plan_preman_online_asset as 预估日人均在手案件量
    
    from
    (
    select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`, sum(分案本金) 分案本金
    from
    (
    SELECT      zb.`账龄`,
                zb.`进入账龄第一天`,
               -- ml.mission_log_id `分案id`,
                ml.mission_group_name,
                zb.`业务组` as `业务组2`,
                -- SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
                ml.director_name `主管`,
                ml.group_leader_name `组长`,
                ml.assigned_sys_user_id `催员ID`,
                date(ml.mission_log_create_at) `分案日期`,
                date(unml.mission_log_create_at) `撤案日期`,
                ml.mission_log_asset_id `资产ID`,
                ml.assign_asset_late_days as `逾期天数`,
                ml.assign_principal_amount * 0.01 `分案本金`,
                IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
                展期费,
                总实收,
                if(mlur.mission_log_unassign_reason in ("分案不均","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均
        from ods_fox_mission_log ml
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
       LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
        AND unml.mission_log_operator = 'unassign' 
        AND ( unml.mission_log_create_at >= '{0}' AND unml.mission_log_create_at <= '{1}' ) 
        LEFT JOIN 
        (SELECT
                mission_log_id,
            MAX(repay_date) 还款日期,
        --                 cr.assigned_date,
        --     SUM(repaid_principal_amount),
            SUM( delay_amount)/100 展期费,
            SUM( repaid_principal_amount )/100 催回本金,
                sum(repaid_total_amount)/100 总实收
        FROM
            fox_ods.ods_fox_collect_recovery 
        WHERE
            repay_date >= '{0}'
            AND repay_date <= '{1}'
            and repaid_total_amount >0
            AND batch_num IS NOT NULL
        GROUP BY
            mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
                    left join 
                    (select `年`,`月`,`队列`,`业务组`,`账龄`,
                    case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
                    from fox_tmp.`组别信息`
                    where `年`=year('{0}') and `月`=month('{1}')
                    ) zb
                    on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列` and 
                    zb.`进入账龄第一天`=ml.assign_asset_late_days and zb.`进入账龄第一天`=ml.assign_debtor_late_days)
        
        WHERE ml.mission_log_operator = 'assign'
        -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
        AND (ml.mission_log_create_at >= '{0}' AND ml.mission_log_create_at <= '{1}')
                    )aa
    WHERE `是否分案不均` = 0
    group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`               
    )a
    left join 
    (select user_id,work_day 
      from 
    fox_ods.ods_fox_collect_attendance_dtl
    where attendance_status=1
    group by user_id,work_day
    )cad
       on (a.`催员ID`=cad.user_id and cad.work_day = substr(a.`分案日期`,1,10))
    left join
    fox_tmp.`主管对应关系` d
    
    on (year(a.`分案日期`)=d.`年` and month(a.`分案日期`)=d.`月` and `主管`=`储备主管`)
    LEFT JOIN (
    select
       business_group,
       round(estimated_daily_reminder_debt/plan_human_resources,0) as renjun_estimated_daily_reminder_debt,
       plan_preman_online_asset as renjun_plan_preman_online_asset
       from fox_tmp.temp_load_bearing_monitor_for
    ) yg
    on yg.business_group = mission_group_name
    where 1 = 1
    and d.`正式主管中文名` is not null
     -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
    and `账龄` is not null
    group by `业务组2`,
    mission_group_name,
    d.`正式主管中文名`,
    renjun_estimated_daily_reminder_debt,
    renjun_plan_preman_online_asset
    order by `业务组2`
    """.format(first_day, yesterday + " 23:59:59")
    return 承载监控1


####20241112 国内
def 承载监控2(first_day, yesterday):
    承载监控2 = """
    select 
    `业务组2` as `业务组`,
    mission_group_name as asset_group_name,
    d.`正式主管中文名` as `管理层`,
    renjun_estimated_daily_reminder_debt as 预估日人均新案分案量,
    round(count(distinct `债务人ID`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
    round(sum(`分案本金`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`,
    count(distinct `债务人ID`) as `日人均新案分案量_分子`,
    count(distinct  concat(`催员ID`,`分案日期`)) as `日人均新案分案量_分母`,
    sum(`分案本金`) as `日人均新案分案金额_分子`,
    count(distinct  concat(`催员ID`,`分案日期`)) as `日人均新案分案金额_分母`
    -- renjun_plan_preman_online_asset as 预估日人均在手案件量
    from
    (
    select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`债务人ID`,`主管`, sum(分案本金) 分案本金
    from
    (
    SELECT      zb.`账龄`,
                zb.`进入账龄第一天`,
               -- ml.mission_log_id `分案id`,
                ml.mission_group_name,
                zb.`业务组` as `业务组2`,
                -- SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
                ml.director_name `主管`,
                ml.group_leader_name `组长`,
                ml.assigned_sys_user_id `催员ID`,
                ml.debtor_id `债务人ID`,
                date(ml.mission_log_create_at) `分案日期`,
                date(unml.mission_log_create_at) `撤案日期`,
                ml.mission_log_asset_id `资产ID`,
                ml.assign_asset_late_days as `逾期天数`,
                ml.assign_principal_amount * 0.01 `分案本金`,
                IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
                展期费,
                总实收,
                if(mlur.mission_log_unassign_reason in ("分案不均","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均
        from ods_fox_mission_log ml
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

       LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
             AND unml.mission_log_operator = 'unassign' 
        AND ( unml.mission_log_create_at >= '{0}' AND unml.mission_log_create_at <= '{1}' ) 
        LEFT JOIN 
        (SELECT
                mission_log_id,
            MAX(repay_date) 还款日期,
        --                 cr.assigned_date,
        --     SUM(repaid_principal_amount),
            SUM( delay_amount)/100 展期费,
            SUM( repaid_principal_amount )/100 催回本金,
                sum(repaid_total_amount)/100 总实收
        FROM
            fox_ods.ods_fox_collect_recovery 
        WHERE
            repay_date >= '{0}'
            AND repay_date <= '{1}'
            and repaid_total_amount >0
            AND batch_num IS NOT NULL
        GROUP BY
            mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
                    left join 
                    (select `年`,`月`,`队列`,`业务组`,`账龄`,
                    case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
                    from fox_tmp.`组别信息`
                    where `年`=year('{0}') and `月`=month('{1}')
                    ) zb
                    on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列` and 
                    zb.`进入账龄第一天`=ml.assign_asset_late_days and zb.`进入账龄第一天`=ml.assign_debtor_late_days)

        WHERE ml.mission_log_operator = 'assign'
       -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
        AND (ml.mission_log_create_at >= '{0}' AND ml.mission_log_create_at <= '{1}')
                    )aa
    WHERE `是否分案不均` = 0
    group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`债务人ID`,`主管`

    )a
    left join 
    (select user_id,work_day 
      from 
    fox_ods.ods_fox_collect_attendance_dtl
    where attendance_status=1
    group by user_id,work_day
    )cad
    on (a.`催员ID`=cad.user_id and cad.work_day = substr(a.`分案日期`,1,10))
    left join
    fox_tmp.`主管对应关系` d

    on (year(a.`分案日期`)=d.`年` and month(a.`分案日期`)=d.`月` and `主管`=`储备主管`)
    LEFT JOIN (
    select
       business_group,
       round(estimated_daily_reminder_debt/plan_human_resources,0) as renjun_estimated_daily_reminder_debt,
       plan_preman_online_asset as renjun_plan_preman_online_asset
       from fox_tmp.temp_load_bearing_monitor_for
    ) yg
    on yg.business_group = mission_group_name
    where 1 = 1
    and d.`正式主管中文名` is not null
     -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
    and `账龄` is not null

    group by `业务组2`,
    mission_group_name,
    -- `账龄`,
    -- `进入账龄第一天`,
    d.`正式主管中文名`,
    renjun_estimated_daily_reminder_debt,
    renjun_plan_preman_online_asset
    order by `业务组2`
    """.format(first_day, yesterday + " 23:59:59")
    return 承载监控2


def 新老天数(first_day,yesterday):
    新老天数="""
    SELECT
    uo.user_id,
    DATE ( work_day ),
    uo.NAME,
    o.schedule_status 预排班状态,
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
    return 新老天数

def 最早上线日期():
    最早上线日期="""
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
    return 最早上线日期

def 外呼(first_day,yesterday):
    外呼 = """
    SELECT
    se.dunner_id id,
    date(ch.`call_at`) 日期,
    COUNT(ch.id) 外呼次数,
    SUM(ch.call_time) 通时
    FROM ods_audit_call_history ch
    LEFT JOIN ods_audit_call_history_extend se
    ON ch.id=se.source_id
    WHERE date(ch.`call_at`)>='{0}'
    AND date(ch.`call_at`)<='{1}'
    AND ch.call_channel=1
    and ch.pt_date>='{0}'
    and ch.pt_date<='{1}'
    and se.pt_date >='{0}'
    and se.pt_date <='{1}'
    -- and ch.call_time=0
    GROUP BY 1,2
    """.format(first_day,yesterday)
    return 外呼
def 晋升时间():
    晋升时间 = """
    SELECT
        distinct a.assigned_sys_user_id as "user_id",
        a.mission_group_name as "asset_group_name",
        MIN( mission_log_assigned_date ) over ( PARTITION BY gn ) 首次晋升队列日期 
        FROM(SELECT
        assigned_sys_user_id,
        mission_group_name,
        CASE
			WHEN mission_group_name LIKE "B%" THEN
			"bg" 
			WHEN mission_group_name LIKE "%M2%" THEN
			"m2g" 
			WHEN mission_group_name LIKE "%M3%" THEN
			"m3g" 
			END AS gn,
			mission_log_assigned_date 
		FROM
			ods_fox_mission_log 
		WHERE
			( mission_group_name LIKE "B%" OR mission_group_name LIKE "%M2%" OR mission_group_name LIKE "%M3%" ) 
		)a
    """
    return 晋升时间


def usertable():
    用户表 = """
    SELECT
    su.id user_id,
    su.`no`,
    work_set_up as "office_name"
    -- case work_set_up when "WFH" then "远程" when "Onsite" then "现场" end as "office_name"
    FROM ods_fox_sys_user su
    left join ods_fox_sys_user_extend sue on su.id = sue.user_id
    """
    return 用户表


def 国内分案回款(a, b, c, first_day, yesterday, 新案):
    分案回款 = """
    SELECT
    DATE(detail.`分案时间`) `分案日期`,
    -- detail.`业务组`,
    -- detail.`主管`,
    -- detail.`组长`,
    detail.`催员ID`,
        COUNT(DISTINCT detail.`债务人ID`) `债务人数`,
	COUNT(DISTINCT IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1,detail.`债务人ID`,NULL)) `新案债务人数`,
	COUNT(DISTINCT IF(detail.`期次表催回总金额`>0,detail.`债务人ID`,NULL)) `催回债务人数`,
	COUNT(DISTINCT IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1 and detail.`期次表催回总金额`>0,detail.`债务人ID`,NULL)) `新案催回债务人数`,
	
	COUNT(DISTINCT detail.`资产ID`) `资产数`,
	COUNT(DISTINCT IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1,detail.`资产ID`,NULL)) `新案资产数`,
	COUNT(DISTINCT IF(detail.`期次表催回总金额`>0,detail.`资产ID`,NULL)) `催回资产数`,
	COUNT(DISTINCT IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1 and detail.`期次表催回总金额`>0,detail.`资产ID`,NULL)) `新案催回资产数`,
	
    SUM(IF(detail.`分案ID序列` = 1 AND detail.`是否计入新案分案本金` = 1,detail.`分案本金`,0)) `新案分案本金`,
    SUM(IF(detail.`是否计入新案回款本金` = 1,detail.`期次表催回本金`,0)) `新案回款本金`,
    SUM(IF(detail.`是否计入新案展期` = 1,detail.`期次表展期费用`,0)) `新案展期费用`,
    SUM(IF(detail.`是否计入实收` = 1,detail.`期次表催回总金额`,0)) `总实收`,
    -- m2,m3实收,a组分案回款
    SUM(IF(detail.`是否计入实收` = 1 and 分案时债务逾期天数 >= 61 ,detail.`期次表催回总金额`,0)) `61以上总实收`,
    SUM(IF(detail.`是否计入实收` = 1 and 分案时债务逾期天数 < 61,detail.`期次表催回总金额`,0)) `61以下总实收`,
    SUM(IF(detail.`分案ID序列` = 1 AND 分案时债务逾期天数 = 1 and detail.`是否计入分案本金` = 1,detail.`分案本金`,0)) `D1分案本金`,
    SUM(IF(detail.`是否计入回款本金` = 1 AND 分案时债务逾期天数 = 1,detail.`期次表催回本金`,0)) `D1回款本金`,
    SUM(IF(detail.`分案ID序列` = 1 AND 分案时债务逾期天数 = 4 and detail.`是否计入分案本金` = 1,detail.`分案本金`,0)) `D4分案本金`,
    SUM(IF(detail.`是否计入回款本金` = 1 AND 分案时债务逾期天数 = 4,detail.`期次表催回本金`,0)) `D4回款本金`,
    
    SUM(IF(detail.`分案ID序列` = 1 AND detail.`是否计入分案本金` = 1,detail.`分案本金`,0)) `分案本金`,
    SUM(IF(detail.`是否计入回款本金` = 1,detail.`期次表催回本金`,0)) `回款本金`,
    0 as `展期费用`    
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
    ml.assign_debtor_late_days `分案时债务逾期天数`,
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
    IFNULL(if(apr.repaid_total_amount < 0,0,apr.repaid_total_amount) * 0.01,0) `期次表催回本金`,
    IFNULL(apr.repaid_fee_amount * 0.01,0) `期次表催回手续费`,
    IFNULL(apr.repaid_penalty_amount * 0.01,0) `期次表催回罚息`,
    IFNULL(apr.repaid_interest_amount * 0.01,0) `期次表催回利息`,
    IFNULL(if(apr.repaid_total_amount < 0,0,apr.repaid_total_amount) * 0.01,0) `期次表催回总金额`,
    cr.batch_num `还款批次号`,
    apr.repaid_period `还款期次`,
    ml.overdue_period `分案期次`,
    cr.overdue_flag `是否逾期`,
    ROW_NUMBER() OVER(PARTITION BY ml.mission_log_id) `分案ID序列`,
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,5) and day(ml.mission_log_create_at) <> 2,0)) AND
    (NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,7),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
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
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,5) and day(ml.mission_log_create_at) <> 2,0)) AND
    (NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,7),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
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
    (IF({5},1,0))) `是否计入新案分案本金`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,5) and day(ml.mission_log_create_at) <> 2,0)) AND
    (NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,7),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
    -- 因为撤案原因不计入
    (NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
    -- 因为撤案原因并且没有回款不计入
    (NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
    -- 长假、稽核下线当天回款不计入
    AND (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
		(NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5") and day(cr.create_at) <> 2,0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
	-- 预提醒只统计当期回款本金，其他业务组统计逾期回款
    (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0,1,
    IF(cr.overdue_flag = 1 AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0 ,1,0))) ) `是否计入回款本金`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,5) and day(ml.mission_log_create_at) <> 2,0)) AND
    (NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,7),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
    -- 因为撤案原因不计入
    (NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
    -- 因为撤案原因并且没有回款不计入
    (NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
    -- 长假、稽核下线当天回款不计入
    AND (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
		(NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5") and day(cr.create_at) <> 2,0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
	-- 预提醒只统计当期回款本金，其他业务组统计逾期回款
    (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0,1,
    IF(cr.overdue_flag = 1 AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0 ,1,0))) AND
    -- 各业务组新案判断
    (IF({5},1,0))) `是否计入新案回款本金`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,5) and day(ml.mission_log_create_at) <> 2,0)) AND
    (NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,7),0)) AND
    -- 当天分案当天展期不计入(展期无回款本金，不用判断)
    (NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
    -- 长假、稽核下线当天回款不计入
    (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
	(NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5") and day(cr.create_at) <> 2,0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
	-- 展期金额不为空
    (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.delay_amount,0) <> 0,1,
    IF(LOCATE('P',ml.mission_group_name) = 0 AND IFNULL(apr.delay_amount,0) <> 0,1,0)))) `是否计入展期`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,5) and day(ml.mission_log_create_at) <> 2,0)) AND
    (NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,7),0)) AND
    -- 当天分案当天展期不计入(展期无回款本金，不用判断)
    (NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
    -- 长假、稽核下线当天回款不计入
    (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
	(NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5") and day(cr.create_at) <> 2,0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
    -- 展期金额不为空
     (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.delay_amount,0) <> 0,1,
    IF(LOCATE('P',ml.mission_group_name) = 0 AND IFNULL(apr.delay_amount,0) <> 0,1,0))) AND
     -- 各业务组新案判断
    (IF({5},1,0)))  `是否计入新案展期`,
    
    
      -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,5) and day(ml.mission_log_create_at) <> 2,0)) AND
    (NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,7),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
    -- 因为撤案原因不计入
    (NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
    -- 因为撤案原因并且没有回款不计入
    (NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) and
      -- 长假、稽核下线当天回款不计入
    (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
	(NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5") and day(cr.create_at) <> 2,0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1)))) `是否计入实收`
    FROM
    ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
    LEFT JOIN ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id 
    AND unml.mission_log_operator = 'unassign' 
    AND ( unml.mission_log_create_at >= "{3}" AND unml.mission_log_create_at <= "{4}" ) 
    LEFT JOIN ods_fox_collect_attendance_dtl cad ON ml.assigned_sys_user_id = cad.user_id
    AND DATE(ml.mission_log_create_at) = cad.work_day
    LEFT JOIN ods_fox_collect_recovery cr ON ml.mission_log_id = cr.mission_log_id
    AND ( cr.create_at >= "{3}" AND cr.create_at <= "{4}") 
    AND cr.repaid_total_amount >0
    AND cr.batch_num IS NOT NULL
    LEFT JOIN ods_fox_asset_period_recovery apr ON cr.batch_num = apr.batch_num
    LEFT JOIN ods_fox_collect_attendance_dtl cadcr ON cr.sys_user_id = cadcr.user_id
    AND DATE(cr.create_at) = cadcr.work_day
    LEFT JOIN ods_fox_asset asset ON ml.mission_log_asset_id = asset.asset_id
    WHERE
    ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{3}" AND ml.mission_log_create_at <= "{4}")
    AND ml.mission_group_name NOT IN ('IVR组','客维组','94','cultivate')) detail
    GROUP BY
    DATE(detail.`分案时间`),
    -- detail.`业务组`,
    -- detail.`主管`,
    -- detail.`组长`,
    detail.`催员ID`
    ORDER BY
    detail.`催员ID`,
    DATE(detail.`分案时间`)
    """.format(a, b, c, first_day, yesterday, 新案)
    return 分案回款


def 案件明细(a,b,c,first_day,yesterday,新案):
    案件明细="""
    SELECT
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
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
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
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
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
    (IF({5},1,0))) `是否计入新案分案本金`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
    -- 因为撤案原因不计入
    (NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
    -- 因为撤案原因并且没有回款不计入
    (NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
    -- 长假、稽核下线当天回款不计入
    AND (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
    -- 预提醒只统计当期回款本金，其他业务组统计逾期回款
    (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0,1,
    IF(cr.overdue_flag = 1 AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0 ,1,0))) ) `是否计入回款本金`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
    -- 因为撤案原因不计入
    (NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
    -- 因为撤案原因并且没有回款不计入
    (NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
    -- 长假、稽核下线当天回款不计入
    AND (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
    -- 预提醒只统计当期回款本金，其他业务组统计逾期回款
    (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0,1,
    IF(cr.overdue_flag = 1 AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0 ,1,0))) AND
    -- 各业务组新案判断
    (IF({5},1,0))) `是否计入新案回款本金`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- 当天分案当天展期不计入(展期无回款本金，不用判断)
    (NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
    -- 长假、稽核下线当天回款不计入
    (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),1) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
    -- 展期金额不为空
    (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.delay_amount,0) <> 0,1,
    IF(LOCATE('P',ml.mission_group_name) = 0 AND IFNULL(apr.delay_amount,0) <> 0,1,0)))) `是否计入展期`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- 当天分案当天展期不计入(展期无回款本金，不用判断)
    (NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
    -- 长假、稽核下线当天回款不计入
     (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),1) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
    -- 展期金额不为空
     (IF(LOCATE('P',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.delay_amount,0) <> 0,1,
    IF(LOCATE('P',ml.mission_group_name) = 0 AND IFNULL(apr.delay_amount,0) <> 0,1,0))) AND
     -- 各业务组新案判断
    (IF({5},1,0)))  `是否计入新案展期`,
    
    
      -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- -- 分案时催员短假、旷工并且当天所有撤案不计入
    (NOT IF(IFNULL(FIND_IN_SET(cad.attendance_status_flag,"4,6"),0) AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at),1,0)) AND
    -- 因为撤案原因不计入
    (NOT FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{2}")) AND
    -- 因为撤案原因并且没有回款不计入
    (NOT (FIND_IN_SET(IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason),"{0}") AND IFNULL(apr.repaid_total_amount * 0.01,0) = 0)) 
      -- 长假、稽核下线当天回款不计入
    AND NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),0) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) `是否计入实收`
    FROM
    ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
    LEFT JOIN ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id 
    AND unml.mission_log_operator = 'unassign' 
    AND ( unml.mission_log_create_at >= "{3}" AND unml.mission_log_create_at <= "{4}" ) 
    LEFT JOIN ods_fox_collect_attendance_dtl cad ON ml.assigned_sys_user_id = cad.user_id
    AND DATE(ml.mission_log_create_at) = cad.work_day
    LEFT JOIN ods_fox_collect_recovery cr ON ml.mission_log_id = cr.mission_log_id
    AND ( cr.create_at >= "{3}" AND cr.create_at <= "{4}" ) 
    AND cr.batch_num IS NOT NULL
    LEFT JOIN ods_fox_asset_period_recovery apr ON cr.batch_num = apr.batch_num
    LEFT JOIN ods_fox_collect_attendance_dtl cadcr ON cr.sys_user_id = cadcr.user_id
    AND DATE(cr.create_at) = cadcr.work_day
    LEFT JOIN ods_fox_asset asset ON ml.mission_log_asset_id = asset.asset_id
    WHERE
    ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{3}" AND ml.mission_log_create_at <= "{4}")
    AND ml.mission_group_name NOT IN ('PHCC','IVR','WIZ-IVR','cultivate')
    """.format(a, b, c, first_day + " 06:00:00", yesterday + " 23:59:59", 新案)
    return 案件明细


def 架构mail(dt1):
    架构mail = """
        SELECT
        uo.pt_date `架构日期`,
        SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `架构表业务组`,
        SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 2 ), ',',- 1 ) `架构表主管`,
        SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 3 ), ',',- 1 ) `架构表组长`,
        sue.position `职位`,
        uo.user_id `员工ID`,
        su.`no` `员工工号`,
        uo.`name` `员工` ,
        IF(cad.attendance_status = 1,'是','否') `是否上线`,
        IF(ISNULL(sue.dimission_date),'否','是') `是否离职`,
        sue.dimission_date `离职日期`,
        IF(sue.count_churn = 1,'是',IF(sue.count_churn = 0,'否','未填')) `是否计入流失`
        FROM fox_dw.dwd_fox_user_organization_df uo
        LEFT JOIN fox_ods.ods_fox_collect_attendance_dtl cad ON uo.user_id = cad.user_id AND uo.pt_date = cad.work_day AND cad.work_day = '{0}'
        LEFT JOIN fox_ods.ods_fox_sys_user su ON uo.user_id = su.id 
        LEFT JOIN fox_ods.ods_fox_sys_user_extend sue ON su.id = sue.user_id
        WHERE uo.pt_date = '{0}' 
        ORDER BY 2 desc,1
        """.format(dt1)
    return 架构mail

def 架构表1(first_day, yesterday, group):
    架构表 = """
    SELECT pt_date,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 2 ), ',', - 1 ) `manager_user_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 3 ), ',', - 1 ) `leader_user_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 4 ), ',', - 1 ) `组员`,
            uo.user_id,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE uo.user_id = su.id) `user_no`,
            (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.user_id = su.id) `组员`,
            -- parent_user_name,
            uo.parent_user_id `leader_user_id`,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE uo.parent_user_id = su.id) `leader_user_no`,
            (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.parent_user_id = su.id) `leader_user_name`,
            manager_user_id,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE su.`name` = uo.manager_user_name) `manager_user_no`,
            manager_user_name
            -- (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.manager_user_id = su.id) `manager_user_name`
            FROM
                    fox_tmp.user_organization_df uo
            WHERE
                    uo.pt_date>="{0}"
                    and LENGTH(parent_user_names) - LENGTH(REPLACE(parent_user_names, ',', '')) = 3
                    and uo.pt_date<="{1}"
			AND SUBSTRING_INDEX(uo.asset_group_name,',',-1) in (SELECT distinct(asset_group_name) FROM `ods_fox_collect_attendance_dtl`
				where work_day >="{0}" and work_day <="{1}" and asset_group_name not like "Telesales%")
    """.format(first_day + " 00:00:00", yesterday + " 23:59:59", group)
    return 架构表

def 菲律宾架构表(first_day, yesterday, group):
    架构表 = """
    SELECT pt_date,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 2 ), ',', - 1 ) `manager_user_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 3 ), ',', - 1 ) `leader_user_name`,
            -- SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 4 ), ',', - 1 ) `组员`,
            uo.user_id,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE uo.user_id = su.id) `user_no`,
            (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.user_id = su.id) `组员`,
            -- parent_user_name,
            uo.parent_user_id `leader_user_id`,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE uo.parent_user_id = su.id) `leader_user_no`,
            (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.parent_user_id = su.id) `leader_user_name`,
            manager_user_id,
            (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE su.`name` = uo.manager_user_name) `manager_user_no`,
            manager_user_name
            -- (SELECT `name` from fox_ods.ods_fox_sys_user su WHERE uo.manager_user_id = su.id) `manager_user_name`
            FROM
                    fox_tmp.tmp_fox_user_organization_df uo
            WHERE
                    uo.pt_date>="{0}"
                    and LENGTH(parent_user_names) - LENGTH(REPLACE(parent_user_names, ',', '')) = 3
                    and uo.pt_date<="{1}"
			AND SUBSTRING_INDEX(uo.asset_group_name,',',-1) in (SELECT distinct(asset_group_name) FROM `ods_fox_collect_attendance_dtl`
				where work_day >="{0}" and work_day <="{1}" and asset_group_name not like "Telesales%")
    """.format(first_day + " 00:00:00", yesterday + " 23:59:59", group)
    return 架构表

def 备份(yesterday):
    sql = """
    INSERT INTO fox_tmp.user_organization_df
    SELECT * FROM fox_dw.dwd_fox_user_organization_df WHERE pt_date = "{}";
    """.format(yesterday)
    return sql
# def 修改(yesterday):
#     sql = """
#     DELETE FROM fox_tmp.user_organization_df2 WHERE pt_date = "{}"
#     AND user_id in (
#     '7323709fbe6b43d0a7f414069bab4cf7',
#     'a67756fc02e8462a8fc78caee23b9ba6');
#     """.format(yesterday)
#     return sql

def 总分案回款(first_day,yesterday):
    sql = """
    WITH a1 as (
SELECT
uo.pt_date,uo.user_id,mx.* from
(
SELECT  
            ml.mission_log_id `分案id`,
            ml.mission_group_name `业务组`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            ml.assigned_sys_user_id `催员ID`,
            date(ml.mission_log_create_at) `分案日期`,
            date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,
            ml.assign_principal_amount * 0.01 `分案本金`,
            IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
            总实收,
            if(mlur.mission_log_unassign_reason = "分案不均",1,0) 是否分案不均
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
    LEFT JOIN ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
    AND unml.mission_log_operator = 'unassign' 
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" ) 
	right JOIN fox_dw.dwd_fox_user_organization_df uo on uo.user_id = ml.assigned_sys_user_id AND uo.pt_date = date(ml.mission_log_create_at)
    LEFT JOIN 
    (SELECT
            mission_log_id,
        MAX(repay_date) 还款日期,
    --                 cr.assigned_date,
    --     SUM(repaid_principal_amount),
        SUM( delay_amount)/100 展期费,
        SUM( repaid_principal_amount )/100 + SUM( delay_amount)/100 催回本金,
        sum(repaid_total_amount)/100 总实收
    FROM
        ods_fox_collect_recovery 
    WHERE
        repay_date >= "{0}"
        AND repay_date <= "{1}"
        and repaid_total_amount >0
        AND batch_num IS NOT NULL
    GROUP BY
        mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
) mx	
right JOIN (select pt_date,user_id from fox_dw.dwd_fox_user_organization_df WHERE pt_date >= "{0}" AND pt_date <= "{1}") uo on uo.user_id = mx.催员ID AND uo.pt_date = date(mx.分案日期)
)
SELECT d.*,if((累计分案-累计撤案) is null,累计分案,累计分案-累计撤案) 日在手案件,
if((累计分案本金-累计撤案本金) is null,累计分案本金,累计分案本金-累计撤案本金) 日在手金额
from (
SELECT c.*,sum(分案数) over(partition by ID order by 日期) 累计分案,
    sum(撤案数) over(partition by ID order by 日期 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 累计撤案,
    sum(`分案本金(总)`) over(partition by ID order by 日期) 累计分案本金,
    sum(`撤案本金(总)`) over(partition by ID order by 日期 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 累计撤案本金
from(
SELECT COALESCE(a.user_id,b.催员ID) as ID,
COALESCE(a.pt_date,b.撤案日期) as 日期,
coalesce(`分案本金(总)`,0) as `分案本金(总)`,
coalesce(`催回本金(总)`,0) as `催回本金(总)`,
coalesce(分案数,0) as `分案数`,
coalesce(`实收(总)`,0) as `实收(总)`,
coalesce(撤案数,0) as `撤案数`,
coalesce(`撤案本金(总)`,0) as `撤案本金(总)`
from
(
    SELECT user_id,pt_date,
    sum(if(是否分案不均 = 0,分案本金,0)) `分案本金(总)`,
    round(sum(if(是否分案不均 = 0,催回本金,0)),2) `催回本金(总)`,
    count(资产ID) 分案数,
    round(sum(总实收),2) `实收(总)`
    from a1
    GROUP BY user_id,pt_date
		) a  
		full JOIN (
		SELECT 催员ID,撤案日期,
		count(if(撤案日期 is not null ,资产ID,null)) 撤案数,		
    sum(if(是否分案不均 = 0 and 撤案日期 is not null,分案本金,0)) `撤案本金(总)`
    from a1
    GROUP BY 催员ID,撤案日期
		) b on a.user_id = b.催员ID AND b.撤案日期 = a.pt_date
		WHERE COALESCE(a.pt_date,b.撤案日期) IS NOT NULL
		ORDER BY 催员ID,pt_date)c)d
    """.format(first_day +" 00:00:00",yesterday+" 23:59:59")
    return sql
def 国内总分案回款(first_day,yesterday):
    sql = """
        WITH a1 as (
SELECT
uo.pt_date,uo.user_id,mx.* from
(
SELECT  
            ml.mission_log_id `分案id`,
            ml.mission_group_name `业务组`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            ml.assigned_sys_user_id `催员ID`,
            date(ml.mission_log_create_at) `分案日期`,
            date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,
            ml.assign_principal_amount * 0.01 `分案本金`,
            IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
            总实收,
            if(mlur.mission_log_unassign_reason in ("分案不均","存在逾期案件时撤预提醒案件","存在到期案件时撤预提醒案件"),1,0) 是否分案不均
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id
    LEFT JOIN ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
    AND unml.mission_log_operator = 'unassign' 
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" ) 
	right JOIN fox_dw.dwd_fox_user_organization_df uo on uo.user_id = ml.assigned_sys_user_id AND uo.pt_date = date(ml.mission_log_create_at)
    LEFT JOIN 
    (SELECT
            mission_log_id,
        MAX(repay_date) 还款日期,
    --                 cr.assigned_date,
    --     SUM(repaid_principal_amount),
        SUM( delay_amount)/100 展期费,
        SUM( repaid_principal_amount )/100 催回本金,
            sum(repaid_total_amount)/100 总实收
    FROM
        ods_fox_collect_recovery 
    WHERE
        repay_date >= "{0}"
        AND repay_date <= "{1}"
        and repaid_total_amount >0
        AND batch_num IS NOT NULL
    GROUP BY
        mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
) mx	
right JOIN (select pt_date,user_id from fox_dw.dwd_fox_user_organization_df WHERE pt_date >= "{0}" AND pt_date <= "{1}") uo on uo.user_id = mx.催员ID AND uo.pt_date = date(mx.分案日期)
)

SELECT d.*,if((累计分案-累计撤案) is null,累计分案,累计分案-累计撤案) 日在手案件,
if((累计分案本金-累计撤案本金) is null,累计分案本金,累计分案本金-累计撤案本金) 日在手金额
from (
SELECT c.*,sum(分案数) over(partition by ID order by 日期) 累计分案,
    sum(撤案数) over(partition by ID order by 日期 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 累计撤案,
    sum(`分案本金(总)`) over(partition by ID order by 日期) 累计分案本金,
    sum(`撤案本金(总)`) over(partition by ID order by 日期 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 累计撤案本金
from(
SELECT COALESCE(a.user_id,b.催员ID) as ID,
COALESCE(a.pt_date,b.撤案日期) as 日期,
coalesce(`分案本金(总)`,0) as `分案本金(总)`,
coalesce(`催回本金(总)`,0) as `催回本金(总)`,
coalesce(分案数,0) as `分案数`,
coalesce(`实收(总)`,0) as `实收(总)`,
coalesce(撤案数,0) as `撤案数`,
coalesce(`撤案本金(总)`,0) as `撤案本金(总)`
from
(
    SELECT user_id,pt_date,
    sum(if(是否分案不均 = 0,分案本金,0)) `分案本金(总)`,
    round(sum(if(是否分案不均 = 0,催回本金,0)),2) `催回本金(总)`,
    count(资产ID) 分案数,
    round(sum(总实收),2) `实收(总)`
    from a1
    GROUP BY user_id,pt_date
		) a  
		full JOIN (
		SELECT 催员ID,撤案日期,
		count(if(撤案日期 is not null ,资产ID,null)) 撤案数,		
    sum(if(是否分案不均 = 0 and 撤案日期 is not null,分案本金,0)) `撤案本金(总)`
    from a1
    GROUP BY 催员ID,撤案日期
		) b on a.user_id = b.催员ID AND b.撤案日期 = a.pt_date
		WHERE COALESCE(a.pt_date,b.撤案日期) IS NOT NULL
		ORDER BY 催员ID,pt_date)c)d
        """.format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql

def 添加主管(yesterday):
    sql = """
	select 正式主管,储备主管,正式主管中文名
    from fox_bi.主管对应关系
    where 1=1
    and 年 = year('{0}')
	and 月 = MONTH('{0}')
    """.format(yesterday)
    return sql
def 添加业务组(yesterday):
    sql = """
	select 队列,业务组,账龄,新案,Out_self
    from fox_bi.组别信息
    where 1=1
    and 年 = year('{0}')
	and 月 = MONTH('{0}')
    """.format(yesterday)
    return sql
