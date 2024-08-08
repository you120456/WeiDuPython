def 架构表(first_day,yesterday,group):
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
                    uo.pt_date>="{0}"
                    and uo.pt_date<="{1}"
    AND SUBSTRING_INDEX(uo.asset_group_name,',',-1) in ({2})
    """.format(first_day +" 00:00:00",yesterday+" 23:59:59",group)
    return 架构表

def 分案回款(a,b,c,first_day,yesterday,新案):
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
    (IF(LOCATE('PreRemind',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0,1,
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
    (IF(LOCATE('PreRemind',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.repaid_principal_amount * 0.01,0) <> 0,1,
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
    (IF(LOCATE('PreRemind',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.delay_amount,0) <> 0,1,
    IF(LOCATE('PreRemind',ml.mission_group_name) = 0 AND IFNULL(apr.delay_amount,0) <> 0,1,0)))) `是否计入展期`,
    
    
    -- 分案时催员长假、稽核下线不计入
    ((NOT IFNULL(FIND_IN_SET(cad.attendance_status_flag,"5,7"),0)) AND
    -- 当天分案当天展期不计入(展期无回款本金，不用判断)
    (NOT (IF(ISNULL(unml.mission_log_create_at),'当前未撤案',mlur.mission_log_unassign_reason) = '展期' AND DATE(ml.mission_log_create_at) = DATE(unml.mission_log_create_at))) AND
    -- 长假、稽核下线当天回款不计入
     (NOT (IFNULL(FIND_IN_SET(cadcr.attendance_status_flag,"5,7"),1) AND IFNULL(DATE(cr.create_at) = (cadcr.work_day),1))) AND
    -- 展期金额不为空
     (IF(LOCATE('PreRemind',ml.mission_group_name) AND ml.overdue_period = apr.repaid_period AND IFNULL(apr.delay_amount,0) <> 0,1,
    IF(LOCATE('PreRemind',ml.mission_group_name) = 0 AND IFNULL(apr.delay_amount,0) <> 0,1,0))) AND
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
    detail.`业务组`,
    detail.`主管`,
    detail.`组长`,
    detail.`催员ID`
    ORDER BY
    detail.`催员ID`,
    DATE(detail.`分案时间`)
    """.format(a,b,c,first_day +" 06:00:00",yesterday+" 23:59:59",新案)
    return 分案回款

def 离职时间():
    离职时间 = """
        SELECT
            sue.user_id,
            su.`name`,
            su.del_date '删除时间',
            su.NO,
            sue.dimission_date '最后工作日',-- 业绩截至日期
            sue.dimission_reason -- uo.NAME,
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
    return 离职时间

def 新老天数(first_day,yesterday):
    新老天数="""
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
    -- and ch.call_time=0
    GROUP BY 1,2
    """.format(first_day,yesterday)
    return 外呼
def 晋升时间():
    晋升时间 = """
    SELECT
            uo.`name` as '催员名',
            a.assigned_sys_user_id as "user_id",
            a.mission_group_name as "asset_group_name",
            a.首次晋升队列日期 
    FROM
            (
            SELECT
                    assigned_sys_user_id,
                    mission_group_name,
                    date(min( mission_log_assigned_date )) AS "首次晋升队列日期" 
            FROM
                    ods_fox_mission_log 
            WHERE
                    (mission_group_name LIKE "B%" OR  mission_group_name LIKE "M2%")
            GROUP BY
                    1,2
            ORDER BY
                    date(
                    min( mission_log_assigned_date )) desc
                    ) a
            LEFT JOIN ods_fox_user_organization uo ON a.assigned_sys_user_id = uo.user_id
    """
    return 晋升时间


def usertable():
    用户表 = """
    SELECT
    su.id user_id, su.`no`, su.`name`
    FROM
    ods_fox_sys_user su """
    return 用户表