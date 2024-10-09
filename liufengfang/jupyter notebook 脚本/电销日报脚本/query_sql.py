#!/usr/bin/env python
# coding: utf-8


# 1.业绩数据 分案数 放款数
performance_sql = '''
SELECT
	DATE(tml.create_at) `assign_date`,
	tml.assign_sys_user_id `user_id`,
	tml.id `mission_log_id`,
	tml.task_number `task_number`,

    
    (CASE   
		WHEN ISNULL(untml.reason) OR  
			(untml.reason NOT IN ('短假撤案', '黑名单上报')) AND  
			NOT (untml.reason IN ('长假撤案', '稽核下线', '离职撤案') AND IFNULL(follow.follow_status, 0) = 0)  
		THEN 1  -- 是，计入分案  
		ELSE 0  -- 否，不计入分案  
		END) AS `include_assign`,
	
	(
		((
			ISNULL(untml.reason) OR
			-- 完全不计入分案
			(untml.reason NOT IN ('短假撤案', '黑名单上报')) AND
			-- 未跟进过的不计入分案
			NOT (untml.reason IN ('长假撤案', '稽核下线', '离职撤案') AND (IFNULL(follow.follow_status,0) = 0))
		))	AND
		IF(tt.on_loan_time > tml.create_at,1,ISNULL(tt.on_loan_time)) AND
		
		(t.customer_status IN ('pre_rejected','not_applied','applied','rejected','not_submit','cancel','grant_fail','on_loan')) AND
		
		(CASE tml.asset_type_group_name 
			WHEN 'Telesales A' THEN (ROW_NUMBER() OVER(PARTITION BY tml.asset_type_group_name,tml.task_number ORDER BY tml.create_at DESC))
		ELSE '非A队列任务' END) = 1
		
	) > 0 `include_application`,
	
	(
	-- 已放款
	(NOT ISNULL(tt.mission_log_id)) AND
	-- 未取消借款
	(ISNULL(tt.cancel_loan_time)) AND
	-- 分案前未放款
	IFNULL((tt.on_loan_time > tml.create_at),0) AND
	-- `不是撤案后放款(不包含放款撤案)`
	IF(NOT ISNULL(tt.on_loan_time) AND ((tt.on_loan_time <= untml.create_at) OR ISNULL(untml.create_at)),1,0)
	)  > 0 `include_loan`
	
FROM
	ods_fox_tel_sale_mission_log tml
	LEFT JOIN ods_fox_tel_sale_mission_log untml ON tml.id = untml.assign_mission_log_id 
	AND untml.operator = 'unassign' 
	AND ( untml.create_at >= '{0}' AND untml.create_at <= '{1}' )
    LEFT JOIN ods_fox_tel_sale_task t ON tml.task_number = t.task_number
	LEFT JOIN (
	SELECT DISTINCT
		tcr.mission_log_id,
		1 `follow_status` 
	FROM
		ods_fox_tel_sale_contact_record tcr 
	WHERE
		( tcr.create_at >= '{0}' AND tcr.create_at <= '{1}' ) 
		AND tcr.call_uuid <> '' 
	) follow ON tml.id = follow.mission_log_id
	LEFT JOIN ods_fox_tel_sale_loan_task tt ON tml.id = tt.mission_log_id 
	AND ( tt.on_loan_time >= '{0}' AND tt.on_loan_time <= '{2}' )
WHERE
	tml.operator = 'assign' 
	AND ( tml.create_at >= '{0}' AND tml.create_at <= '{1}' )
'''



# 2.出勤天数
attendance_days_sql = '''
SELECT
	tad.user_id,
	tad.work_day,
	tad.attendance_status 
FROM
	ods_fox_tel_sale_attendance_dtl tad 
WHERE
	(tad.work_day >= '{0}' AND tad.work_day <= '{1}' )
	AND attendance_status = 1
'''




# 3.过程数据 通话时长，外呼次数
process_sql = '''
SELECT
	DATE(call_detail.dial_time) `call_date`,
	call_detail.dunner_id `user_id`,
	SUM( call_detail.talk_duration ) `talk_duration`,
	COUNT(IF( call_detail.talk_duration > 0, 1, NULL )) `call_times` 
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
	( tch.dial_time >= '{0}' AND tch.dial_time <= '{1}' )) `call_detail` 
	-- 通话明细 end-- 
GROUP BY
	DATE(call_detail.dial_time),
	call_detail.dunner_id
'''



# 4.查询离职日期
dimission_date_sql = '''
SELECT
	sue.user_id,
	sue.dimission_date
FROM
	ods_fox_sys_user_extend sue
WHERE
	DATE(sue.dimission_date) <= '{0}'
'''




# 5.首次上线日期查询
first_online_sql = '''
SELECT
	tad.user_id `坐席ID`,
	MIN( tad.work_day ) `电销首次上线日期` 
FROM
	ods_fox_tel_sale_attendance_dtl tad 
WHERE
	tad.attendance_status = 1 
GROUP BY
	tad.user_id
'''




# 6.应出勤天数
required_attendance_days = '''
SELECT
	SUM( tad.schedule_status ) `schedule_count` 
FROM
	ods_fox_tel_sale_attendance_dtl tad 
WHERE
	tad.work_day >= '{0}' 
	AND tad.work_day <= LAST_DAY('{1}') 
GROUP BY
	tad.user_id 
ORDER BY
	schedule_count DESC 
	LIMIT 1
'''




# 7.查询当月所有电销排班人员
current_month_first_scheduled = '''
SELECT
	tad.user_id,
	MIN( work_day ) `current_month_first_scheduled`
FROM
	ods_fox_tel_sale_attendance_dtl tad
	WHERE
    -- 各个电销国家对应电销队列
	tad.asset_group_name in ({2})
	AND (tad.work_day>='{0}' AND tad.work_day<='{1}')
GROUP BY
	tad.user_id
'''




# 8.电销人员每一天架构
organization_sql = '''
SELECT
	uo.pt_date `uo_date`,
	SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `group`,
	SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 2 ), ',',- 1 ) `director`,
	SUBSTRING_INDEX( SUBSTRING_INDEX( uo.parent_user_names, ',', 3 ), ',',- 1 ) `team_leader`,
	uo.user_id `user_id`,
	uo.`name` `user_name`
FROM
	fox_dw.dwd_fox_user_organization_df uo
WHERE
	uo.pt_date >= '{0}' AND uo.pt_date <= '{1}'
	AND SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) IN ({2})
'''



# 9.每日分案架构
assign_organization = '''
SELECT DISTINCT
	DATE(tml.create_at) `assign_date`,
	tml.asset_type_group_name `group`,
	tml.director_name `director`,
	tml.leader_name `team_leader`,
	tml.assign_sys_user_id `user_id`,
	tml.assign_sys_user_name `user_name`
FROM
	ods_fox_tel_sale_mission_log tml
WHERE
	tml.create_at >= '{0}' AND tml.create_at <= '{1}'
	AND tml.operator = 'assign' '''



# 10.查询工号及是否计入流失
no_count_churn = '''
SELECT
	sue.user_id,
    su.`name`,
	su.`no`,
	sue.count_churn 
FROM
	ods_fox_sys_user_extend sue
	LEFT JOIN ods_fox_sys_user su ON sue.user_id = su.id '''