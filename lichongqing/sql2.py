def 团队过程指标1(first_day, yesterday):
    sql = """
    WITH `分案还款及wa数据` AS (
    SELECT
    assign.debtor_id AS '债务人ID',
    SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) AS '组别',
    assign.assigned_sys_user_id AS '催员ID',
    uo.manager_user_name '主管',
    -- 	sum( wa.WA回复数 ) WA回复数,
    -- 	sum( wa.WA发送数 ) WA发送数,
        IF(SUM(r.实收) > 0,1,0) `是否还款`
    FROM
        ods_fox_mission_log assign
        LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on date(assign.mission_log_create_at) = uo.pt_date AND assign.assigned_sys_user_id = uo.user_id
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
    -- 			WA跟进记录
    -- 	LEFT JOIN (
    -- 	SELECT
    -- 		chat.mission_log_id,
    -- 		count(IF(( action_type = 1 ), chat.mission_log_id, NULL )) 'WA发送数',
    -- 		count(IF(( action_type = 2 ), chat.mission_log_id, NULL )) 'WA回复数'
    -- 	FROM
    -- 		fox_dw.dwd_fox_whatsapp_chat_record chat
    -- 	WHERE
    -- 		chat.mission_log_id <> 0
    -- 		AND chat.chat_type = 1
    -- 		AND chat.send_time >= "{0}"
    -- 		AND chat.send_time < "{1}"
    -- 	GROUP BY
    -- 		1
    -- 	) wa ON wa.mission_log_id = assign.mission_log_id
        LEFT JOIN (
        SELECT
            cr.mission_log_id,
            SUM(cr.repaid_total_amount)/100 `实收`
        FROM
            ods_fox_collect_recovery cr
        WHERE
            cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
        AND unassign.mission_log_operator = 'unassign'
        AND unassign.mission_log_create_at <= "{1}"
        WHERE 1 = 1
        AND assign.mission_log_operator = 'assign'
        -- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
        AND assign.director_name IS NOT NULL
        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
        AND NOT (mlur.mission_log_unassign_reason IN ( '分案不均','UNEVEN_WITHDRAW_CASE') AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
    GROUP BY 1,2,3,4),

    拨打数据 AS (
    SELECT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
        sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
        and ods_audit_call_history.pt_date >= "{0}"
        and ods_audit_call_history.pt_date <= "{1}"
        and ods_audit_call_history_extend.pt_date >= "{0}"
        and ods_audit_call_history_extend.pt_date <= "{1}"
    GROUP BY
        催员ID,债务人ID),

    拨打首通接通 AS(
    SELECT DISTINCT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
        and ods_audit_call_history.pt_date >= "{0}"
        and ods_audit_call_history.pt_date <= "{1}"
        and ods_audit_call_history_extend.pt_date >= "{0}"
        and ods_audit_call_history_extend.pt_date <= "{1}"),

    短信数据 AS (
    SELECT
	ods_audit_sms_history_extend.dunner_id AS '催员ID',
	ods_audit_sms_history_extend.debtor_id AS '债务人ID',
	count(ods_audit_sms_history.id) as '短信发送次数',
	COUNT(DISTINCT ods_fox_sms_record.sms_record_ticket) '短信回执次数'
    FROM
	ods_audit_sms_history
	JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
	LEFT JOIN ods_fox_sms_record ON ods_audit_sms_history.origin_sms_record_id = ods_fox_sms_record.sms_record_id
    WHERE
	ods_audit_sms_history.sms_at >= "{0}"
	AND ods_audit_sms_history.sms_at <= "{1}"
	and sms_channel =1
    GROUP BY
	催员ID,债务人ID)
    -- 	,

    -- 墨西哥wa AS (
    -- 	SELECT
    -- 		t1.dunner_id `催员ID`,
    -- 		t1.debtor_id `债务人ID`,
    -- 		count( t1.debtor_id ) AS `wa点击次数`,
    -- 		count( DISTINCT t1.debtor_id ) AS `wa点击个数`
    -- 	FROM
    -- 		(
    -- 		SELECT
    -- 			t.create_user_id AS `dunner_id`,
    -- 			get_json_object ( t.content, '$.debtorId' ) `debtor_id`,
    -- 			t.create_at
    -- 		FROM
    -- 			`ods_fox_oper_business_log` t
    -- 		WHERE
    -- 			date( t.create_at ) >= "{0}" AND date ( t.create_at ) <= "{1}"
    -- 			AND t.title = 'WHATSAPP_LINK') t1
    -- 	GROUP BY t1.dunner_id,t1.debtor_id)

    SELECT
        date("{1}") as 统计日期,
        t.业务组,
        t.组别 as 队列,
        -- t.类别,
        t.正式主管中文名 as '管理层',
    -- 	ROUND(COUNT(IF(t.WA发送数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(COUNT(IF(t.wa点击次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(SUM(t.WA发送数) / COUNT(t.债务人ID),4) `案均WA触达次数`,
    -- 	ROUND(SUM(t.WA回复数) / COUNT(t.债务人ID),4) `案均WA接通次数`,
        ROUND(COUNT(IF(t.短信发送次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `短信覆盖率`,
        ROUND(SUM(t.短信发送次数) / COUNT(t.债务人ID),4) `案均短信发送次数`,
	    ROUND(SUM(t.短信回执次数) / COUNT(t.债务人ID),4) `案均短信回执次数`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL))  `可联还款率_分子`,
        COUNT(t.债务人ID) `可联还款率_分母`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
        ,COUNT(t.债务人ID) `总债务人量`,
        -- COUNT(IF(t.WA发送数 > 0,1,NULL)) `wa覆盖债务人量`,
        -- SUM(t.WA发送数) `wa发送量`,
        -- SUM(t.WA回复数) `wa回复次数`,
        COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
        SUM(t.短信发送次数) `短信发送量`,
        COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
        SUM(t.拨打次数) `电话拨打次数`,
        COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
        COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
        COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`
    FROM
    (SELECT
        e.业务组,
        IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
        a.组别,
        a.主管,
        d.正式主管中文名,
        a.催员ID,
        a.债务人ID,
    -- 	a.WA发送数,
    -- 	a.WA回复数,
    -- 	mw.wa点击次数,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b1.是否首通接通,
        c.短信发送次数,
        c.短信回执次数
    FROM
        `分案还款及wa数据` a
        LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
        AND a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
        AND a.`债务人ID` = b1.`债务人ID`
        LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
        AND a.`债务人ID` = c.`债务人ID`
        LEFT JOIN fox_tmp.`主管对应关系` d ON a.主管 = d.储备主管
        AND d.年 = year("{0}") AND d.月 = month("{0}")
        LEFT JOIN fox_tmp.组别信息 e ON a.组别 = e.队列
        AND e.年 = year("{0}") AND e.月 = month("{0}")
    WHERE a.组别 NOT IN ('cultivate','PKCC','THCC','MXCC','客维组')
    and 正式主管中文名 is not null
        ) t
    GROUP BY 1,2,3,4
    """.format(first_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 团队过程指标2(first_day, yesterday):
    sql = """
    WITH `分案还款及wa数据` AS (
    SELECT
    assign.debtor_id AS '债务人ID',
    SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) AS '组别',
    assign.assigned_sys_user_id AS '催员ID',
    uo.manager_user_name '主管',
    -- 	sum( wa.WA回复数 ) WA回复数,
    -- 	sum( wa.WA发送数 ) WA发送数,
        IF(SUM(r.实收) > 0,1,0) `是否还款`
    FROM
        ods_fox_mission_log assign
        LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on date(assign.mission_log_create_at) = uo.pt_date AND assign.assigned_sys_user_id = uo.user_id
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
        LEFT JOIN (
        SELECT
            chat.mission_log_id,
            count(IF(( action_type = 1 ), chat.mission_log_id, NULL )) 'WA发送数',
            count(IF(( action_type = 2 ), chat.mission_log_id, NULL )) 'WA回复数'
        FROM
            fox_dw.dwd_fox_whatsapp_chat_record chat
        WHERE
            chat.mission_log_id <> 0
            AND chat.chat_type = 1
            AND chat.send_time >= "{0}"
            AND chat.send_time <= "{1}"
        GROUP BY
            1
        ) wa ON wa.mission_log_id = assign.mission_log_id
        LEFT JOIN (
        SELECT
            cr.mission_log_id,
            SUM(cr.repaid_total_amount)/100 `实收`
        FROM
            ods_fox_collect_recovery cr
        WHERE
            cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
        AND unassign.mission_log_operator = 'unassign'
        AND unassign.mission_log_create_at <= "{1}"
        WHERE 1 = 1
        AND assign.mission_log_operator = 'assign'
        -- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
        AND assign.director_name IS NOT NULL
        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
        AND NOT ( mlur.mission_log_unassign_reason IN ( '分案不均', 'UNEVEN_WITHDRAW_CASE' ) AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
    GROUP BY 1,2,3,4),

    拨打数据 AS (
    SELECT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
        sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
    GROUP BY
        催员ID,债务人ID),

    拨打首通接通 AS(
    SELECT DISTINCT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"),

    短信数据 AS (
    SELECT
	ods_audit_sms_history_extend.dunner_id AS '催员ID',
	ods_audit_sms_history_extend.debtor_id AS '债务人ID',
	count(ods_audit_sms_history.id) as '短信发送次数',
	COUNT(DISTINCT ods_fox_sms_record.sms_record_ticket) '短信回执次数'
    FROM
	ods_audit_sms_history
	JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
	LEFT JOIN ods_fox_sms_record ON ods_audit_sms_history.origin_sms_record_id = ods_fox_sms_record.sms_record_id
    WHERE
	ods_audit_sms_history.sms_at >= "{0}"
	AND ods_audit_sms_history.sms_at <= "{1}"
	and sms_channel =1
    GROUP BY
	催员ID,债务人ID),
    墨西哥wa AS (
        SELECT
            t1.dunner_id `催员ID`,
            t1.debtor_id `债务人ID`,
            count( t1.debtor_id ) AS `wa点击次数`,
            count( DISTINCT t1.debtor_id ) AS `wa点击个数`
        FROM
            (
            SELECT
                t.create_user_id AS `dunner_id`,
                get_json_object ( t.content, '$.debtorId' ) `debtor_id`,
                t.create_at
            FROM
                `ods_fox_oper_business_log` t
            WHERE
                date( t.create_at ) >= "{0}" AND date ( t.create_at ) <= "{1}"
                AND t.title = 'WHATSAPP_LINK') t1
        GROUP BY t1.dunner_id,t1.debtor_id)

    SELECT
        date("{1}") as 统计日期,
        t.业务组,
        t.组别 as 队列,
        t.正式主管中文名 `管理层`,
    -- 	ROUND(COUNT(IF(t.WA发送数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
        ROUND(COUNT(IF(t.wa点击次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(SUM(t.WA发送数) / COUNT(t.债务人ID),4) `案均WA触达次数`,
    -- 	ROUND(SUM(t.WA回复数) / COUNT(t.债务人ID),4) `案均WA接通次数`,
        ROUND(COUNT(IF(t.短信发送次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `短信覆盖率`,
        ROUND(SUM(t.短信发送次数) / COUNT(t.债务人ID),4) `案均短信发送次数`,
	    ROUND(SUM(t.短信回执次数) / COUNT(t.债务人ID),4) `案均短信回执次数`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL))  `可联还款率_分子`,
        COUNT(t.债务人ID) `可联还款率_分母`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
        ,COUNT(t.债务人ID) `总债务人量`,
        -- COUNT(IF(t.WA发送数 > 0,1,NULL)) `wa覆盖债务人量`,
        -- SUM(t.WA发送数) `wa发送量`,
        -- SUM(t.WA回复数) `wa回复次数`,
        COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
        SUM(t.短信发送次数) `短信发送量`,
        COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
        SUM(t.拨打次数) `电话拨打次数`,
        COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
        COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
        COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`
    FROM
    (SELECT
        e.业务组,
        IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
        a.主管,
        a.组别,
        d.正式主管中文名,
        a.催员ID,
        a.债务人ID,
        -- a.WA发送数,
        -- a.WA回复数,
        mw.wa点击次数,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b1.是否首通接通,
        c.短信发送次数,
        c.短信回执次数
    FROM
        `分案还款及wa数据` a
        LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
        AND a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
        AND a.`债务人ID` = b1.`债务人ID`
        LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
        AND a.`债务人ID` = c.`债务人ID`
        LEFT JOIN 墨西哥wa mw ON a.`催员ID` = mw.`催员ID`
        AND a.`债务人ID` = mw.`债务人ID`
        LEFT JOIN fox_bi.`主管对应关系` d ON a.主管 = d.储备主管
        AND d.年 = year("{0}") AND d.月 = month("{0}")
        LEFT JOIN fox_bi.组别信息 e ON a.组别 = e.队列
        AND e.年 = year("{0}") AND e.月 = month("{0}")
    WHERE a.组别 NOT IN ('cultivate','PKCC','PHCC','MXCC','THCC')
    AND 正式主管中文名 is not null
        ) t
    GROUP BY 1,2,3,4
    """.format(first_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 组长过程指标1(third_day, yesterday):
    sql = """
    WITH a1 as (SELECT uo.pt_date,uo.user_id,mx.* from(
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
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <=  "{1}" )
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
right JOIN (select pt_date,user_id from fox_dw.dwd_fox_user_organization_df WHERE pt_date >= "{0}" AND pt_date <= "{1}") uo on uo.user_id = mx.催员ID AND uo.pt_date = date(mx.分案日期)),

`累计新分案及在手` AS (
    select manager_user_name 主管,asset_group_name 组别,parent_user_name 组长,
        sum(分案数) 累计分案,
		round(avg(if(e.日期 = date("{1}"),日在手案件,null)),2) 本日在手案件,
		round(avg(if(e.日期 = date("{1}"),日在手金额,null)),2) 本日在手案件金额,
		round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手案件,null)),2) 本日在线在手案件,
        round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手金额,null)),2) 本日在线在手案件金额
        from (
SELECT d.*,if((累计分案-累计撤案) is null,累计分案,累计分案-累计撤案) 日在手案件,
if((累计分案本金-累计撤案本金) is null,累计分案本金,累计分案本金-累计撤案本金) 日在手金额,
uo.manager_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
uo.parent_user_name,dtl.attendance_status

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
		LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = d.日期 AND uo.user_id = d.ID
		left JOIN ods_fox_collect_attendance_dtl dtl ON dtl.work_day = d.日期 and dtl.user_id = d.ID
		)e
		where
		 attendance_status is not null
		group by manager_user_name,asset_group_name,parent_user_name),
         `排班及出勤` AS(
SELECT SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) 组别,uo.manager_user_name,uo.parent_user_name,sum(schedule_status) 排班天数,sum(attendance_status) 出勤天数
from ods_fox_collect_attendance_dtl dtl
LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = dtl.work_day AND uo.user_id = dtl.user_id
WHERE dtl.work_day >= "{0}"
AND dtl.work_day <= "{1}"
GROUP BY uo.manager_user_name,uo.parent_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 )),
            `分案还款及wa数据` AS (
        SELECT
            assign.debtor_id AS '债务人ID',
            assign.mission_group_name AS '组别',
            assign.assigned_sys_user_id AS '催员ID',
            assign.group_leader_name AS '组长',
            assign.director_name '主管',
        -- 	sum( wa.WA回复数 ) WA回复数,
        -- 	sum( wa.WA发送数 ) WA发送数,
            IF(SUM(r.实收) > 0,1,0) `是否还款`
        FROM
            ods_fox_mission_log assign
            LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
        -- 			WA跟进记录
        -- 	LEFT JOIN (
        -- 	SELECT
        -- 		chat.mission_log_id,
        -- 		count(IF(( action_type = 1 ), chat.mission_log_id, NULL )) 'WA发送数',
        -- 		count(IF(( action_type = 2 ), chat.mission_log_id, NULL )) 'WA回复数'
        -- 	FROM
        -- 		fox_dw.dwd_fox_whatsapp_chat_record chat
        -- 	WHERE
        -- 		chat.mission_log_id <> 0
        -- 		AND chat.chat_type = 1
        -- 		AND chat.send_time >= "{0}"
        -- 		AND chat.send_time < "{1}"
        -- 	GROUP BY
        -- 		1
        -- 	) wa ON wa.mission_log_id = assign.mission_log_id
            LEFT JOIN (
            SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
            FROM
                ods_fox_collect_recovery cr
            WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
            GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
            LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
            AND unassign.mission_log_operator = 'unassign'
            AND unassign.mission_log_create_at <= "{1}"
            WHERE 1 = 1
            AND assign.mission_log_operator = 'assign'
            -- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
            AND assign.director_name IS NOT NULL
            AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
            AND NOT ( mlur.mission_log_unassign_reason IN ( '分案不均', 'UNEVEN_WITHDRAW_CASE') AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
        GROUP BY 1,2,3,4,5),

        拨打数据 AS (
        SELECT
            ods_audit_call_history.dunner_id AS '催员ID',
            debtor_id AS '债务人ID',
            sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
            sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数' ,
            sum(ods_audit_call_history.call_time) '通话时长'
        FROM
            ods_audit_call_history
            JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
        WHERE
            ods_audit_call_history.call_at >= "{0}"
            AND ods_audit_call_history.call_at <= "{1}"
        GROUP BY
            催员ID,债务人ID),

        拨打首通接通 AS(
        SELECT DISTINCT
            ods_audit_call_history.dunner_id AS '催员ID',
            debtor_id AS '债务人ID',
            IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
        FROM
            ods_audit_call_history
            JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
        WHERE
            ods_audit_call_history.call_at >= "{0}"
            AND ods_audit_call_history.call_at <= "{1}"),

        短信数据 AS (
        SELECT
            ods_audit_sms_history_extend.dunner_id AS '催员ID',
            ods_audit_sms_history_extend.debtor_id AS '债务人ID',
            count(*) as '短信发送次数'
        FROM
            ods_audit_sms_history
            JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
        WHERE
            ods_audit_sms_history.sms_at >= "{0}"
            AND ods_audit_sms_history.sms_at <= "{1}"
            and sms_channel =1
        GROUP BY
            催员ID,债务人ID),

     `过程结果数据` AS(
        SELECT
            t.组别,
            t.业务组,
            t.类别,
            t.主管,
            t.正式主管中文名,
            t.组长,
            COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
            SUM(t.短信发送次数) `短信发送量`,
            COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
            SUM(t.拨打次数) `电话拨打次数`,
            COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
            COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
            COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`,
            SUM(通话时长) `通话时长`,
            SUM(接通次数) `接通次数`
        FROM
        (SELECT
            e.业务组,
            IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
            a.组别,
            a.组长,
            a.主管,
            d.正式主管中文名,
            a.催员ID,
            a.债务人ID,
        -- 	a.WA发送数,
        -- 	a.WA回复数,
        -- 	mw.wa点击次数,
            a.是否还款,
            b.拨打次数,
            b.接通次数,
            b.通话时长,
            b1.是否首通接通,
            c.短信发送次数
        FROM
            `分案还款及wa数据` a
            LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
            AND a.`债务人ID` = b.`债务人ID`
            LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
            AND a.`债务人ID` = b1.`债务人ID`
            LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
            AND a.`债务人ID` = c.`债务人ID`
    -- 		LEFT JOIN 墨西哥wa mw ON a.`催员ID` = mw.`催员ID`
    -- 		AND a.`债务人ID` = mw.`债务人ID`
            LEFT JOIN fox_tmp.`主管对应关系` d ON a.主管 = d.储备主管
            AND d.年 = year("{0}") AND d.月 = month("{0}")
            LEFT JOIN fox_tmp.组别信息 e ON a.组别 = e.队列
            AND e.年 = year("{0}") AND e.月 = month("{0}")
        WHERE a.组别 NOT IN ('cultivate','PKCC','THCC','MXCC','94','客维组','IVR组')
            ) t
        GROUP BY 1,2,3,4,5,6),
    `日人均新案量` as(
select
`业务组2` as `业务组`,
`队列`,
-- mission_group_name as asset_group_name,
d.`正式主管中文名` as `管理层`,
`组长`,

-- `账龄`,
-- `进入账龄第一天`,
count(distinct `资产ID`) as `月累计资产数`,
round(count(distinct `资产ID`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
sum(`分案本金`) as `分案本金`,
round(sum(`分案本金`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`

from
(
select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`, sum(分案本金) 分案本金
from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
           -- ml.mission_log_id `分案id`,
            ml.mission_group_name,
            zb.`业务组` as `业务组2`,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
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
            if(mlur.mission_log_unassign_reason in( "分案不均","UNEVEN_WITHDRAW_CASE","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
         AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" )
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
                zb.`进入账龄第一天`=ml.assign_asset_late_days  and zb.`进入账龄第一天`=ml.assign_debtor_late_days)

	left join
		(select user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1) as group_name
		 from fox_dw.dwd_fox_user_organization_df -- 印尼
		-- fox_tmp.user_organization_df-- 除印尼和中国外的其他国家
		group by user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1)

		)uo
		on (ml.assigned_sys_user_id=uo.user_id and substr(ml.mission_log_create_at,1,10)=substr(uo.pt_date,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
    -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
                )aa
 WHERE `是否分案不均` = 0
group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`

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
 -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
and `账龄` is not null
and d.`正式主管中文名` is not null

group by `业务组2`,
-- mission_group_name,
-- `账龄`,
-- `进入账龄第一天`,
d.`正式主管中文名`,`组长`,`队列`
order by `业务组2`
)




SELECT  date("{1}") as 统计日期,
        y.业务组,
				y.组别 as "队列",
				y.正式主管中文名 as 管理层,
				(SELECT `no` from fox_ods.ods_fox_sys_user su WHERE a.组长 = su.`name`) `leader_user_no`,
				a.组长,
				排班天数,
				出勤天数,
        round(出勤天数/排班天数,4)出勤率,
		-- round(累计分案/出勤天数,0) 日人均新案量,
		c.`日人均新案分案量` as 日人均新案量,
		-- 本日在手案件,本日在手案件金额,本日在线在手案件,本日在线在手案件金额,
		round(电话拨打覆盖债务人量/出勤天数,0) 日人均拨打案件数,
        round(电话拨打次数/出勤天数,0) 日人均拨打次数,
        round(通话时长/出勤天数,0) 日人均通话时长,
        round(接通次数/出勤天数,0) 日人均接通次数,
        null as 日人均WA发送案件数,null as 日人均WA发送次数,
        round(短信覆盖债务人量/出勤天数,0) 日人均短信发送案件数,
        round(短信发送量/出勤天数,0) 日人均短信发送条数

        FROM 累计新分案及在手 a
        left JOIN 排班及出勤 z ON a.组长 = z.parent_user_name and a.主管 = z.manager_user_name and a.组别= z.组别
				left join 过程结果数据 y on y.组长 = a.组长 and a.主管 = y.主管 and y.组别 = a.组别
				left join `日人均新案量` c
		on y.`业务组`=c.`业务组` and y.`组别`=c.`队列` and
				y.`正式主管中文名`=c.`管理层` and
				a.`组长`=c.`组长`
				WHERE 正式主管中文名 is not null AND 排班天数 is not NULL
        """.format(third_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 组长过程指标中国(third_day, yesterday):
    sql = """
    WITH a1 as (SELECT uo.pt_date,uo.user_id,mx.* from(
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
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <=  "{1}" )
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
right JOIN (select pt_date,user_id from fox_dw.dwd_fox_user_organization_df WHERE pt_date >= "{0}" AND pt_date <= "{1}") uo on uo.user_id = mx.催员ID AND uo.pt_date = date(mx.分案日期)),

`累计新分案及在手` AS (
    select manager_user_name 主管,asset_group_name 组别,parent_user_name 组长,
        sum(分案数) 累计分案,
		round(avg(if(e.日期 = date("{1}"),日在手案件,null)),2) 本日在手案件,
		round(avg(if(e.日期 = date("{1}"),日在手金额,null)),2) 本日在手案件金额,
		round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手案件,null)),2) 本日在线在手案件,
        round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手金额,null)),2) 本日在线在手案件金额
        from (
SELECT d.*,if((累计分案-累计撤案) is null,累计分案,累计分案-累计撤案) 日在手案件,
if((累计分案本金-累计撤案本金) is null,累计分案本金,累计分案本金-累计撤案本金) 日在手金额,
uo.manager_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
uo.parent_user_name,dtl.attendance_status

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
		LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = d.日期 AND uo.user_id = d.ID
		left JOIN ods_fox_collect_attendance_dtl dtl ON dtl.work_day = d.日期 and dtl.user_id = d.ID
		)e
		where
		 attendance_status is not null
		group by manager_user_name,asset_group_name,parent_user_name),
         `排班及出勤` AS(
SELECT SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) 组别,uo.manager_user_name,uo.parent_user_name,sum(schedule_status) 排班天数,sum(attendance_status) 出勤天数
from ods_fox_collect_attendance_dtl dtl
LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = dtl.work_day AND uo.user_id = dtl.user_id
WHERE dtl.work_day >= "{0}"
AND dtl.work_day <= "{1}"
GROUP BY uo.manager_user_name,uo.parent_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 )),
            `分案还款及wa数据` AS (
        SELECT
            assign.debtor_id AS '债务人ID',
            assign.mission_group_name AS '组别',
            assign.assigned_sys_user_id AS '催员ID',
            assign.group_leader_name AS '组长',
            assign.director_name '主管',
        -- 	sum( wa.WA回复数 ) WA回复数,
        -- 	sum( wa.WA发送数 ) WA发送数,
            IF(SUM(r.实收) > 0,1,0) `是否还款`
        FROM
            ods_fox_mission_log assign
            LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
        -- 			WA跟进记录
        -- 	LEFT JOIN (
        -- 	SELECT
        -- 		chat.mission_log_id,
        -- 		count(IF(( action_type = 1 ), chat.mission_log_id, NULL )) 'WA发送数',
        -- 		count(IF(( action_type = 2 ), chat.mission_log_id, NULL )) 'WA回复数'
        -- 	FROM
        -- 		fox_dw.dwd_fox_whatsapp_chat_record chat
        -- 	WHERE
        -- 		chat.mission_log_id <> 0
        -- 		AND chat.chat_type = 1
        -- 		AND chat.send_time >= "{0}"
        -- 		AND chat.send_time < "{1}"
        -- 	GROUP BY
        -- 		1
        -- 	) wa ON wa.mission_log_id = assign.mission_log_id
            LEFT JOIN (
            SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
            FROM
                ods_fox_collect_recovery cr
            WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
            GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
            LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
            AND unassign.mission_log_operator = 'unassign'
            AND unassign.mission_log_create_at <= "{1}"
            WHERE 1 = 1
            AND assign.mission_log_operator = 'assign'
            -- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
            AND assign.director_name IS NOT NULL
            AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
            AND NOT ( mlur.mission_log_unassign_reason IN ( '分案不均', 'UNEVEN_WITHDRAW_CASE') AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
        GROUP BY 1,2,3,4,5),

        拨打数据 AS (
        SELECT
            ods_audit_call_history.dunner_id AS '催员ID',
            debtor_id AS '债务人ID',
            sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
            sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数' ,
            sum(ods_audit_call_history.call_time) '通话时长'
        FROM
            ods_audit_call_history
            JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
        WHERE
            ods_audit_call_history.call_at >= "{0}"
            AND ods_audit_call_history.call_at <= "{1}"
        GROUP BY
            催员ID,债务人ID),

        拨打首通接通 AS(
        SELECT DISTINCT
            ods_audit_call_history.dunner_id AS '催员ID',
            debtor_id AS '债务人ID',
            IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
        FROM
            ods_audit_call_history
            JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
        WHERE
            ods_audit_call_history.call_at >= "{0}"
            AND ods_audit_call_history.call_at <= "{1}"),

        短信数据 AS (
        SELECT
            ods_audit_sms_history_extend.dunner_id AS '催员ID',
            ods_audit_sms_history_extend.debtor_id AS '债务人ID',
            count(*) as '短信发送次数'
        FROM
            ods_audit_sms_history
            JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
        WHERE
            ods_audit_sms_history.sms_at >= "{0}"
            AND ods_audit_sms_history.sms_at <= "{1}"
            and sms_channel =1
        GROUP BY
            催员ID,债务人ID),

     `过程结果数据` AS(
        SELECT
            t.组别,
            t.业务组,
            t.类别,
            t.主管,
            t.正式主管中文名,
            t.组长,
            COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
            SUM(t.短信发送次数) `短信发送量`,
            COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
            SUM(t.拨打次数) `电话拨打次数`,
            COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
            COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
            COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`,
            SUM(通话时长) `通话时长`,
            SUM(接通次数) `接通次数`
        FROM
        (SELECT
            e.业务组,
            IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
            a.组别,
            a.组长,
            a.主管,
            d.正式主管中文名,
            a.催员ID,
            a.债务人ID,
        -- 	a.WA发送数,
        -- 	a.WA回复数,
        -- 	mw.wa点击次数,
            a.是否还款,
            b.拨打次数,
            b.接通次数,
            b.通话时长,
            b1.是否首通接通,
            c.短信发送次数
        FROM
            `分案还款及wa数据` a
            LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
            AND a.`债务人ID` = b.`债务人ID`
            LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
            AND a.`债务人ID` = b1.`债务人ID`
            LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
            AND a.`债务人ID` = c.`债务人ID`
    -- 		LEFT JOIN 墨西哥wa mw ON a.`催员ID` = mw.`催员ID`
    -- 		AND a.`债务人ID` = mw.`债务人ID`
            LEFT JOIN fox_tmp.`主管对应关系` d ON a.主管 = d.储备主管
            AND d.年 = year("{0}") AND d.月 = month("{0}")
            LEFT JOIN fox_tmp.组别信息 e ON a.组别 = e.队列
            AND e.年 = year("{0}") AND e.月 = month("{0}")
        WHERE a.组别 NOT IN ('cultivate','PKCC','THCC','MXCC','94','客维组','IVR组')
            ) t
        GROUP BY 1,2,3,4,5,6),

`日人均新案量` as(
select
`业务组2` as `业务组`,
`队列`,
-- mission_group_name as asset_group_name,
d.`正式主管中文名` as `管理层`,
`组长`,

-- `账龄`,
-- `进入账龄第一天`,
count(distinct `债务人ID`) as `月累计资产数`,
round(count(distinct `债务人ID`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
sum(`分案本金`) as `分案本金`,
round(sum(`分案本金`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`

from
(
select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`, `债务人ID`,sum(分案本金) 分案本金
from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
           -- ml.mission_log_id `分案id`,
            ml.mission_group_name,
            zb.`业务组` as `业务组2`,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
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
            if(mlur.mission_log_unassign_reason in ("分案不均","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均,
			ml.debtor_id as `债务人ID`
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
         AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" )
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

	left join
		(select user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1) as group_name
		 from fox_tmp.dwd_fox_user_organization_df
		group by user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1)

		)uo
		on (ml.assigned_sys_user_id=uo.user_id and substr(ml.mission_log_create_at,1,10)=substr(uo.pt_date,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
    -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
                )aa
 WHERE `是否分案不均` = 0
group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`,`债务人ID`

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
 -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
and `账龄` is not null
and d.`正式主管中文名` is not null

group by `业务组2`,
-- mission_group_name,
-- `账龄`,
-- `进入账龄第一天`,
d.`正式主管中文名`,`组长`,`队列`
order by `业务组2`
)








SELECT  date("{1}") as 统计日期,
        y.业务组,
				y.组别 as "队列",
				y.正式主管中文名 as 管理层,
				(SELECT `no` from fox_ods.ods_fox_sys_user su WHERE a.组长 = su.`name`) `leader_user_no`,
				a.组长,
				排班天数,
				出勤天数,
        round(出勤天数/排班天数,4)出勤率,
		c.`日人均新案分案量` as 日人均新案量,
		-- 本日在手案件,本日在手案件金额,
		-- 本日在线在手案件,本日在线在手案件金额,
		round(电话拨打覆盖债务人量/出勤天数,0) 日人均拨打案件数,
        round(电话拨打次数/出勤天数,0) 日人均拨打次数,
        round(通话时长/出勤天数,0) 日人均通话时长,
        round(接通次数/出勤天数,0) 日人均接通次数,
        null as 日人均WA发送案件数,null as 日人均WA发送次数,
        round(短信覆盖债务人量/出勤天数,0) 日人均短信发送案件数,
        round(短信发送量/出勤天数,0) 日人均短信发送条数

        FROM 累计新分案及在手 a
        left JOIN 排班及出勤 z ON a.组长 = z.parent_user_name and a.主管 = z.manager_user_name and a.组别= z.组别
				left join 过程结果数据 y on y.组长 = a.组长 and a.主管 = y.主管 and y.组别 = a.组别
		left join `日人均新案量` c
		on y.`业务组`=c.`业务组` and y.`组别`=c.`队列` and
				y.`正式主管中文名`=c.`管理层` and
				a.`组长`=c.`组长`
				WHERE 正式主管中文名 is not null AND 排班天数 is not NULL
        """.format(third_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 组长过程指标2(third_day, yesterday):
    sql = """
    WITH a1 as (SELECT uo.pt_date,uo.user_id,mx.* from(
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
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <=  "{1}" )
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
right JOIN (select pt_date,user_id from fox_dw.dwd_fox_user_organization_df WHERE pt_date >= "{0}" AND pt_date <= "{1}") uo on uo.user_id = mx.催员ID AND uo.pt_date = date(mx.分案日期)),

`累计新分案及在手` AS (
    select manager_user_name 主管,asset_group_name 组别,parent_user_name 组长,
        sum(累计分案) 累计新分案,
		round(avg(if(e.日期 = date("{1}"),日在手案件,null)),2) 本日在手案件,
		round(avg(if(e.日期 = date("{1}"),日在手金额,null)),2) 本日在手案件金额,
		round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手案件,null)),2) 本日在线在手案件,
        round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手金额,null)),2) 本日在线在手案件金额
        from (
SELECT d.*,if((累计分案-累计撤案) is null,累计分案,累计分案-累计撤案) 日在手案件,
if((累计分案本金-累计撤案本金) is null,累计分案本金,累计分案本金-累计撤案本金) 日在手金额,
uo.manager_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
uo.parent_user_name,dtl.attendance_status

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
		LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = d.日期 AND uo.user_id = d.ID
		left JOIN ods_fox_collect_attendance_dtl dtl ON dtl.work_day = d.日期 and dtl.user_id = d.ID
		)e
		where
		 attendance_status is not null
		group by manager_user_name,asset_group_name,parent_user_name),
         `排班及出勤` AS(
SELECT SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) 组别,uo.manager_user_name,uo.parent_user_name,sum(schedule_status) 排班天数,sum(attendance_status) 出勤天数
from ods_fox_collect_attendance_dtl dtl
LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = dtl.work_day AND uo.user_id = dtl.user_id
WHERE dtl.work_day >= "{0}"
AND dtl.work_day <= "{1}"
GROUP BY uo.manager_user_name,uo.parent_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 )),
		`分案还款及wa数据` AS (
	SELECT
		assign.debtor_id AS '债务人ID',
		assign.mission_group_name AS '组别',
		assign.assigned_sys_user_id AS '催员ID',
		assign.group_leader_name AS '组长',
		assign.director_name '主管',
	-- 	sum( wa.WA回复数 ) WA回复数,
	-- 	sum( wa.WA发送数 ) WA发送数,
		IF(SUM(r.实收) > 0,1,0) `是否还款`
	FROM
		ods_fox_mission_log assign
		LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
	-- 			WA跟进记录
	-- 	LEFT JOIN (
	-- 	SELECT
	-- 		chat.mission_log_id,
	-- 		count(IF(( action_type = 1 ), chat.mission_log_id, NULL )) 'WA发送数',
	-- 		count(IF(( action_type = 2 ), chat.mission_log_id, NULL )) 'WA回复数'
	-- 	FROM
	-- 		fox_dw.dwd_fox_whatsapp_chat_record chat
	-- 	WHERE
	-- 		chat.mission_log_id <> 0
	-- 		AND chat.chat_type = 1
	-- 		AND chat.send_time >= "{0}"
	-- 		AND chat.send_time < "{1}"
	-- 	GROUP BY
	-- 		1
	-- 	) wa ON wa.mission_log_id = assign.mission_log_id
		LEFT JOIN (
		SELECT
			cr.mission_log_id,
			SUM(cr.repaid_total_amount)/100 `实收`
		FROM
			ods_fox_collect_recovery cr
		WHERE
			cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
		GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
		LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
		AND unassign.mission_log_operator = 'unassign'
		AND unassign.mission_log_create_at <= "{1}"
		WHERE 1 = 1
		AND assign.mission_log_operator = 'assign'
		-- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
		AND assign.director_name IS NOT NULL
		AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
		AND NOT ( mlur.mission_log_unassign_reason IN ( '分案不均', 'UNEVEN_WITHDRAW_CASE' ) AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
	GROUP BY 1,2,3,4,5),

	拨打数据 AS (
	SELECT
		ods_audit_call_history.dunner_id AS '催员ID',
		debtor_id AS '债务人ID',
		sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
		sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数' ,
		sum(ods_audit_call_history.call_time) '通话时长'
	FROM
		ods_audit_call_history
		JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
	WHERE
		ods_audit_call_history.call_at >= "{0}"
		AND ods_audit_call_history.call_at <= "{1}"
	GROUP BY
		催员ID,债务人ID),

	拨打首通接通 AS(
	SELECT DISTINCT
		ods_audit_call_history.dunner_id AS '催员ID',
		debtor_id AS '债务人ID',
		IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
	FROM
		ods_audit_call_history
		JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
	WHERE
		ods_audit_call_history.call_at >= "{0}"
		AND ods_audit_call_history.call_at <= "{1}"),

	短信数据 AS (
	SELECT
		ods_audit_sms_history_extend.dunner_id AS '催员ID',
		ods_audit_sms_history_extend.debtor_id AS '债务人ID',
		count(*) as '短信发送次数'
	FROM
		ods_audit_sms_history
		JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
	WHERE
		ods_audit_sms_history.sms_at >= "{0}"
		AND ods_audit_sms_history.sms_at <= "{1}"
		and sms_channel =1
	GROUP BY
		催员ID,债务人ID),
	-- 	,

	墨西哥wa AS (
		SELECT
			t1.dunner_id `催员ID`,
			t1.debtor_id `债务人ID`,
			count( t1.debtor_id ) AS `wa点击次数`,
			count( DISTINCT t1.debtor_id ) AS `wa点击个数`
		FROM
			(
			SELECT
				t.create_user_id AS `dunner_id`,
				get_json_object ( t.content, '$.debtorId' ) `debtor_id`,
				t.create_at
			FROM
				`ods_fox_oper_business_log` t
			WHERE
				date( t.create_at ) >= "{0}" AND date ( t.create_at ) <= "{1}"
				AND t.title = 'WHATSAPP_LINK') t1
		GROUP BY t1.dunner_id,t1.debtor_id),
 `过程结果数据` AS(
	SELECT
		t.业务组,
		t.类别,
		t.组别,
		t.主管,
		t.正式主管中文名,
		t.组长,
		sum(t.wa点击次数) wa点击次数,
-- 		ROUND(COUNT(IF(t.WA发送数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
-- 		ROUND(COUNT(IF(t.wa点击次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
	-- 	ROUND(SUM(t.WA发送数) / COUNT(t.债务人ID),4) `案均WA触达次数`,
	-- 	ROUND(SUM(t.WA回复数) / COUNT(t.债务人ID),4) `案均WA接通次数`,
	-- 		ROUND(COUNT(IF(t.短信发送次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `短信覆盖率`,
	-- 		ROUND(SUM(t.短信发送次数) / COUNT(t.债务人ID),4) `案均短信发送次数`,
	-- 		ROUND('',4) `案均短信回执次数`,
	-- 		ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
	-- 		ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
	-- 		ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
	-- 		ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
	-- 		ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`,
	-- 		COUNT(t.债务人ID) `总债务人量`,
-- 		COUNT(IF(t.WA发送数 > 0,1,NULL)) `wa覆盖债务人量`,
	-- 	SUM(t.WA发送数) `wa发送量`,
	-- 	SUM(t.WA回复数) `wa回复次数`,
		COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
		SUM(t.短信发送次数) `短信发送量`,
		COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
		SUM(t.拨打次数) `电话拨打次数`,
		COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
		COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
		COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`,
		SUM(通话时长) `通话时长`,
		SUM(接通次数) `接通次数`
	FROM
	(SELECT
		e.业务组,
		IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
		a.组别,
		a.组长,
		a.主管,
		d.正式主管中文名,
		a.催员ID,
		a.债务人ID,
	-- 	a.WA发送数,
	-- 	a.WA回复数,
		mw.wa点击次数,

		a.是否还款,
		b.拨打次数,
		b.接通次数,
		b.通话时长,
		b1.是否首通接通,
		c.短信发送次数
	FROM
		`分案还款及wa数据` a
		LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
		AND a.`债务人ID` = b.`债务人ID`
		LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
		AND a.`债务人ID` = b1.`债务人ID`
		LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
		AND a.`债务人ID` = c.`债务人ID`
		LEFT JOIN 墨西哥wa mw ON a.`催员ID` = mw.`催员ID`
	  AND a.`债务人ID` = mw.`债务人ID`
		LEFT JOIN fox_bi.`主管对应关系` d ON a.主管 = d.储备主管
		AND d.年 = year("{0}") AND d.月 = month("{0}")
		LEFT JOIN fox_bi.组别信息 e ON a.组别 = e.队列
		AND e.年 = year("{0}") AND e.月 = month("{0}")
	WHERE a.组别 NOT IN ('cultivate','PKCC','THCC','MXCC','94','客维组','IVR组')
		) t
	GROUP BY 1,2,3,4,5,6),
	`日人均新案量` as(
select
`业务组2` as `业务组`,
`队列`,
-- mission_group_name as asset_group_name,
d.`正式主管中文名` as `管理层`,
`组长`,

-- `账龄`,
-- `进入账龄第一天`,
count(distinct `资产ID`) as `月累计资产数`,
round(count(distinct `资产ID`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
sum(`分案本金`) as `分案本金`,
round(sum(`分案本金`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`

from
(
select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`, sum(分案本金) 分案本金
from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
           -- ml.mission_log_id `分案id`,
            ml.mission_group_name,
            zb.`业务组` as `业务组2`,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
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
            if(mlur.mission_log_unassign_reason in( "分案不均","UNEVEN_WITHDRAW_CASE","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
         AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" )
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

	left join
		(select user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1) as group_name
		 from fox_dw.dwd_fox_user_organization_df -- 印尼
		-- fox_tmp.user_organization_df-- 除印尼和中国外的其他国家
		group by user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1)

		)uo
		on (ml.assigned_sys_user_id=uo.user_id and substr(ml.mission_log_create_at,1,10)=substr(uo.pt_date,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
    -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
                )aa
 WHERE `是否分案不均` = 0
group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`

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
 -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
and `账龄` is not null
and d.`正式主管中文名` is not null

group by `业务组2`,
-- mission_group_name,
-- `账龄`,
-- `进入账龄第一天`,
d.`正式主管中文名`,`组长`,`队列`
order by `业务组2`
)




    SELECT  date("{1}") as 统计日期,
        y.业务组,
        y.组别 as "队列",
		y.正式主管中文名 as 管理层,
		(SELECT `no` from fox_ods.ods_fox_sys_user su WHERE a.组长 = su.`name`) `leader_user_no`,
		a.组长,
		排班天数,出勤天数,
        round(出勤天数/排班天数,4)出勤率,

		c.`日人均新案分案量` as 日人均新案量,
		-- 本日在手案件,本日在手案件金额,本日在线在手案件,本日在线在手案件金额,
		round(电话拨打覆盖债务人量/出勤天数,0) 日人均拨打案件数,
        round(电话拨打次数/出勤天数,0) 日人均拨打次数,
        round(通话时长/出勤天数,0) 日人均通话时长,
        round(接通次数/出勤天数,0) 日人均接通次数,
        round(wa点击次数/出勤天数,0) as 日人均wa点击次数,
        round(短信覆盖债务人量/出勤天数,0) 日人均短信发送案件数,
        round(短信发送量/出勤天数,0) 日人均短信发送条数

        FROM 累计新分案及在手 a
        left JOIN 排班及出勤 z ON a.组长 = z.parent_user_name and a.主管 = z.manager_user_name and a.组别= z.组别
				left join 过程结果数据 y on y.组长 = a.组长 and a.主管 = y.主管 and y.组别 = a.组别
				left join `日人均新案量` c
		on y.`业务组`=c.`业务组` and y.`组别`=c.`队列` and
				y.`正式主管中文名`=c.`管理层` and
				a.`组长`=c.`组长`
				WHERE 正式主管中文名 is not null AND 排班天数 is not NULL


        """.format(third_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 组员过程指标1(third_day, yesterday):
    sql = """
    WITH `分案还款及wa数据` AS (
    SELECT
        assign.debtor_id AS '债务人ID',
        assign.mission_group_name AS '组别',
        assign.assigned_sys_user_id AS '催员ID',
        assign.mission_log_assigned_user_name AS '催员name',
        assign.group_leader_name as '组长',
        assign.director_name '主管',
    -- 	sum( wa.WA回复数 ) WA回复数,
    -- 	sum( wa.WA发送数 ) WA发送数,
        IF(SUM(r.实收) > 0,1,0) `是否还款`
    FROM
        ods_fox_mission_log assign
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
    -- 			WA跟进记录
    -- 	LEFT JOIN (
    -- 	SELECT
    -- 		chat.mission_log_id,
    -- 		count(IF(( action_type = 1 ), chat.mission_log_id, NULL )) 'WA发送数',
    -- 		count(IF(( action_type = 2 ), chat.mission_log_id, NULL )) 'WA回复数'
    -- 	FROM
    -- 		fox_dw.dwd_fox_whatsapp_chat_record chat
    -- 	WHERE
    -- 		chat.mission_log_id <> 0
    -- 		AND chat.chat_type = 1
    -- 		AND chat.send_time >= "{0}"
    -- 		AND chat.send_time < "{1}"
    -- 	GROUP BY
    -- 		1
    -- 	) wa ON wa.mission_log_id = assign.mission_log_id
        LEFT JOIN (
        SELECT
            cr.mission_log_id,
            SUM(cr.repaid_total_amount)/100 `实收`
        FROM
            ods_fox_collect_recovery cr
        WHERE
            cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
        AND unassign.mission_log_operator = 'unassign'
        AND unassign.mission_log_create_at <= "{1}"
        WHERE 1 = 1
        AND assign.mission_log_operator = 'assign'
        -- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
        AND assign.director_name IS NOT NULL
        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
        AND NOT ( mlur.mission_log_unassign_reason IN ( '分案不均', 'UNEVEN_WITHDRAW_CASE' ) AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
    GROUP BY 1,2,3,4,5,6),

    拨打数据 AS (
    SELECT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        sum(call_time) '接通时长',
        sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
        sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
        and ods_audit_call_history.pt_date >= "{0}"
        and ods_audit_call_history.pt_date <=  "{1}"
        and ods_audit_call_history_extend.pt_date >= "{0}"
        and ods_audit_call_history_extend.pt_date <= "{1}"
    GROUP BY
        催员ID,债务人ID),

    拨打首通接通 AS(
    SELECT DISTINCT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
        and ods_audit_call_history.pt_date >= "{0}"
        and ods_audit_call_history.pt_date <=  "{1}"
        and ods_audit_call_history_extend.pt_date >= "{0}"
        and ods_audit_call_history_extend.pt_date <= "{1}"),

    短信数据 AS (
    SELECT
        ods_audit_sms_history_extend.dunner_id AS '催员ID',
        ods_audit_sms_history_extend.debtor_id AS '债务人ID',
        count(*) as '短信发送次数'
    FROM
        ods_audit_sms_history
        JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
    WHERE
        ods_audit_sms_history.sms_at >= "{0}"
        AND ods_audit_sms_history.sms_at <= "{1}"

        and sms_channel =1
    GROUP BY
        催员ID,债务人ID),

    排班 as(
    SELECT SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) 组别,dtl.user_id,uo.manager_user_name,uo.parent_user_name,sum(schedule_status) 排班天数,sum(attendance_status) 上线天数
    from ods_fox_collect_attendance_dtl dtl
    LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = dtl.work_day AND uo.user_id = dtl.user_id
    WHERE dtl.work_day >= "{0}"
    AND dtl.work_day <= "{1}"
    GROUP BY dtl.user_id,uo.manager_user_name,uo.parent_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',-1)
    ),
    a1 as (SELECT uo.pt_date,uo.user_id,mx.* from(
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
        AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <=  "{1}" )
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
    right JOIN (select pt_date,user_id from fox_dw.dwd_fox_user_organization_df WHERE pt_date >= "{0}" AND pt_date <= "{1}") uo on uo.user_id = mx.催员ID AND uo.pt_date = date(mx.分案日期)),

    `a2` AS (select manager_user_name 主管,
    asset_group_name 组别,
    parent_user_name 组长,
    user_id,
    sum(分案数) 新分案,
                    sum(`分案本金(总)`) 新案本金,
            round(avg(if(e.日期 = date("{1}"),日在手案件,null)),2) 本日在手案件,
            round(avg(if(e.日期 = date("{1}"),日在手金额,null)),2) 本日在手案件金额,
            round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手案件,null)),2) 本日在线在手案件,
            round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手金额,null)),2) 本日在线在手案件金额
            from (
    SELECT d.*,if((累计分案-累计撤案) is null,累计分案,累计分案-累计撤案) 日在手案件,
    if((累计分案本金-累计撤案本金) is null,累计分案本金,累计分案本金-累计撤案本金) 日在手金额,
    uo.manager_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
    uo.parent_user_name,uo.user_id,dtl.attendance_status

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
            LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = d.日期 AND uo.user_id = d.ID
            left JOIN ods_fox_collect_attendance_dtl dtl ON dtl.work_day = d.日期 and dtl.user_id = d.ID
            )e
            where
             attendance_status is not null
             group by manager_user_name,
    asset_group_name,
    parent_user_name,
    user_id),

    base as(
    select
`业务组2` as `业务组`,
`队列`,
-- mission_group_name as asset_group_name,
d.`正式主管中文名` as `管理层`,
`组长`,
`催员姓名`,
-- `账龄`,
-- `进入账龄第一天`,
count(distinct `资产ID`) as `月累计资产数`,
round(count(distinct `资产ID`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
sum(`分案本金`) as `分案本金`,
round(sum(`分案本金`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`

from
(
select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`催员姓名`,`资产ID`,`主管`,`组长`,`队列`, sum(分案本金) 分案本金
from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
           -- ml.mission_log_id `分案id`,
            ml.mission_group_name,
            zb.`业务组` as `业务组2`,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            ml.assigned_sys_user_id `催员ID`,
			ml.mission_log_assigned_user_name `催员姓名`,
            date(ml.mission_log_create_at) `分案日期`,
            date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,
            ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
            IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in( "分案不均","UNEVEN_WITHDRAW_CASE","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
         AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <="{1}" )
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
        repay_date >= "{0}"
        AND repay_date <="{1}"
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

	left join
		(select user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1) as group_name
		 from fox_dw.dwd_fox_user_organization_df
	    -- fox_tmp.dwd_fox_user_organization_df -- 印尼
		-- fox_bi.user_organization_df-- 除印尼和中国外的其他国家

		group by user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1)

		)uo
		on (ml.assigned_sys_user_id=uo.user_id and substr(ml.mission_log_create_at,1,10)=substr(uo.pt_date,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <="{1}")
                )aa
 WHERE `是否分案不均` = 0
group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`,`催员姓名`

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
 -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
and `账龄` is not null
and d.`正式主管中文名` is not null

group by `业务组2`,
-- mission_group_name,
-- `账龄`,
-- `进入账龄第一天`,
d.`正式主管中文名`,`组长`,`队列`,`催员姓名`
order by `业务组2`
    )


    SELECT date("{1}") as 统计日期,
    ca1.业务组,
    ca1.组别 as 队列,
    ca1.`正式主管中文名`  as '管理层',
    ca1.组长,
    (SELECT min(`no`) from fox_ods.ods_fox_sys_user su WHERE ca1.`催员name` = su.`name`) `user_no`,
    ca1.`催员name`,
    c.user_type as `在职or离职`,
    上线天数,排班天数,
        round(上线天数/排班天数,4) as `上线率`,
        -- round(分案数/上线天数,0) as `日均新案量`,
        -- round(`分案本金(总)`/上线天数) as `日均新案金额`,
        ba.`日人均新案分案量` as `日均新案量`,
        ba.`日人均新案分案金额` as `日均新案金额`,
        -- 本日在手案件,
        -- 本日在手案件金额,
        -- 本日在线在手案件,
        -- 本日在线在手案件金额,
        round(电话拨打覆盖债务人量/上线天数,0) as `日均拨打案件数`,
        round(电话拨打次数/上线天数,0) as `日均拨打次数`,
        round(通时/上线天数,0) as `日均通话时长`,
        round(电话接通次数/上线天数,0) as `日均接通次数`,
    -- 	日均WA发送案件数,
    -- 	日均WA发送次数,
        round(短信覆盖债务人量/上线天数,0) as `日均短信发送案件数`,
        round(短信发送量 / 上线天数,0) as `日均短信发送条数`

    from (SELECT
        t.组别,
        t.业务组,
        t.主管,
        t.正式主管中文名,
        t.组长,
        t.`催员name`,
        t.催员ID,
    -- 	round(sum(上线天数)/sum(排班天数),4) as `上线率`,
    -- 	sum(分案数)/sum(上线天数) as `日均新案量`,
    -- 	round(sum(`分案本金(总)`)/sum(上线天数)) as `日均新案金额`,
    -- 	sum(在手案件),
    -- 	sum(在手金额),
    -- 	ROUND(COUNT(IF(t.WA发送数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(COUNT(IF(t.wa点击次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(SUM(t.WA发送数) / COUNT(t.债务人ID),4) `案均WA触达次数`,
    -- 	ROUND(SUM(t.WA回复数) / COUNT(t.债务人ID),4) `案均WA接通次数`,
        ROUND(COUNT(IF(t.短信发送次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `短信覆盖率`,
        ROUND(SUM(t.短信发送次数) / COUNT(t.债务人ID),4) `案均短信发送次数`,
    -- 	ROUND('',4) `案均短信回执次数`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`,
        COUNT(t.债务人ID) `总债务人量`,
    -- 	COUNT(IF(t.WA发送数 > 0,1,NULL)) `wa覆盖债务人量`,
    -- 	SUM(t.WA发送数) `wa发送量`,
    -- 	SUM(t.WA回复数) `wa回复次数`,
        COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
        SUM(t.短信发送次数) `短信发送量`,
        COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
        SUM(t.拨打次数) `电话拨打次数`,
        SUM(t.接通次数) `电话接通次数`,
        sum(t.接通时长) 通时,
        COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
        COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
        COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`
    from(
    SELECT
        e.业务组,
        IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
        a.组别,
        a.主管,
        a.组长,
        d.正式主管中文名,
        a.催员ID,
        a.`催员name`,
        a.债务人ID,
    -- 	a.WA发送数,
    -- 	a.WA回复数,
    -- 	mw.wa点击次数,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b.接通时长,
        b1.是否首通接通,
        c.短信发送次数
     FROM
        `分案还款及wa数据` a
        LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
        AND a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
        AND a.`债务人ID` = b1.`债务人ID`
        LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
        AND a.`债务人ID` = c.`债务人ID`
        LEFT JOIN fox_tmp.`主管对应关系` d ON a.主管 = d.储备主管
        AND d.年 = year("{0}") AND d.月 = month("{0}")
        LEFT JOIN fox_tmp.组别信息 e ON a.组别 = e.队列
        AND e.年 = year("{0}") AND e.月 = month("{0}")
        WHERE a.组别 NOT IN ('cultivate','PKCC','THCC','MXCC','94','客维组','IVR组')
        and 正式主管中文名 is not NULL) t
        group by 1,2,3,4,5,6,7) ca1
        LEFT JOIN 排班 on 排班.user_id = ca1.`催员ID` and 排班.manager_user_name = ca1.主管 and 排班.parent_user_name = ca1.组长 and ca1.`组别`=排班.`组别`
        LEFT join (SELECT 	user_id,主管,组长,组别,sum(本日在手案件) 本日在手案件,sum(本日在手案件金额) 本日在手案件金额 ,sum(本日在线在手案件) 本日在线在手案件,sum(本日在线在手案件金额) 本日在线在手案件金额 ,sum(新分案) 分案数,sum(新案本金) as `分案本金(总)`
        from a2 GROUP BY user_id,主管,组长,组别) 在手 ON 在手.user_id = ca1.`催员ID` and 在手.主管 = ca1.主管 and ca1.组长 = 在手.组长 AND 在手.组别 = ca1.组别
         left join base ba
        on ca1.`业务组`=ba.`业务组` and
        ca1.`组别`=ba.`队列` and
        ca1.`正式主管中文名`=ba.`管理层` and
        ca1.`组长`=ba.`组长` and
        ca1.`催员name`=ba.`催员姓名`
        left join 
				(select id,
				case when del_date is not null and 
				          datediff("{1}",del_date)>0 then '离职' else '在职' end as user_type
				from ods_fox_sys_user 
				group by id,
				case when del_date is not null and 
				          datediff("{1}",del_date)>0 then '离职' else '在职' end)c
				on (ca1.`催员ID`=c.id)
        WHERE 排班天数 > 0
        """.format(third_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 组员过程指标中国(third_day, yesterday):
    sql = """
    WITH `分案还款及wa数据` AS (
    SELECT
        assign.debtor_id AS '债务人ID',
        assign.mission_group_name AS '组别',
        assign.assigned_sys_user_id AS '催员ID',
        assign.mission_log_assigned_user_name AS '催员name',
        assign.group_leader_name as '组长',
        assign.director_name '主管',
    -- 	sum( wa.WA回复数 ) WA回复数,
    -- 	sum( wa.WA发送数 ) WA发送数,
        IF(SUM(r.实收) > 0,1,0) `是否还款`
    FROM
        ods_fox_mission_log assign
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
    -- 			WA跟进记录
    -- 	LEFT JOIN (
    -- 	SELECT
    -- 		chat.mission_log_id,
    -- 		count(IF(( action_type = 1 ), chat.mission_log_id, NULL )) 'WA发送数',
    -- 		count(IF(( action_type = 2 ), chat.mission_log_id, NULL )) 'WA回复数'
    -- 	FROM
    -- 		fox_dw.dwd_fox_whatsapp_chat_record chat
    -- 	WHERE
    -- 		chat.mission_log_id <> 0
    -- 		AND chat.chat_type = 1
    -- 		AND chat.send_time >= "{0}"
    -- 		AND chat.send_time < "{1}"
    -- 	GROUP BY
    -- 		1
    -- 	) wa ON wa.mission_log_id = assign.mission_log_id
        LEFT JOIN (
        SELECT
            cr.mission_log_id,
            SUM(cr.repaid_total_amount)/100 `实收`
        FROM
            ods_fox_collect_recovery cr
        WHERE
            cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
        AND unassign.mission_log_operator = 'unassign'
        AND unassign.mission_log_create_at <= "{1}"
        WHERE 1 = 1
        AND assign.mission_log_operator = 'assign'
        -- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
        AND assign.director_name IS NOT NULL
        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
        AND NOT ( mlur.mission_log_unassign_reason IN ( '分案不均', 'UNEVEN_WITHDRAW_CASE' ) AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
    GROUP BY 1,2,3,4,5,6),

    拨打数据 AS (
    SELECT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        sum(call_time) '接通时长',
        sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
        sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
        and ods_audit_call_history.pt_date >= "{0}"
        and ods_audit_call_history.pt_date <=  "{1}"
        and ods_audit_call_history_extend.pt_date >= "{0}"
        and ods_audit_call_history_extend.pt_date <= "{1}"
    GROUP BY
        催员ID,债务人ID),

    拨打首通接通 AS(
    SELECT DISTINCT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
        and ods_audit_call_history.pt_date >= "{0}"
        and ods_audit_call_history.pt_date <=  "{1}"
        and ods_audit_call_history_extend.pt_date >= "{0}"
        and ods_audit_call_history_extend.pt_date <= "{1}"),

    短信数据 AS (
    SELECT
        ods_audit_sms_history_extend.dunner_id AS '催员ID',
        ods_audit_sms_history_extend.debtor_id AS '债务人ID',
        count(*) as '短信发送次数'
    FROM
        ods_audit_sms_history
        JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
    WHERE
        ods_audit_sms_history.sms_at >= "{0}"
        AND ods_audit_sms_history.sms_at <= "{1}"

        and sms_channel =1
    GROUP BY
        催员ID,债务人ID),

    排班 as(
    SELECT SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) 组别,dtl.user_id,uo.manager_user_name,uo.parent_user_name,sum(schedule_status) 排班天数,sum(attendance_status) 上线天数
    from ods_fox_collect_attendance_dtl dtl
    LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = dtl.work_day AND uo.user_id = dtl.user_id
    WHERE dtl.work_day >= "{0}"
    AND dtl.work_day <= "{1}"
    GROUP BY dtl.user_id,uo.manager_user_name,uo.parent_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',-1)
    ),
    a1 as (SELECT uo.pt_date,uo.user_id,mx.* from(
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
        AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <=  "{1}" )
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
    right JOIN (select pt_date,user_id from fox_dw.dwd_fox_user_organization_df WHERE pt_date >= "{0}" AND pt_date <= "{1}") uo on uo.user_id = mx.催员ID AND uo.pt_date = date(mx.分案日期)),

    `a2` AS (select manager_user_name 主管,
    asset_group_name 组别,
    parent_user_name 组长,
    user_id,
    sum(分案数) 新分案,
                    sum(`分案本金(总)`) 新案本金,
            round(avg(if(e.日期 = date("{1}"),日在手案件,null)),2) 本日在手案件,
            round(avg(if(e.日期 = date("{1}"),日在手金额,null)),2) 本日在手案件金额,
            round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手案件,null)),2) 本日在线在手案件,
            round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手金额,null)),2) 本日在线在手案件金额
            from (
    SELECT d.*,if((累计分案-累计撤案) is null,累计分案,累计分案-累计撤案) 日在手案件,
    if((累计分案本金-累计撤案本金) is null,累计分案本金,累计分案本金-累计撤案本金) 日在手金额,
    uo.manager_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
    uo.parent_user_name,uo.user_id,dtl.attendance_status

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
            LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = d.日期 AND uo.user_id = d.ID
            left JOIN ods_fox_collect_attendance_dtl dtl ON dtl.work_day = d.日期 and dtl.user_id = d.ID
            )e
            where
             attendance_status is not null
             group by manager_user_name,
    asset_group_name,
    parent_user_name,
    user_id),

    base as (
    select
`业务组2` as `业务组`,
`队列`,
-- mission_group_name as asset_group_name,
d.`正式主管中文名` as `管理层`,
`组长`,
`催员姓名`,
-- `账龄`,
-- `进入账龄第一天`,
count(distinct `债务人ID`) as `月累计资产数`,
round(count(distinct `债务人ID`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
sum(`分案本金`) as `分案本金`,
round(sum(`分案本金`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`

from
(
select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`催员姓名`,`资产ID`,`主管`,`组长`,`队列`, `债务人ID`,sum(分案本金) 分案本金
from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
           -- ml.mission_log_id `分案id`,
            ml.mission_group_name,
            zb.`业务组` as `业务组2`,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            ml.assigned_sys_user_id `催员ID`,
			ml.mission_log_assigned_user_name `催员姓名`,
            date(ml.mission_log_create_at) `分案日期`,
            date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,
                                                ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
            IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in ("分案不均","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均,
			ml.debtor_id `债务人ID`
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
         AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" )
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

	left join
		(select user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1) as group_name
		 from fox_dw.dwd_fox_user_organization_df
		group by user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1)

		)uo
		on (ml.assigned_sys_user_id=uo.user_id and substr(ml.mission_log_create_at,1,10)=substr(uo.pt_date,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
    -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
                )aa
 WHERE `是否分案不均` = 0
group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`,`催员姓名`,`债务人ID`

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
 -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
and `账龄` is not null
and d.`正式主管中文名` is not null

group by `业务组2`,
-- mission_group_name,
-- `账龄`,
-- `进入账龄第一天`,
d.`正式主管中文名`,`组长`,`队列`,`催员姓名`
order by `业务组2`
    )



    SELECT date("{1}") as 统计日期,
    ca1.业务组,
    ca1.组别 as 队列,
    ca1.`正式主管中文名`  as '管理层',
    ca1.组长,
    (SELECT min(`no`) from fox_ods.ods_fox_sys_user su WHERE ca1.`催员name` = su.`name`) `user_no`,
    ca1.`催员name`,
    c.user_type as `在职or离职`,
    上线天数,排班天数,
        round(上线天数/排班天数,4) as `上线率`,
        -- round(分案数/上线天数,0) as `日均新案量`,
        -- round(`分案本金(总)`/上线天数) as `日均新案金额`,
        ba.`日人均新案分案量` as `日均新案量`,
        ba.`日人均新案分案金额` as `日均新案金额`,
        -- 本日在手案件,
        -- 本日在手案件金额,
        -- 本日在线在手案件,
        -- 本日在线在手案件金额,
        round(电话拨打覆盖债务人量/上线天数,0) as `日均拨打案件数`,
        round(电话拨打次数/上线天数,0) as `日均拨打次数`,
        round(通时/上线天数,0) as `日均通话时长`,
        round(电话接通次数/上线天数,0) as `日均接通次数`,
    -- 	日均WA发送案件数,
    -- 	日均WA发送次数,
        round(短信覆盖债务人量/上线天数,0) as `日均短信发送案件数`,
        round(短信发送量 / 上线天数,0) as `日均短信发送条数`

    from (SELECT
        t.组别,
        t.业务组,
        t.主管,
        t.正式主管中文名,
        t.组长,
        t.`催员name`,
        t.催员ID,
    -- 	round(sum(上线天数)/sum(排班天数),4) as `上线率`,
    -- 	sum(分案数)/sum(上线天数) as `日均新案量`,
    -- 	round(sum(`分案本金(总)`)/sum(上线天数)) as `日均新案金额`,
    -- 	sum(在手案件),
    -- 	sum(在手金额),
    -- 	ROUND(COUNT(IF(t.WA发送数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(COUNT(IF(t.wa点击次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(SUM(t.WA发送数) / COUNT(t.债务人ID),4) `案均WA触达次数`,
    -- 	ROUND(SUM(t.WA回复数) / COUNT(t.债务人ID),4) `案均WA接通次数`,
        ROUND(COUNT(IF(t.短信发送次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `短信覆盖率`,
        ROUND(SUM(t.短信发送次数) / COUNT(t.债务人ID),4) `案均短信发送次数`,
    -- 	ROUND('',4) `案均短信回执次数`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`,
        COUNT(t.债务人ID) `总债务人量`,
    -- 	COUNT(IF(t.WA发送数 > 0,1,NULL)) `wa覆盖债务人量`,
    -- 	SUM(t.WA发送数) `wa发送量`,
    -- 	SUM(t.WA回复数) `wa回复次数`,
        COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
        SUM(t.短信发送次数) `短信发送量`,
        COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
        SUM(t.拨打次数) `电话拨打次数`,
        SUM(t.接通次数) `电话接通次数`,
        sum(t.接通时长) 通时,
        COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
        COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
        COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`
    from(
    SELECT
        e.业务组,
        IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
        a.组别,
        a.主管,
        a.组长,
        d.正式主管中文名,
        a.催员ID,
        a.`催员name`,
        a.债务人ID,
    -- 	a.WA发送数,
    -- 	a.WA回复数,
    -- 	mw.wa点击次数,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b.接通时长,
        b1.是否首通接通,
        c.短信发送次数
     FROM
        `分案还款及wa数据` a
        LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
        AND a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
        AND a.`债务人ID` = b1.`债务人ID`
        LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
        AND a.`债务人ID` = c.`债务人ID`
        LEFT JOIN fox_tmp.`主管对应关系` d ON a.主管 = d.储备主管
        AND d.年 = year("{0}") AND d.月 = month("{0}")
        LEFT JOIN fox_tmp.组别信息 e ON a.组别 = e.队列
        AND e.年 = year("{0}") AND e.月 = month("{0}")
        WHERE a.组别 NOT IN ('cultivate','PKCC','THCC','MXCC','94','客维组','IVR组')
        and 正式主管中文名 is not NULL) t
        group by 1,2,3,4,5,6,7) ca1
        LEFT JOIN 排班 on 排班.user_id = ca1.`催员ID` and 排班.manager_user_name = ca1.主管 and 排班.parent_user_name = ca1.组长 and ca1.`组别`=排班.`组别`
        LEFT join (SELECT 	user_id,主管,组长,组别,sum(本日在手案件) 本日在手案件,sum(本日在手案件金额) 本日在手案件金额 ,sum(本日在线在手案件) 本日在线在手案件,sum(本日在线在手案件金额) 本日在线在手案件金额 ,sum(新分案) 分案数,sum(新案本金) as `分案本金(总)`
        from a2 GROUP BY user_id,主管,组长,组别) 在手 ON 在手.user_id = ca1.`催员ID` and 在手.主管 = ca1.主管 and ca1.组长 = 在手.组长 AND 在手.组别 = ca1.组别
        left join base ba
        on ca1.`业务组`=ba.`业务组` and
        ca1.`组别`=ba.`队列` and
        ca1.`正式主管中文名`=ba.`管理层` and
        ca1.`组长`=ba.`组长` and
        ca1.`催员name`=ba.`催员姓名`
        left join 
				(select id,
				case when del_date is not null and 
				          datediff("{1}",del_date)>0 then '离职' else '在职' end as user_type
				from ods_fox_sys_user 
				group by id,
				case when del_date is not null and 
				          datediff("{1}",del_date)>0 then '离职' else '在职' end)c
				on (ca1.`催员ID`=c.id)
        WHERE 排班天数 > 0
        """.format(third_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 组员过程指标2(third_day, yesterday):
    sql = """
WITH `分案还款及wa数据` AS (
SELECT
	assign.debtor_id AS '债务人ID',
	assign.mission_group_name AS '组别',
	assign.assigned_sys_user_id AS '催员ID',
	assign.mission_log_assigned_user_name AS '催员name',
	assign.group_leader_name as '组长',
	assign.director_name '主管',
-- 	sum( wa.WA回复数 ) WA回复数,
-- 	sum( wa.WA发送数 ) WA发送数,
	IF(SUM(r.实收) > 0,1,0) `是否还款`
FROM
	ods_fox_mission_log assign
	LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
-- 			WA跟进记录
-- 	LEFT JOIN (
-- 	SELECT
-- 		chat.mission_log_id,
-- 		count(IF(( action_type = 1 ), chat.mission_log_id, NULL )) 'WA发送数',
-- 		count(IF(( action_type = 2 ), chat.mission_log_id, NULL )) 'WA回复数'
-- 	FROM
-- 		fox_dw.dwd_fox_whatsapp_chat_record chat
-- 	WHERE
-- 		chat.mission_log_id <> 0
-- 		AND chat.chat_type = 1
-- 		AND chat.send_time >= "{0}"
-- 		AND chat.send_time < "{1}"
-- 	GROUP BY
-- 		1
-- 	) wa ON wa.mission_log_id = assign.mission_log_id
	LEFT JOIN (
	SELECT
		cr.mission_log_id,
		SUM(cr.repaid_total_amount)/100 `实收`
	FROM
		ods_fox_collect_recovery cr
	WHERE
		cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
	GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
	LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
	AND unassign.mission_log_operator = 'unassign'
	AND unassign.mission_log_create_at <= "{1}"
	WHERE 1 = 1
	AND assign.mission_log_operator = 'assign'
	-- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
	AND assign.director_name IS NOT NULL
	AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
	AND NOT ( mlur.mission_log_unassign_reason IN ( '分案不均', 'UNEVEN_WITHDRAW_CASE' ) AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
GROUP BY 1,2,3,4,5,6),

拨打数据 AS (
SELECT
	ods_audit_call_history.dunner_id AS '催员ID',
	debtor_id AS '债务人ID',
	sum(call_time) '接通时长',
	sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
	sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数'
FROM
	ods_audit_call_history
	JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
WHERE
	ods_audit_call_history.call_at >= "{0}"
	AND ods_audit_call_history.call_at <= "{1}"
	and ods_audit_call_history.pt_date >= "{0}"
	and ods_audit_call_history.pt_date <=  "{1}"
	and ods_audit_call_history_extend.pt_date >= "{0}"
	and ods_audit_call_history_extend.pt_date <= "{1}"
GROUP BY
	催员ID,债务人ID),

拨打首通接通 AS(
SELECT DISTINCT
	ods_audit_call_history.dunner_id AS '催员ID',
	debtor_id AS '债务人ID',
	IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
FROM
	ods_audit_call_history
	JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
WHERE
	ods_audit_call_history.call_at >= "{0}"
	AND ods_audit_call_history.call_at <= "{1}"
	and ods_audit_call_history.pt_date >= "{0}"
	and ods_audit_call_history.pt_date <=  "{1}"
	and ods_audit_call_history_extend.pt_date >= "{0}"
	and ods_audit_call_history_extend.pt_date <= "{1}"),

短信数据 AS (
SELECT
	ods_audit_sms_history_extend.dunner_id AS '催员ID',
	ods_audit_sms_history_extend.debtor_id AS '债务人ID',
	count(*) as '短信发送次数'
FROM
	ods_audit_sms_history
	JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
WHERE
	ods_audit_sms_history.sms_at >= "{0}"
	AND ods_audit_sms_history.sms_at <= "{1}"

	and sms_channel =1
GROUP BY
	催员ID,债务人ID),

墨西哥wa AS (
		SELECT
			t1.dunner_id `催员ID`,
			t1.debtor_id `债务人ID`,
			count( t1.debtor_id ) AS `wa点击次数`,
			count( DISTINCT t1.debtor_id ) AS `wa点击个数`
		FROM
			(
			SELECT
				t.create_user_id AS `dunner_id`,
				get_json_object ( t.content, '$.debtorId' ) `debtor_id`,
				t.create_at
			FROM
				`ods_fox_oper_business_log` t
			WHERE
				date( t.create_at ) >= "{0}" AND date ( t.create_at ) <= "{1}"
				AND t.title = 'WHATSAPP_LINK') t1
		GROUP BY t1.dunner_id,t1.debtor_id),
排班 as(
    SELECT SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) 组别,dtl.user_id,uo.manager_user_name,uo.parent_user_name,sum(schedule_status) 排班天数,sum(attendance_status) 上线天数
    from ods_fox_collect_attendance_dtl dtl
    LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = dtl.work_day AND uo.user_id = dtl.user_id
    WHERE dtl.work_day >= "{0}"
    AND dtl.work_day <= "{1}"
    GROUP BY dtl.user_id,uo.manager_user_name,uo.parent_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',-1)
    ),
a1 as (SELECT uo.pt_date,uo.user_id,mx.* from(
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
        AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <=  "{1}" )
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
    right JOIN (select pt_date,user_id from fox_dw.dwd_fox_user_organization_df WHERE pt_date >= "{0}" AND pt_date <= "{1}") uo on uo.user_id = mx.催员ID AND uo.pt_date = date(mx.分案日期)),

    `a2` AS (select manager_user_name 主管,
    asset_group_name 组别,
    parent_user_name 组长,
    user_id,
    sum(分案数) 新分案,
                    sum(`分案本金(总)`) 新案本金,
            round(avg(if(e.日期 = date("{1}"),日在手案件,null)),2) 本日在手案件,
            round(avg(if(e.日期 = date("{1}"),日在手金额,null)),2) 本日在手案件金额,
            round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手案件,null)),2) 本日在线在手案件,
            round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手金额,null)),2) 本日在线在手案件金额
            from (
    SELECT d.*,if((累计分案-累计撤案) is null,累计分案,累计分案-累计撤案) 日在手案件,
    if((累计分案本金-累计撤案本金) is null,累计分案本金,累计分案本金-累计撤案本金) 日在手金额,
    uo.manager_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
    uo.parent_user_name,uo.user_id,dtl.attendance_status

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
            LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = d.日期 AND uo.user_id = d.ID
            left JOIN ods_fox_collect_attendance_dtl dtl ON dtl.work_day = d.日期 and dtl.user_id = d.ID
            )e
            where
             attendance_status is not null
             group by manager_user_name,
    asset_group_name,
    parent_user_name,
    user_id),

    base as(
    select
`业务组2` as `业务组`,
`队列`,
-- mission_group_name as asset_group_name,
d.`正式主管中文名` as `管理层`,
`组长`,
`催员姓名`,
-- `账龄`,
-- `进入账龄第一天`,
count(distinct `资产ID`) as `月累计资产数`,
round(count(distinct `资产ID`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
sum(`分案本金`) as `分案本金`,
round(sum(`分案本金`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`

from
(
select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`催员姓名`,`资产ID`,`主管`,`组长`,`队列`, sum(分案本金) 分案本金
from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
           -- ml.mission_log_id `分案id`,
            ml.mission_group_name,
            zb.`业务组` as `业务组2`,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            ml.assigned_sys_user_id `催员ID`,
			ml.mission_log_assigned_user_name `催员姓名`,
            date(ml.mission_log_create_at) `分案日期`,
            date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,
            ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
            IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in( "分案不均","UNEVEN_WITHDRAW_CASE","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
         AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <="{1}" )
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
        repay_date >= "{0}"
        AND repay_date <="{1}"
        and repaid_total_amount >0
        AND batch_num IS NOT NULL
    GROUP BY
        mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
                left join
                (select `年`,`月`,`队列`,`业务组`,`账龄`,
                case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
                from fox_bi.`组别信息`
                where `年`=year("{0}") and `月`=month("{0}")
                ) zb
                on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列` and
                zb.`进入账龄第一天`=ml.assign_asset_late_days and zb.`进入账龄第一天`=ml.assign_debtor_late_days)

	left join
		(select user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1) as group_name
		 from fox_dw.dwd_fox_user_organization_df
	    -- fox_tmp.dwd_fox_user_organization_df -- 印尼
		-- fox_bi.user_organization_df-- 除印尼和中国外的其他国家

		group by user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1)

		)uo
		on (ml.assigned_sys_user_id=uo.user_id and substr(ml.mission_log_create_at,1,10)=substr(uo.pt_date,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <="{1}")
    -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
                )aa
 WHERE `是否分案不均` = 0
group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`,`催员姓名`

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
fox_bi.`主管对应关系` d

on (year(a.`分案日期`)=d.`年` and month(a.`分案日期`)=d.`月` and `主管`=`储备主管`)
where 1 = 1
 -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
and `账龄` is not null
and d.`正式主管中文名` is not null

group by `业务组2`,
-- mission_group_name,
-- `账龄`,
-- `进入账龄第一天`,
d.`正式主管中文名`,`组长`,`队列`,`催员姓名`
order by `业务组2`

    )


SELECT date("{1}") as 统计日期,ca1.业务组,ca1.组别 as 队列,ca1.正式主管中文名  as '管理层',ca1.组长,
    (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE ca1.`催员name` = su.`name`) `user_no`,
    ca1.`催员name`,
    c.user_type as `在职or离职`,
    上线天数,排班天数,
	round(上线天数/排班天数,4) as `上线率`,
	-- round(分案数/上线天数,0) as `日均新案量`,
	-- round(`分案本金(总)`/上线天数) as `日均新案金额`,
	ba.`日人均新案分案量` as `日均新案量`,
    ba.`日人均新案分案金额` as `日均新案金额`,
    -- 本日在手案件,
    --  本日在手案件金额,
    -- 本日在线在手案件,
    -- 本日在线在手案件金额,
	round(电话拨打覆盖债务人量/上线天数,0) as `日均拨打案件数`,
	round(电话拨打次数/上线天数,0) as `日均拨打次数`,
	round(通时/上线天数,0) as `日均通话时长`,
	round(电话接通次数/上线天数,0) as `日均接通次数`,
-- 	日均WA发送案件数,
 	round(wa点击次数/上线天数,0) as 日均wa点击次数,
	round(短信覆盖债务人量/上线天数,0) as `日均短信发送案件数`,
	round(短信发送量 / 上线天数,0) as `日均短信发送条数`

from (SELECT
	t.组别,
	t.业务组,
	t.主管,
	t.正式主管中文名,
	t.组长,
	t.`催员name`,
	t.催员ID,
	sum(t.wa点击次数) wa点击次数,
    count(distinct if((t.wa点击次数)>0,债务人ID,null)) wa覆盖债务人数,
-- 	round(sum(上线天数)/sum(排班天数),4) as `上线率`,
-- 	sum(分案数)/sum(上线天数) as `日均新案量`,
-- 	round(sum(`分案本金(总)`)/sum(上线天数)) as `日均新案金额`,
-- 	sum(在手案件),
-- 	sum(在手金额),
-- 	ROUND(COUNT(IF(t.WA发送数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
-- 	ROUND(COUNT(IF(t.wa点击次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
-- 	ROUND(SUM(t.WA发送数) / COUNT(t.债务人ID),4) `案均WA触达次数`,
-- 	ROUND(SUM(t.WA回复数) / COUNT(t.债务人ID),4) `案均WA接通次数`,
	ROUND(COUNT(IF(t.短信发送次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `短信覆盖率`,
	ROUND(SUM(t.短信发送次数) / COUNT(t.债务人ID),4) `案均短信发送次数`,
-- 	ROUND('',4) `案均短信回执次数`,
	ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
	ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
	ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
	ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
	ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`,
	COUNT(t.债务人ID) `总债务人量`,
-- 	COUNT(IF(t.WA发送数 > 0,1,NULL)) `wa覆盖债务人量`,
-- 	SUM(t.WA发送数) `wa发送量`,
-- 	SUM(t.WA回复数) `wa回复次数`,
	COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
	SUM(t.短信发送次数) `短信发送量`,
	COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
	SUM(t.拨打次数) `电话拨打次数`,
	SUM(t.接通次数) `电话接通次数`,
	sum(t.接通时长) 通时,
	COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
	COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
	COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`
from(
SELECT
	e.业务组,
	IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
	a.组别,
	a.主管,
	a.组长,
	d.正式主管中文名,
	a.催员ID,
	a.`催员name`,
	a.债务人ID,
-- 	a.WA发送数,
-- 	a.WA回复数,
 	mw.wa点击次数,
	a.是否还款,
	b.拨打次数,
	b.接通次数,
	b.接通时长,
	b1.是否首通接通,
	c.短信发送次数
 FROM
	`分案还款及wa数据` a
	LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
	AND a.`债务人ID` = b.`债务人ID`
	LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
	AND a.`债务人ID` = b1.`债务人ID`
	LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
	AND a.`债务人ID` = c.`债务人ID`
	LEFT JOIN 墨西哥wa mw ON a.`催员ID` = mw.`催员ID`
	AND a.`债务人ID` = mw.`债务人ID`
	LEFT JOIN fox_bi.`主管对应关系` d ON a.主管 = d.储备主管
	AND d.年 = year("{0}") AND d.月 = month("{0}")
	LEFT JOIN fox_bi.组别信息 e ON a.组别 = e.队列
	AND e.年 = year("{0}") AND e.月 = month("{0}")
	WHERE a.组别 NOT IN ('cultivate','PKCC','THCC','MXCC','94','客维组','IVR组')
	and 正式主管中文名 is not NULL) t
	group by 1,2,3,4,5,6,7) ca1
	LEFT JOIN 排班 on 排班.user_id = ca1.`催员ID` and 排班.manager_user_name = ca1.主管 and 排班.parent_user_name = ca1.组长 and ca1.`组别`=排班.`组别`
        LEFT join (SELECT 	user_id,主管,组长,组别,sum(本日在手案件) 本日在手案件,sum(本日在手案件金额) 本日在手案件金额 ,sum(本日在线在手案件) 本日在线在手案件,sum(本日在线在手案件金额) 本日在线在手案件金额 ,sum(新分案) 分案数,sum(新案本金) as `分案本金(总)`
        from a2 GROUP BY user_id,主管,组长,组别) 在手 ON 在手.user_id = ca1.`催员ID` and 在手.主管 = ca1.主管 and ca1.组长 = 在手.组长 AND 在手.组别 = ca1.组别
left join base ba
        on ca1.`业务组`=ba.`业务组` and
        ca1.`组别`=ba.`队列` and
        ca1.`正式主管中文名`=ba.`管理层` and
        ca1.`组长`=ba.`组长` and
        ca1.`催员name`=ba.`催员姓名`
        left join 
				(select id,
				case when del_date is not null and 
				          datediff("{1}",del_date)>0 then '离职' else '在职' end as user_type
				from ods_fox_sys_user 
				group by id,
				case when del_date is not null and 
				          datediff("{1}",del_date)>0 then '离职' else '在职' end)c
				on (ca1.`催员ID`=c.id)
        WHERE 排班天数 > 0
        """.format(third_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 团队过程指标3(first_day, yesterday):
    sql = """
    WITH `分案还款及wa数据` AS (
    SELECT
    assign.debtor_id AS '债务人ID',
    SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) AS '组别',
    assign.assigned_sys_user_id AS '催员ID',
    uo.manager_user_name '主管',
    -- 	sum( wa.WA回复数 ) WA回复数,
    -- 	sum( wa.WA发送数 ) WA发送数,
        IF(SUM(r.实收) > 0,1,0) `是否还款`
    FROM
        ods_fox_mission_log assign
        LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on date(assign.mission_log_create_at) = uo.pt_date AND assign.assigned_sys_user_id = uo.user_id
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
        LEFT JOIN (
        SELECT
            cr.mission_log_id,
            SUM(cr.repaid_total_amount)/100 `实收`
        FROM
            ods_fox_collect_recovery cr
        WHERE
            cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
        AND unassign.mission_log_operator = 'unassign'
        AND unassign.mission_log_create_at <= "{1}"
        WHERE 1 = 1
        AND assign.mission_log_operator = 'assign'
        -- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
        AND assign.director_name IS NOT NULL
        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
        AND NOT ( mlur.mission_log_unassign_reason IN ( '分案不均', 'UNEVEN_WITHDRAW_CASE' ) AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
    GROUP BY 1,2,3,4),

    拨打数据 AS (
    SELECT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
        sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
        and ods_audit_call_history.pt_date >= "{0}"
        and ods_audit_call_history.pt_date <= "{1}"
        and ods_audit_call_history_extend.pt_date >= "{0}"
        and ods_audit_call_history_extend.pt_date <= "{1}"
    GROUP BY
        催员ID,债务人ID),

    拨打首通接通 AS(
    SELECT DISTINCT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
        and ods_audit_call_history.pt_date >= "{0}"
        and ods_audit_call_history.pt_date <= "{1}"
        and ods_audit_call_history_extend.pt_date >= "{0}"
        and ods_audit_call_history_extend.pt_date <= "{1}"),

    短信数据 AS (
    SELECT
	ods_audit_sms_history_extend.dunner_id AS '催员ID',
	ods_audit_sms_history_extend.debtor_id AS '债务人ID',
	count(ods_audit_sms_history.id) as '短信发送次数',
	COUNT(DISTINCT ods_fox_sms_record.sms_record_ticket) '短信回执次数'
    FROM
	ods_audit_sms_history
	JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
	LEFT JOIN ods_fox_sms_record ON ods_audit_sms_history.origin_sms_record_id = ods_fox_sms_record.sms_record_id
    WHERE
	ods_audit_sms_history.sms_at >= "{0}"
	AND ods_audit_sms_history.sms_at <= "{1}"
	and sms_channel =1
    GROUP BY
	催员ID,债务人ID)
    -- 	,

    -- 墨西哥wa AS (
    -- 	SELECT
    -- 		t1.dunner_id `催员ID`,
    -- 		t1.debtor_id `债务人ID`,
    -- 		count( t1.debtor_id ) AS `wa点击次数`,
    -- 		count( DISTINCT t1.debtor_id ) AS `wa点击个数`
    -- 	FROM
    -- 		(
    -- 		SELECT
    -- 			t.create_user_id AS `dunner_id`,
    -- 			get_json_object ( t.content, '$.debtorId' ) `debtor_id`,
    -- 			t.create_at
    -- 		FROM
    -- 			`ods_fox_oper_business_log` t
    -- 		WHERE
    -- 			date( t.create_at ) >= "{0}" AND date ( t.create_at ) <= "{1}"
    -- 			AND t.title = 'WHATSAPP_LINK') t1
    -- 	GROUP BY t1.dunner_id,t1.debtor_id)

    SELECT
        date("{1}") as 统计日期,
        t.业务组,
        t.组别 as 队列,
        t.正式主管中文名 as '管理层',
    -- 	ROUND(COUNT(IF(t.WA发送数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(COUNT(IF(t.wa点击次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(SUM(t.WA发送数) / COUNT(t.债务人ID),4) `案均WA触达次数`,
    -- 	ROUND(SUM(t.WA回复数) / COUNT(t.债务人ID),4) `案均WA接通次数`,
        ROUND(COUNT(IF(t.短信发送次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `短信覆盖率`,
        ROUND(SUM(t.短信发送次数) / COUNT(t.债务人ID),4) `案均短信发送次数`,
	    ROUND(SUM(t.短信回执次数) / COUNT(t.债务人ID),4) `案均短信回执次数`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL))  `可联还款率_分子`,
        COUNT(t.债务人ID) `可联还款率_分母`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
        ,COUNT(t.债务人ID) `总债务人量`,
        -- COUNT(IF(t.WA发送数 > 0,1,NULL)) `wa覆盖债务人量`,
        -- SUM(t.WA发送数) `wa发送量`,
        -- SUM(t.WA回复数) `wa回复次数`,
        COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
        SUM(t.短信发送次数) `短信发送量`,
        COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
        SUM(t.拨打次数) `电话拨打次数`,
        COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
        COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
        COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`
    FROM
    (SELECT
        e.业务组,
        IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
        a.组别,
        a.主管,
        d.正式主管中文名,
        a.催员ID,
        a.债务人ID,
    -- 	a.WA发送数,
    -- 	a.WA回复数,
    -- 	mw.wa点击次数,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b1.是否首通接通,
        c.短信发送次数,
        c.短信回执次数
    FROM
        `分案还款及wa数据` a
        LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
        AND a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
        AND a.`债务人ID` = b1.`债务人ID`
        LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
        AND a.`债务人ID` = c.`债务人ID`
        LEFT JOIN fox_tmp.`主管对应关系` d ON a.主管 = d.储备主管
        AND d.年 = year("{0}") AND d.月 = month("{0}")
        LEFT JOIN fox_tmp.组别信息 e ON a.组别 = e.队列
        AND e.年 = year("{0}") AND e.月 = month("{0}")
    WHERE a.组别 NOT IN ('cultivate','PKCC','THCC','MXCC','94','客维组')
    and 正式主管中文名 is not null
        ) t
    GROUP BY 1,2,3,4
    """.format(first_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 组长过程指标3(third_day, yesterday):
    sql = """
    WITH a1 as (SELECT uo.pt_date,uo.user_id,mx.* from(
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
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <=  "{1}" )
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
right JOIN (select pt_date,user_id from fox_dw.dwd_fox_user_organization_df WHERE pt_date >= "{0}" AND pt_date <= "{1}") uo on uo.user_id = mx.催员ID AND uo.pt_date = date(mx.分案日期)),

`累计新分案及在手` AS (
    select manager_user_name 主管,asset_group_name 组别,parent_user_name 组长,
        sum(累计分案) 累计新分案,
		round(avg(if(e.日期 = date("{1}"),日在手案件,null)),2) 本日在手案件,
		round(avg(if(e.日期 = date("{1}"),日在手金额,null)),2) 本日在手案件金额,
		round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手案件,null)),2) 本日在线在手案件,
        round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手金额,null)),2) 本日在线在手案件金额
        from (
SELECT d.*,if((累计分案-累计撤案) is null,累计分案,累计分案-累计撤案) 日在手案件,
if((累计分案本金-累计撤案本金) is null,累计分案本金,累计分案本金-累计撤案本金) 日在手金额,
uo.manager_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
uo.parent_user_name,dtl.attendance_status

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
		LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = d.日期 AND uo.user_id = d.ID
		left JOIN ods_fox_collect_attendance_dtl dtl ON dtl.work_day = d.日期 and dtl.user_id = d.ID
		)e
		where
		 attendance_status is not null
		group by manager_user_name,asset_group_name,parent_user_name),
         `排班及出勤` AS(
SELECT SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) 组别,uo.manager_user_name,uo.parent_user_name,sum(schedule_status) 排班天数,sum(attendance_status) 出勤天数
from ods_fox_collect_attendance_dtl dtl
LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = dtl.work_day AND uo.user_id = dtl.user_id
WHERE dtl.work_day >= "{0}"
AND dtl.work_day <= "{1}"
GROUP BY uo.manager_user_name,uo.parent_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 )),
            `分案还款及wa数据` AS (
        SELECT
            assign.debtor_id AS '债务人ID',
            assign.mission_group_name AS '组别',
            assign.assigned_sys_user_id AS '催员ID',
            assign.group_leader_name AS '组长',
            assign.director_name '主管',
        -- 	sum( wa.WA回复数 ) WA回复数,
        -- 	sum( wa.WA发送数 ) WA发送数,
            IF(SUM(r.实收) > 0,1,0) `是否还款`
        FROM
            ods_fox_mission_log assign
            LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
        -- 			WA跟进记录
        -- 	LEFT JOIN (
        -- 	SELECT
        -- 		chat.mission_log_id,
        -- 		count(IF(( action_type = 1 ), chat.mission_log_id, NULL )) 'WA发送数',
        -- 		count(IF(( action_type = 2 ), chat.mission_log_id, NULL )) 'WA回复数'
        -- 	FROM
        -- 		fox_dw.dwd_fox_whatsapp_chat_record chat
        -- 	WHERE
        -- 		chat.mission_log_id <> 0
        -- 		AND chat.chat_type = 1
        -- 		AND chat.send_time >= "{0}"
        -- 		AND chat.send_time < "{1}"
        -- 	GROUP BY
        -- 		1
        -- 	) wa ON wa.mission_log_id = assign.mission_log_id
            LEFT JOIN (
            SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
            FROM
                ods_fox_collect_recovery cr
            WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
            GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
            LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
            AND unassign.mission_log_operator = 'unassign'
            AND unassign.mission_log_create_at <= "{1}"
            WHERE 1 = 1
            AND assign.mission_log_operator = 'assign'
            -- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
            AND assign.director_name IS NOT NULL
            AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
            AND NOT ( mlur.mission_log_unassign_reason IN ( '分案不均', 'UNEVEN_WITHDRAW_CASE') AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
        GROUP BY 1,2,3,4,5),

        拨打数据 AS (
        SELECT
            ods_audit_call_history.dunner_id AS '催员ID',
            debtor_id AS '债务人ID',
            sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
            sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数' ,
            sum(ods_audit_call_history.call_time) '通话时长'
        FROM
            ods_audit_call_history
            JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
        WHERE
            ods_audit_call_history.call_at >= "{0}"
            AND ods_audit_call_history.call_at <= "{1}"
            AND ods_audit_call_history_extend.pt_date>="{0}"
			AND ods_audit_call_history_extend.pt_date<="{1}"
        GROUP BY
            催员ID,债务人ID),

        拨打首通接通 AS(
        SELECT DISTINCT
            ods_audit_call_history.dunner_id AS '催员ID',
            debtor_id AS '债务人ID',
            IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
        FROM
            ods_audit_call_history
            JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
        WHERE
            ods_audit_call_history.call_at >= "{0}"
            AND ods_audit_call_history.call_at <= "{1}"),

        短信数据 AS (
        SELECT
            ods_audit_sms_history_extend.dunner_id AS '催员ID',
            ods_audit_sms_history_extend.debtor_id AS '债务人ID',
            count(*) as '短信发送次数'
        FROM
            ods_audit_sms_history
            JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
        WHERE
            ods_audit_sms_history.sms_at >= "{0}"
            AND ods_audit_sms_history.sms_at <= "{1}"
            and sms_channel =1
        GROUP BY
            催员ID,债务人ID),

     `过程结果数据` AS(
        SELECT
            t.组别,
            t.业务组,
            t.类别,
            t.主管,
            t.正式主管中文名,
            t.组长,
            COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
            SUM(t.短信发送次数) `短信发送量`,
            COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
            SUM(t.拨打次数) `电话拨打次数`,
            COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
            COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
            COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`,
            SUM(通话时长) `通话时长`,
            SUM(接通次数) `接通次数`
        FROM
        (SELECT
            e.业务组,
            IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
            a.组别,
            a.组长,
            a.主管,
            d.正式主管中文名,
            a.催员ID,
            a.债务人ID,
        -- 	a.WA发送数,
        -- 	a.WA回复数,
        -- 	mw.wa点击次数,
            a.是否还款,
            b.拨打次数,
            b.接通次数,
            b.通话时长,
            b1.是否首通接通,
            c.短信发送次数
        FROM
            `分案还款及wa数据` a
            LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
            AND a.`债务人ID` = b.`债务人ID`
            LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
            AND a.`债务人ID` = b1.`债务人ID`
            LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
            AND a.`债务人ID` = c.`债务人ID`
    -- 		LEFT JOIN 墨西哥wa mw ON a.`催员ID` = mw.`催员ID`
    -- 		AND a.`债务人ID` = mw.`债务人ID`
            LEFT JOIN fox_tmp.`主管对应关系` d ON a.主管 = d.储备主管
            AND d.年 = year("{0}") AND d.月 = month("{0}")
            LEFT JOIN fox_tmp.组别信息 e ON a.组别 = e.队列
            AND e.年 = year("{0}") AND e.月 = month("{0}")
        WHERE a.组别 NOT IN ('cultivate','PKCC','THCC','MXCC','94','客维组','IVR组')
            ) t
        GROUP BY 1,2,3,4,5,6),


        `日人均新案量` as(
select
`业务组2` as `业务组`,
`队列`,
-- mission_group_name as asset_group_name,
d.`正式主管中文名` as `管理层`,
`组长`,

-- `账龄`,
-- `进入账龄第一天`,
count(distinct `债务人ID`) as `月累计资产数`,
round(count(distinct `债务人ID`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
sum(`分案本金`) as `分案本金`,
round(sum(`分案本金`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`

from
(
select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`, `债务人ID`,sum(分案本金) 分案本金
from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
           -- ml.mission_log_id `分案id`,
            ml.mission_group_name,
            zb.`业务组` as `业务组2`,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
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
            if(mlur.mission_log_unassign_reason in( "分案不均","UNEVEN_WITHDRAW_CASE","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均,
			ml.debtor_id as `债务人ID`
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
         AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" )
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

	left join
		(select user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1) as group_name
		 from fox_dw.dwd_fox_user_organization_df -- 印尼
		-- fox_tmp.user_organization_df-- 除印尼和中国外的其他国家
		group by user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1)

		)uo
		on (ml.assigned_sys_user_id=uo.user_id and substr(ml.mission_log_create_at,1,10)=substr(uo.pt_date,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
    -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
                )aa
 WHERE `是否分案不均` = 0
group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`,债务人ID

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
 -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
and `账龄` is not null
and d.`正式主管中文名` is not null

group by `业务组2`,
-- mission_group_name,
-- `账龄`,
-- `进入账龄第一天`,
d.`正式主管中文名`,`组长`,`队列`
order by `业务组2`
)

SELECT  date("{1}") as 统计日期,
        y.业务组,
				y.组别 as "队列",
				y.正式主管中文名 as 管理层,
				(SELECT `no` from fox_ods.ods_fox_sys_user su WHERE a.组长 = su.`name`) `leader_user_no`,
				a.组长,
				排班天数,
				出勤天数,
        round(出勤天数/排班天数,4)出勤率,
		-- round(累计新分案/出勤天数,0) 日人均新案量,
		c.`日人均新案分案量` as 日人均新案量,
		-- 本日在手案件,本日在手案件金额,本日在线在手案件,本日在线在手案件金额,
		round(电话拨打覆盖债务人量/出勤天数,0) 日人均拨打案件数,
        round(电话拨打次数/出勤天数,0) 日人均拨打次数,
        round(通话时长/出勤天数,0) 日人均通话时长,
        round(接通次数/出勤天数,0) 日人均接通次数,
        null as 日人均WA发送案件数,null as 日人均WA发送次数,
        round(短信覆盖债务人量/出勤天数,0) 日人均短信发送案件数,
        round(短信发送量/出勤天数,0) 日人均短信发送条数

        FROM 累计新分案及在手 a
        left JOIN 排班及出勤 z ON a.组长 = z.parent_user_name and a.主管 = z.manager_user_name and a.组别= z.组别
				left join 过程结果数据 y on y.组长 = a.组长 and a.主管 = y.主管 and y.组别 = a.组别
		left join `日人均新案量` c
		on y.`业务组`=c.`业务组` and y.`组别`=c.`队列` and
				y.`正式主管中文名`=c.`管理层` and
				a.`组长`=c.`组长`
				WHERE 正式主管中文名 is not null AND 排班天数 is not NULL
        """.format(third_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 组员过程指标3(third_day, yesterday):
    sql = """
    WITH `分案还款及wa数据` AS (
    SELECT
        assign.debtor_id AS '债务人ID',
        assign.mission_group_name AS '组别',
        assign.assigned_sys_user_id AS '催员ID',
        assign.mission_log_assigned_user_name AS '催员name',
        assign.group_leader_name as '组长',
        assign.director_name '主管',
    -- 	sum( wa.WA回复数 ) WA回复数,
    -- 	sum( wa.WA发送数 ) WA发送数,
        IF(SUM(r.实收) > 0,1,0) `是否还款`
    FROM
        ods_fox_mission_log assign
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id
    -- 			WA跟进记录
    -- 	LEFT JOIN (
    -- 	SELECT
    -- 		chat.mission_log_id,
    -- 		count(IF(( action_type = 1 ), chat.mission_log_id, NULL )) 'WA发送数',
    -- 		count(IF(( action_type = 2 ), chat.mission_log_id, NULL )) 'WA回复数'
    -- 	FROM
    -- 		fox_dw.dwd_fox_whatsapp_chat_record chat
    -- 	WHERE
    -- 		chat.mission_log_id <> 0
    -- 		AND chat.chat_type = 1
    -- 		AND chat.send_time >= "{0}"
    -- 		AND chat.send_time < "{1}"
    -- 	GROUP BY
    -- 		1
    -- 	) wa ON wa.mission_log_id = assign.mission_log_id
        LEFT JOIN (
        SELECT
            cr.mission_log_id,
            SUM(cr.repaid_total_amount)/100 `实收`
        FROM
            ods_fox_collect_recovery cr
        WHERE
            cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        LEFT JOIN ods_fox_mission_log unassign ON unassign.mission_log_id = mlur.mission_log_id
        AND unassign.mission_log_operator = 'unassign'
        AND unassign.mission_log_create_at <= "{1}"
        WHERE 1 = 1
        AND assign.mission_log_operator = 'assign'
        -- 	AND assign.mission_strategy != 'new_assets_returned_division_cases'
        AND assign.director_name IS NOT NULL
        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
        AND NOT ( mlur.mission_log_unassign_reason IN ( '分案不均', 'UNEVEN_WITHDRAW_CASE' ) AND mlur.mission_log_unassign_reason IS NOT NULL AND unassign.mission_log_create_at IS NOT NULL )
    GROUP BY 1,2,3,4,5,6),

    拨打数据 AS (
    SELECT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        sum(call_time) '接通时长',
        sum(IF( ods_audit_call_history.dial_time > 0, 1, 0 )) AS '拨打次数',
        sum(IF( ods_audit_call_history.call_time > 0, 1, 0 )) AS '接通次数'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
        and ods_audit_call_history.pt_date >= "{0}"
        and ods_audit_call_history.pt_date <=  "{1}"
        and ods_audit_call_history_extend.pt_date >= "{0}"
        and ods_audit_call_history_extend.pt_date <= "{1}"
    GROUP BY
        催员ID,债务人ID),

    拨打首通接通 AS(
    SELECT DISTINCT
        ods_audit_call_history.dunner_id AS '催员ID',
        debtor_id AS '债务人ID',
        IF((FIRST_VALUE(call_time) OVER(PARTITION BY debtor_id ORDER BY call_at ASC)) > 0 ,1,0) '是否首通接通'
    FROM
        ods_audit_call_history
        JOIN ods_audit_call_history_extend ON ods_audit_call_history.id = ods_audit_call_history_extend.source_id
    WHERE
        ods_audit_call_history.call_at >= "{0}"
        AND ods_audit_call_history.call_at <= "{1}"
        and ods_audit_call_history.pt_date >= "{0}"
        and ods_audit_call_history.pt_date <=  "{1}"
        and ods_audit_call_history_extend.pt_date >= "{0}"
        and ods_audit_call_history_extend.pt_date <= "{1}"),

    短信数据 AS (
    SELECT
        ods_audit_sms_history_extend.dunner_id AS '催员ID',
        ods_audit_sms_history_extend.debtor_id AS '债务人ID',
        count(*) as '短信发送次数'
    FROM
        ods_audit_sms_history
        JOIN ods_audit_sms_history_extend ON ods_audit_sms_history.id = ods_audit_sms_history_extend.source_id
    WHERE
        ods_audit_sms_history.sms_at >= "{0}"
        AND ods_audit_sms_history.sms_at <= "{1}"

        and sms_channel =1
    GROUP BY
        催员ID,债务人ID),

    排班 as(
    SELECT SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) 组别,dtl.user_id,uo.manager_user_name,uo.parent_user_name,sum(schedule_status) 排班天数,sum(attendance_status) 上线天数
    from ods_fox_collect_attendance_dtl dtl
    LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = dtl.work_day AND uo.user_id = dtl.user_id
    WHERE dtl.work_day >= "{0}"
    AND dtl.work_day <= "{1}"
    GROUP BY dtl.user_id,uo.manager_user_name,uo.parent_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',-1)
    ),
    a1 as (SELECT uo.pt_date,uo.user_id,mx.* from(
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
        AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <=  "{1}" )
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
    right JOIN (select pt_date,user_id from fox_dw.dwd_fox_user_organization_df WHERE pt_date >= "{0}" AND pt_date <= "{1}") uo on uo.user_id = mx.催员ID AND uo.pt_date = date(mx.分案日期)),

    `a2` AS (select manager_user_name 主管,
    asset_group_name 组别,
    parent_user_name 组长,
    user_id,
    sum(分案数) 新分案,
                    sum(`分案本金(总)`) 新案本金,
            round(avg(if(e.日期 = date("{1}"),日在手案件,null)),2) 本日在手案件,
            round(avg(if(e.日期 = date("{1}"),日在手金额,null)),2) 本日在手案件金额,
            round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手案件,null)),2) 本日在线在手案件,
            round(avg(if(attendance_status =1 and e.日期 = date("{1}"),日在手金额,null)),2) 本日在线在手案件金额
            from (
    SELECT d.*,if((累计分案-累计撤案) is null,累计分案,累计分案-累计撤案) 日在手案件,
    if((累计分案本金-累计撤案本金) is null,累计分案本金,累计分案本金-累计撤案本金) 日在手金额,
    uo.manager_user_name,SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) `asset_group_name`,
    uo.parent_user_name,uo.user_id,dtl.attendance_status

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
            LEFT JOIN fox_dw.dwd_fox_user_organization_df uo on uo.pt_date = d.日期 AND uo.user_id = d.ID
            left JOIN ods_fox_collect_attendance_dtl dtl ON dtl.work_day = d.日期 and dtl.user_id = d.ID
            )e
            where
             attendance_status is not null
             group by manager_user_name,
    asset_group_name,
    parent_user_name,
    user_id),

    base as(
    select
`业务组2` as `业务组`,
`队列`,
-- mission_group_name as asset_group_name,
d.`正式主管中文名` as `管理层`,
`组长`,
`催员姓名`,
-- `账龄`,
-- `进入账龄第一天`,
count(distinct `债务人ID`) as `月累计资产数`,
round(count(distinct `债务人ID`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案量`,
sum(`分案本金`) as `分案本金`,
round(sum(`分案本金`)/count(distinct  concat(`催员ID`,`分案日期`)),0) as `日人均新案分案金额`

from
(
select `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`催员姓名`,`资产ID`,`主管`,`组长`,`队列`,`债务人ID`, sum(分案本金) 分案本金
from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
           -- ml.mission_log_id `分案id`,
            ml.mission_group_name,
            zb.`业务组` as `业务组2`,
            SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            ml.assigned_sys_user_id `催员ID`,
			ml.mission_log_assigned_user_name `催员姓名`,
            date(ml.mission_log_create_at) `分案日期`,
            date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,
            ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
            IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in( "分案不均","UNEVEN_WITHDRAW_CASE","分案前已结清","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均,
			ml.debtor_id as `债务人ID`
    from ods_fox_mission_log ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
         AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <="{1}" )
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
        repay_date >= "{0}"
        AND repay_date <="{1}"
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

	left join
		(select user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1) as group_name
		 from fox_dw.dwd_fox_user_organization_df
	    -- fox_tmp.dwd_fox_user_organization_df -- 印尼
		-- fox_bi.user_organization_df-- 除印尼和中国外的其他国家

		group by user_id,pt_date,asset_group_name, SUBSTRING_INDEX(asset_group_name, ',',-1)

		)uo
		on (ml.assigned_sys_user_id=uo.user_id and substr(ml.mission_log_create_at,1,10)=substr(uo.pt_date,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <="{1}")
    -- and not (mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION") and date(ml.mission_log_create_at) = date(unml.mission_log_create_at))
                )aa
 WHERE `是否分案不均` = 0
group by `账龄`,`进入账龄第一天`, `业务组2`,mission_group_name,`分案日期`,`催员ID`,`资产ID`,`主管`,`组长`,`队列`,`催员姓名`,`债务人ID`

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
 -- and `队列`not in ('94','IVR组','客维组','月中补案组')----不同国家需要修改
and `账龄` is not null
and d.`正式主管中文名` is not null

group by `业务组2`,
-- mission_group_name,
-- `账龄`,
-- `进入账龄第一天`,
d.`正式主管中文名`,`组长`,`队列`,`催员姓名`
order by `业务组2`

    )



    SELECT date("{1}") as 统计日期,
    ca1.业务组,
    ca1.组别 as 队列,
    ca1.正式主管中文名  as '管理层',
    ca1.组长,
    (SELECT `no` from fox_ods.ods_fox_sys_user su WHERE ca1.`催员name` = su.`name`) `user_no`,
    ca1.`催员name`,
    c.user_type as `在职or离职`,
    上线天数,
    排班天数,
        round(上线天数/排班天数,4) as `上线率`,
        -- round(分案数/上线天数,0) as `日均新案量`,
        -- round(`分案本金(总)`/上线天数) as `日均新案金额`,
        ba.`日人均新案分案量` as `日均新案量`,
        ba.`日人均新案分案金额` as `日均新案金额`,
        -- 本日在手案件,
        -- 本日在手案件金额,
        -- 本日在线在手案件,
        -- 本日在线在手案件金额,
        round(电话拨打覆盖债务人量/上线天数,0) as `日均拨打案件数`,
        round(电话拨打次数/上线天数,0) as `日均拨打次数`,
        round(通时/上线天数,0) as `日均通话时长`,
        round(电话接通次数/上线天数,0) as `日均接通次数`,
    -- 	日均WA发送案件数,
    -- 	日均WA发送次数,
        round(短信覆盖债务人量/上线天数,0) as `日均短信发送案件数`,
        round(短信发送量 / 上线天数,0) as `日均短信发送条数`

    from (SELECT
        t.组别,
        t.业务组,
        t.主管,
        t.正式主管中文名,
        t.组长,
        t.`催员name`,
        t.催员ID,
    -- 	round(sum(上线天数)/sum(排班天数),4) as `上线率`,
    -- 	sum(分案数)/sum(上线天数) as `日均新案量`,
    -- 	round(sum(`分案本金(总)`)/sum(上线天数)) as `日均新案金额`,
    -- 	sum(在手案件),
    -- 	sum(在手金额),
    -- 	ROUND(COUNT(IF(t.WA发送数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(COUNT(IF(t.wa点击次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `WA覆盖率`,
    -- 	ROUND(SUM(t.WA发送数) / COUNT(t.债务人ID),4) `案均WA触达次数`,
    -- 	ROUND(SUM(t.WA回复数) / COUNT(t.债务人ID),4) `案均WA接通次数`,
        ROUND(COUNT(IF(t.短信发送次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `短信覆盖率`,
        ROUND(SUM(t.短信发送次数) / COUNT(t.债务人ID),4) `案均短信发送次数`,
    -- 	ROUND('',4) `案均短信回执次数`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`,
        COUNT(t.债务人ID) `总债务人量`,
    -- 	COUNT(IF(t.WA发送数 > 0,1,NULL)) `wa覆盖债务人量`,
    -- 	SUM(t.WA发送数) `wa发送量`,
    -- 	SUM(t.WA回复数) `wa回复次数`,
        COUNT(IF(t.短信发送次数 > 0,1,NULL)) `短信覆盖债务人量`,
        SUM(t.短信发送次数) `短信发送量`,
        COUNT(IF(t.拨打次数 > 0,1,NULL)) `电话拨打覆盖债务人量`,
        SUM(t.拨打次数) `电话拨打次数`,
        SUM(t.接通次数) `电话接通次数`,
        sum(t.接通时长) 通时,
        COUNT(IF(t.接通次数 > 0,1,NULL)) `电话拨打接通债务人量`,
        COUNT(IF(t.是否首通接通 > 0,1,NULL)) `首通接通债务人量`,
        COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) `可联还款债务人量`
    from(
    SELECT
        e.业务组,
        IF(LOWER(a.组别) LIKE '%os%','委外','自营') 类别,
        a.组别,
        a.主管,
        a.组长,
        d.正式主管中文名,
        a.催员ID,
        a.`催员name`,
        a.债务人ID,
    -- 	a.WA发送数,
    -- 	a.WA回复数,
    -- 	mw.wa点击次数,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b.接通时长,
        b1.是否首通接通,
        c.短信发送次数
     FROM
        `分案还款及wa数据` a
        LEFT JOIN `拨打数据` b ON a.`催员ID` = b.`催员ID`
        AND a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`催员ID` = b1.`催员ID`
        AND a.`债务人ID` = b1.`债务人ID`
        LEFT JOIN `短信数据` c ON a.`催员ID` = c.`催员ID`
        AND a.`债务人ID` = c.`债务人ID`
        LEFT JOIN fox_tmp.`主管对应关系` d ON a.主管 = d.储备主管
        AND d.年 = year("{0}") AND d.月 = month("{0}")
        LEFT JOIN fox_tmp.组别信息 e ON a.组别 = e.队列
        AND e.年 = year("{0}") AND e.月 = month("{0}")
        WHERE a.组别 NOT IN ('cultivate','PKCC','THCC','MXCC','94','客维组','IVR组')
        and 正式主管中文名 is not NULL) t
        group by 1,2,3,4,5,6,7) ca1
        LEFT JOIN 排班 on 排班.user_id = ca1.`催员ID` and 排班.manager_user_name = ca1.主管 and 排班.parent_user_name = ca1.组长 and ca1.`组别`=排班.`组别`
        LEFT join (SELECT 	user_id,主管,组长,组别,sum(本日在手案件) 本日在手案件,sum(本日在手案件金额) 本日在手案件金额 ,sum(本日在线在手案件) 本日在线在手案件,sum(本日在线在手案件金额) 本日在线在手案件金额 ,sum(新分案) 分案数,sum(新案本金) as `分案本金(总)`
        from a2 GROUP BY user_id,主管,组长,组别) 在手 ON 在手.user_id = ca1.`催员ID` and 在手.主管 = ca1.主管 and ca1.组长 = 在手.组长 AND 在手.组别 = ca1.组别
        left join base ba
        on ca1.`业务组`=ba.`业务组` and
        ca1.`组别`=ba.`队列` and
        ca1.`正式主管中文名`=ba.`管理层` and
        ca1.`组长`=ba.`组长`  and
        ca1.`催员name`=ba.`催员姓名`  
        left join 
				(select id,
				case when del_date is not null and 
				          datediff("{1}",del_date)>0 then '离职' else '在职' end as user_type
				from ods_fox_sys_user 
				group by id,
				case when del_date is not null and 
				          datediff("{1}",del_date)>0 then '离职' else '在职' end)c
				on (ca1.`催员ID`=c.id)
        WHERE 排班天数 > 0
        """.format(third_day + " 00:00:00", yesterday + " 23:59:59", 2024, 11)
    return sql


def 完全出催数据中国(first_day, yesterday):
    sql = """
    select
substr(`分案统计截止日期`,1,10) as `分案截止日期`,
`账龄`,
sum(`分案本金`) as `分案本金`,
sum(`催回本金`) as `催回本金`,
sum(`展期费`) as `展期费`,
concat(round(sum(`催回本金`)/sum(`分案本金`)*100,2),'%') as `催回率`,
concat(round((sum(`催回本金`)+sum(`展期费`))/sum(`分案本金`)*100,2),'%') as `催回率(含展期费)`

from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
            ml.mission_log_id `分案id`,
            ml.mission_group_name `业务组`,
						zb.`业务组` as `业务组2`,
            -- SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            -- ml.assigned_sys_user_id `催员ID`,
            -- date(ml.mission_log_create_at) `分案日期`,
            -- date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,




						ml.debtor_id as `债务人id`,
						ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
           case when IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金)>0 then IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) else 0 end 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION","分案不均","存在逾期案件时撤预提醒案件","存在到期案件时撤预提醒案件"),1,0) 是否分案不均,
						f.`分案统计截止日期`
    from 
		(
		select *
			FROM
			(
						select 
						*,
						ROW_NUMBER() over(partition by mission_log_asset_id,date(mission_log_create_at) order by mission_log_create_at desc) as num
						from
						ods_fox_mission_log ml
						WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
		)a
		where a.num=1
		) ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" )
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
		on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列`
		 and zb.`进入账龄第一天`=ml.assign_asset_late_days
		 and zb.`进入账龄第一天`=ml.assign_debtor_late_days
		)

		left join
		(
		select distinct `年`,`月`,`账龄`,
     `进入账龄第一天`,
     `进入账龄最后一天`,
     date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))) as `分案统计截止日期`,
     case when DATEDIFF(date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(`进入账龄第一天` as int))),"{0}")>=0 then 1 else 0 end as `分案统计截止日期_是否有效`
from
(
select `年`,`月`,`队列`,`业务组`,`账龄`,
case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`,
split(`账龄`,'-')[2]  as `进入账龄最后一天`
from fox_tmp.`组别信息`
where `年`=year("{0}") and `月`=month("{0}")

)a
where cast(a.`进入账龄第一天` as int)>=1
and cast(a.`进入账龄最后一天` as int)<=15
order by `账龄`
		)f
		on (zb.`账龄`=f.`账龄`)
    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
		and datediff(f.`分案统计截止日期`,ml.mission_log_create_at)>=0
		and f.`分案统计截止日期_是否有效`=1

		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)a
where `是否分案不均`=0
group by `账龄`,substr(`分案统计截止日期`,1,10)
order by `账龄`
    """.format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql


# 上一个数据 10月 完全出催数据中国



def 完全出催数据中国_上个月(first_day, yesterday):
    sql = """
select
substr(`分案统计截止日期`,1,10) as `分案截止日期`,
`账龄`,
sum(`分案本金`) as `分案本金`,
sum(`催回本金`) as `催回本金`,
sum(`展期费`) as `展期费`,
concat(round(sum(`催回本金`)/sum(`分案本金`)*100,2),'%') as `催回率`,
concat(round((sum(`催回本金`)+sum(`展期费`))/sum(`分案本金`)*100,2),'%') as `催回率(含展期费)`

from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
            ml.mission_log_id `分案id`,
            ml.mission_group_name `业务组`,
						zb.`业务组` as `业务组2`,
            -- SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            -- ml.assigned_sys_user_id `催员ID`,
            -- date(ml.mission_log_create_at) `分案日期`,
            -- date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,
						ml.debtor_id as `债务人id`,
						ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
           case when IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金)>0 then IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) else 0 end 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in ("分案不均","存在逾期案件时撤预提醒案件","存在到期案件时撤预提醒案件"),1,0) 是否分案不均,
						f.`分案统计截止日期`
    from 
		(
		select *
			FROM
			(
						select 
						*,
						ROW_NUMBER() over(partition by mission_log_asset_id,date(mission_log_create_at) order by mission_log_create_at desc) as num
						from
						ods_fox_mission_log ml
						WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'))
		AND ml.mission_log_create_at <= "{1}")
		)a
		where a.num=1
		) ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00') AND unml.mission_log_create_at <= "{1}" )
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
        repay_date >= concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00')
        AND repay_date <= "{1}"
        and repaid_total_amount >0
        AND batch_num IS NOT NULL
    GROUP BY
        mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
		left join
		(select `年`,`月`,`队列`,`业务组`,`账龄`,
		case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
		from fox_tmp.`组别信息`
		where `年`=year(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00')) and `月`=month(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00'))
		) zb
		on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列`
		 and zb.`进入账龄第一天`=ml.assign_asset_late_days
		 and zb.`进入账龄第一天`=ml.assign_debtor_late_days
		)

		left join
		(
select distinct `年`,`月`,`账龄`,
     `进入账龄第一天`,
     `进入账龄最后一天`,
		 case when datediff(concat(substr(date_sub("{0}",1),1,10),' 23:59:59'),date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))))>=0 then date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))) else date(concat(substr(date_sub("{0}",1),1,10),' 23:59:59')) end as `分案统计截止日期`,
     case when DATEDIFF(case when datediff(concat(substr(date_sub("{0}",1),1,10),' 23:59:59'),date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))))>=0 then date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))) else date(concat(substr(date_sub("{0}",1),1,10),' 23:59:59')) end,concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00'))>=0 then 1 else 0 end as `分案统计截止日期_是否有效`
from
(
select `年`,`月`,`队列`,`业务组`,`账龄`,
case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`,
split(`账龄`,'-')[2]  as `进入账龄最后一天`
from fox_tmp.`组别信息`
where `年`=year(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00')) and `月`=month(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00'))

)a
where cast(a.`进入账龄第一天` as int)>=1
and cast(a.`进入账龄最后一天` as int)<=15
order by `账龄`


		)f
		on (zb.`账龄`=f.`账龄`)
	LEFT JOIN 
	(select mlur.*,unml.mission_log_create_at
	from 
	ods_fox_mission_log_unassign_reason mlur 
   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 where mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION")
	 and unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00') AND unml.mission_log_create_at <= "{1}")
	)ff
	ON (ml.mission_log_id = ff.assign_mission_log_id and substr(ml.mission_log_create_at,1,10)=substr(ff.mission_log_create_at,1,10))


    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00') AND ml.mission_log_create_at <= "{1}")
		and datediff(f.`分案统计截止日期`,ml.mission_log_create_at)>=0
		-- and datediff(concat(substr(date_sub("{0}",1),1,10),' 23:59:59'),f.`分案统计截止日期`)>=0 -- 增加月底日期限制
		and f.`分案统计截止日期_是否有效`=1
		and ff.assign_mission_log_id is null

		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)a
where `是否分案不均`=0
group by `账龄`,substr(`分案统计截止日期`,1,10)
order by `账龄`
""".format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql


# 上一个数据 9月 完全出催数据中国
def 完全出催数据中国_上上个月(first_day, yesterday):
    sql = """
select
substr(`分案统计截止日期`,1,10) as `分案截止日期`,
`账龄`,
sum(`分案本金`) as `分案本金`,
sum(`催回本金`) as `催回本金`,
sum(`展期费`) as `展期费`,
concat(round(sum(`催回本金`)/sum(`分案本金`)*100,2),'%') as `催回率`,
concat(round((sum(`催回本金`)+sum(`展期费`))/sum(`分案本金`)*100,2),'%') as `催回率(含展期费)`

from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
            ml.mission_log_id `分案id`,
            ml.mission_group_name `业务组`,
						zb.`业务组` as `业务组2`,
            -- SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            -- ml.assigned_sys_user_id `催员ID`,
            -- date(ml.mission_log_create_at) `分案日期`,
            -- date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,
						ml.debtor_id as `债务人id`,
						ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
           case when IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金)>0 then IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) else 0 end 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in ("分案不均","存在逾期案件时撤预提醒案件","存在到期案件时撤预提醒案件"),1,0) 是否分案不均,
						f.`分案统计截止日期`
    from 
		(
		select *
			FROM
			(
						select 
						*,
						ROW_NUMBER() over(partition by mission_log_asset_id,date(mission_log_create_at) order by mission_log_create_at desc) as num
						from
						ods_fox_mission_log ml
						WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00')
		AND ml.mission_log_create_at <= "{1}")
		)a
		where a.num=1
		) ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00') AND unml.mission_log_create_at <= "{1}" )
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
        repay_date >= concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00')
        AND repay_date <= "{1}"
        and repaid_total_amount >0
        AND batch_num IS NOT NULL
    GROUP BY
        mission_log_id) cr ON ml.mission_log_id = cr.mission_log_id
		left join
		(select `年`,`月`,`队列`,`业务组`,`账龄`,
		case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
		from fox_tmp.`组别信息`
		where `年`=year(concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00')) and `月`=month(concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00'))
		) zb
		on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列`
		 and zb.`进入账龄第一天`=ml.assign_asset_late_days
		 and zb.`进入账龄第一天`=ml.assign_debtor_late_days
		)

		left join
		(
select distinct `年`,`月`,`账龄`,
     `进入账龄第一天`,
     `进入账龄最后一天`,
		 case when datediff(date_sub(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 23:59:59'),1),date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))))>=0 then date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))) else date(date_sub(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 23:59:59'),1)) end as `分案统计截止日期`,
     case when DATEDIFF(case when datediff(date_sub(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 23:59:59'),1),date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))))>=0 then date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))) else date(date_sub(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 23:59:59'),1)) end,concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00'))>=0 then 1 else 0 end as `分案统计截止日期_是否有效`
from
(
select `年`,`月`,`队列`,`业务组`,`账龄`,
case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`,
split(`账龄`,'-')[2]  as `进入账龄最后一天`
from fox_tmp.`组别信息`
where `年`=year(concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00')) and `月`=month(concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00'))

)a
where cast(a.`进入账龄第一天` as int)>=1
and cast(a.`进入账龄最后一天` as int)<=15
order by `账龄`


		)f
		on (zb.`账龄`=f.`账龄`)
	LEFT JOIN 
	(select mlur.*,unml.mission_log_create_at
	from 
	ods_fox_mission_log_unassign_reason mlur 
   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 where mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION")
	 and unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00') AND unml.mission_log_create_at <= "{1}")
	)ff
	ON (ml.mission_log_id = ff.assign_mission_log_id and substr(ml.mission_log_create_at,1,10)=substr(ff.mission_log_create_at,1,10))


    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00') AND ml.mission_log_create_at <= "{1}")
		and datediff(f.`分案统计截止日期`,ml.mission_log_create_at)>=0
		-- and datediff(date_sub(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 23:59:59'),1),f.`分案统计截止日期`)>=0 -- 增加月底日期限制
		and f.`分案统计截止日期_是否有效`=1
		and ff.assign_mission_log_id is null

		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)a
where `是否分案不均`=0
group by `账龄`,substr(`分案统计截止日期`,1,10)
order by `账龄`
""".format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql




def 完全出催数据非中国(first_day, yesterday):
    sql = """
    select
substr(`分案统计截止日期`,1,10) as `分案截止日期`,
`账龄`,
sum(`分案本金`) as `分案本金`,
sum(`催回本金`) as `催回本金`,
sum(`展期费`) as `展期费`,
concat(round(sum(`催回本金`)/sum(`分案本金`)*100,2),'%') as `催回率`,
-- concat(round((1-sum(`催回本金`)/sum(`分案本金`))*100,2),'%') as `D1入催率`,
concat(round((sum(`催回本金`)+sum(`展期费`))/sum(`分案本金`)*100,2),'%') as `催回率(含展期费)`

from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
            ml.mission_log_id `分案id`,
            ml.mission_group_name `业务组`,
						zb.`业务组` as `业务组2`,
            -- SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            -- ml.assigned_sys_user_id `催员ID`,
            -- date(ml.mission_log_create_at) `分案日期`,
            -- date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,			
			      ml.debtor_id as `债务人id`,
			      ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
           case when IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金)>0 then IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) else 0 end 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in( "分案不均","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均,
			f.`分案统计截止日期`
    from (
		select *
			FROM
			(
						select 
						*,
						ROW_NUMBER() over(partition by mission_log_asset_id,date(mission_log_create_at) order by mission_log_create_at desc) as num
						from
						ods_fox_mission_log ml
						WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
		)a
		where a.num=1
		) ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}" )
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
        repay_date >= "{0}"
        AND repay_date <= "{1}"
        and repaid_total_amount >0
        AND batch_num IS NOT NULL
    GROUP BY
        mission_log_id) cr ON (ml.mission_log_id = cr.mission_log_id )
		left join
		(select `年`,`月`,`队列`,`业务组`,`账龄`,
		case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
		from fox_tmp.`组别信息`
		where `年`=year("{0}") and `月`=month("{0}")
		) zb
		on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列`
		 and zb.`进入账龄第一天`=ml.assign_asset_late_days
		 and zb.`进入账龄第一天`=ml.assign_debtor_late_days
		)

		left join
		(
		select distinct `年`,`月`,`账龄`,
     `进入账龄第一天`,
     `进入账龄最后一天`,
     date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))) as `分案统计截止日期`,
     case when DATEDIFF(date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(`进入账龄第一天` as int))),"{0}")>=0 then 1 else 0 end as `分案统计截止日期_是否有效`
from
(
select `年`,`月`,`队列`,`业务组`,`账龄`,
case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`,
split(`账龄`,'-')[2]  as `进入账龄最后一天`
from fox_tmp.`组别信息`
where `年`=year("{0}") and `月`=month("{0}")

)a
where cast(a.`进入账龄第一天` as int)>=1
and cast(a.`进入账龄最后一天` as int)<=15
order by `账龄`
		)f
		on (zb.`账龄`=f.`账龄`)

		LEFT JOIN 
	(select mlur.*,unml.mission_log_create_at
	from 
	ods_fox_mission_log_unassign_reason mlur 
   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 where mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION")
	 and unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= "{0}" AND unml.mission_log_create_at <= "{1}")
	)ff
	ON (ml.mission_log_id = ff.assign_mission_log_id and substr(ml.mission_log_create_at,1,10)=substr(ff.mission_log_create_at,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= "{0}" AND ml.mission_log_create_at <= "{1}")
		and datediff(f.`分案统计截止日期`,ml.mission_log_create_at)>=0
		and f.`分案统计截止日期_是否有效`=1
		and ff.assign_mission_log_id is null
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

)a
where `是否分案不均`=0
group by `账龄`,substr(`分案统计截止日期`,1,10)
order by `账龄`
    """.format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql


# 10月 非中国完全出催数据

def 完全出催数据非中国_上个月(first_day, yesterday):
    sql = """		
select
substr(`分案统计截止日期`,1,10) as `分案截止日期`,
`账龄`,
sum(`分案本金`) as `分案本金`,
sum(`催回本金`) as `催回本金`,
sum(`展期费`) as `展期费`,
concat(round(sum(`催回本金`)/sum(`分案本金`)*100,2),'%') as `催回率`,
-- concat(round((1-sum(`催回本金`)/sum(`分案本金`))*100,2),'%') as `D1入催率`,
concat(round((sum(`催回本金`)+sum(`展期费`))/sum(`分案本金`)*100,2),'%') as `催回率(含展期费)`

from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
            ml.mission_log_id `分案id`,
            ml.mission_group_name `业务组`,
						zb.`业务组` as `业务组2`,
            -- SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            -- ml.assigned_sys_user_id `催员ID`,
            -- date(ml.mission_log_create_at) `分案日期`,
            -- date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,			
			      ml.debtor_id as `债务人id`,
			      ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
           case when IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金)>0 then IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) else 0 end 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in( "分案不均","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均,
			f.`分案统计截止日期`
    from (
		select *
			FROM
			(
						select 
						*,
						ROW_NUMBER() over(partition by mission_log_asset_id,date(mission_log_create_at) order by mission_log_create_at desc) as num
						from
						ods_fox_mission_log ml
						WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00') AND ml.mission_log_create_at <= "{1}")
		)a
		where a.num=1
		) ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00') AND unml.mission_log_create_at <= "{1}" )
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
        repay_date >= concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00')
        AND repay_date <= "{1}"
        and repaid_total_amount >0
        AND batch_num IS NOT NULL
    GROUP BY
        mission_log_id) cr ON (ml.mission_log_id = cr.mission_log_id )
		left join
		(select `年`,`月`,`队列`,`业务组`,`账龄`,
		case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
		from fox_tmp.`组别信息`
		where `年`=year(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00')) and `月`=month(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00'))
		) zb
		on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列`
		 and zb.`进入账龄第一天`=ml.assign_asset_late_days
		 and zb.`进入账龄第一天`=ml.assign_debtor_late_days
		)


		left join
		(
select distinct `年`,`月`,`账龄`,
     `进入账龄第一天`,
     `进入账龄最后一天`,
		 case when datediff(concat(substr(date_sub("{0}",1),1,10),' 23:59:59'),date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))))>=0 then date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))) else date(concat(substr(date_sub("{0}",1),1,10),' 23:59:59')) end as `分案统计截止日期`,
     case when DATEDIFF(case when datediff(concat(substr(date_sub("{0}",1),1,10),' 23:59:59'),date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))))>=0 then date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))) else date(concat(substr(date_sub("{0}",1),1,10),' 23:59:59')) end,concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00'))>=0 then 1 else 0 end as `分案统计截止日期_是否有效`
from
(
select `年`,`月`,`队列`,`业务组`,`账龄`,
case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`,
split(`账龄`,'-')[2]  as `进入账龄最后一天`
from fox_tmp.`组别信息`
where `年`=year(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00')) and `月`=month(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00'))

)a
where cast(a.`进入账龄第一天` as int)>=1
and cast(a.`进入账龄最后一天` as int)<=15
order by `账龄`


		)f
		on (zb.`账龄`=f.`账龄`)

		LEFT JOIN 
	(select mlur.*,unml.mission_log_create_at
	from 
	ods_fox_mission_log_unassign_reason mlur 
   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 where mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION")
	 and unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00') AND unml.mission_log_create_at <= "{1}")
	)ff
	ON (ml.mission_log_id = ff.assign_mission_log_id and substr(ml.mission_log_create_at,1,10)=substr(ff.mission_log_create_at,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 00:00:00') AND ml.mission_log_create_at <= "{1}")
		and datediff(f.`分案统计截止日期`,ml.mission_log_create_at)>=0
		and f.`分案统计截止日期_是否有效`=1
		and ff.assign_mission_log_id is null
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

)a
where `是否分案不均`=0
group by `账龄`,substr(`分案统计截止日期`,1,10)
order by `账龄`

""".format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql


# 9月 非中国完全出催数据


def 完全出催数据非中国_上上个月(first_day, yesterday):
    sql = """			
select
substr(`分案统计截止日期`,1,10) as `分案截止日期`,
`账龄`,
sum(`分案本金`) as `分案本金`,
sum(`催回本金`) as `催回本金`,
sum(`展期费`) as `展期费`,
concat(round(sum(`催回本金`)/sum(`分案本金`)*100,2),'%') as `催回率`,
-- concat(round((1-sum(`催回本金`)/sum(`分案本金`))*100,2),'%') as `D1入催率`,
concat(round((sum(`催回本金`)+sum(`展期费`))/sum(`分案本金`)*100,2),'%') as `催回率(含展期费)`

from
(
SELECT      zb.`账龄`,
            zb.`进入账龄第一天`,
            ml.mission_log_id `分案id`,
            ml.mission_group_name `业务组`,
						zb.`业务组` as `业务组2`,
            -- SUBSTRING_INDEX( uo.asset_group_name, ',',- 1 ) as `队列`,
            ml.director_name `主管`,
            ml.group_leader_name `组长`,
            -- ml.assigned_sys_user_id `催员ID`,
            -- date(ml.mission_log_create_at) `分案日期`,
            -- date(unml.mission_log_create_at) `撤案日期`,
            ml.mission_log_asset_id `资产ID`,			
			      ml.debtor_id as `债务人id`,
			      ml.assign_asset_late_days as `逾期天数`,
            ml.assign_principal_amount * 0.01 `分案本金`,
           case when IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金)>0 then IF(催回本金 > ml.assign_principal_amount * 0.01,ml.assign_principal_amount * 0.01,催回本金) else 0 end 催回本金,
            展期费,
            总实收,
            if(mlur.mission_log_unassign_reason in( "分案不均","SETTLED_BEFORE_DIVISION"),1,0) 是否分案不均,
			f.`分案统计截止日期`
    from (
		select *
			FROM
			(
						select 
						*,
						ROW_NUMBER() over(partition by mission_log_asset_id,date(mission_log_create_at) order by mission_log_create_at desc) as num
						from
						ods_fox_mission_log ml
						WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00') AND ml.mission_log_create_at <= "{1}")
		)a
		where a.num=1
		) ml
    LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON ml.mission_log_id = mlur.assign_mission_log_id

   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 AND unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00') AND unml.mission_log_create_at <= "{1}" )
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
        repay_date >= concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00')
        AND repay_date <= "{1}"
        and repaid_total_amount >0
        AND batch_num IS NOT NULL
    GROUP BY
        mission_log_id) cr ON (ml.mission_log_id = cr.mission_log_id )
		left join
		(select `年`,`月`,`队列`,`业务组`,`账龄`,
		case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`
		from fox_tmp.`组别信息`
		where `年`=year(concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00')) and `月`=month(concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00'))
		) zb
		on (year(ml.mission_log_create_at)=zb.`年` and MONTH(ml.mission_log_create_at)=zb.`月` and ml.mission_group_name=zb.`队列`
		 and zb.`进入账龄第一天`=ml.assign_asset_late_days
		 and zb.`进入账龄第一天`=ml.assign_debtor_late_days
		)


		left join
		(
select distinct `年`,`月`,`账龄`,
     `进入账龄第一天`,
     `进入账龄最后一天`,
		 case when datediff(date_sub(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 23:59:59'),1),date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))))>=0 then date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))) else date(date_sub(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 23:59:59'),1)) end as `分案统计截止日期`,
     case when DATEDIFF(case when datediff(date_sub(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 23:59:59'),1),date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))))>=0 then date_sub("{1}",(cast(`进入账龄最后一天` as int)-cast(   `进入账龄第一天` as int))) else date(date_sub(concat(DATE_FORMAT("{0}" - INTERVAL 1 MONTH, '%Y-%m-01'),' 23:59:59'),1)) end,concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00'))>=0 then 1 else 0 end as `分案统计截止日期_是否有效`
from
(
select `年`,`月`,`队列`,`业务组`,`账龄`,
case when substr(`账龄`,1,2)<0 then substr(`账龄`,1,2) else split(`账龄`,'-')[1] end as `进入账龄第一天`,
split(`账龄`,'-')[2]  as `进入账龄最后一天`
from fox_tmp.`组别信息`
where `年`=year(concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00')) and `月`=month(concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00'))

)a
where cast(a.`进入账龄第一天` as int)>=1
and cast(a.`进入账龄最后一天` as int)<=15
order by `账龄`


		)f
		on (zb.`账龄`=f.`账龄`)

		LEFT JOIN 
	(select mlur.*,unml.mission_log_create_at
	from 
	ods_fox_mission_log_unassign_reason mlur 
   LEFT JOIN fox_ods.ods_fox_mission_log unml ON mlur.mission_log_id = unml.mission_log_id
	 where mlur.mission_log_unassign_reason in ("分案前已结清","SETTLED_BEFORE_DIVISION")
	 and unml.mission_log_operator = 'unassign'
    AND ( unml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00') AND unml.mission_log_create_at <= "{1}")
	)ff
	ON (ml.mission_log_id = ff.assign_mission_log_id and substr(ml.mission_log_create_at,1,10)=substr(ff.mission_log_create_at,1,10))

    WHERE ml.mission_log_operator = 'assign'
    AND (ml.mission_log_create_at >= concat(DATE_FORMAT("{0}" - INTERVAL 2 MONTH, '%Y-%m-01'),' 00:00:00') AND ml.mission_log_create_at <= "{1}")
		and datediff(f.`分案统计截止日期`,ml.mission_log_create_at)>=0
		and f.`分案统计截止日期_是否有效`=1
		and ff.assign_mission_log_id is null
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

)a
where `是否分案不均`=0
group by `账龄`,substr(`分案统计截止日期`,1,10)
order by `账龄`
""".format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql


def 团队过程指标IVR_中国(first_day, yesterday):
    sql = """
WITH `分案还款` AS (
SELECT
        assign.debtor_id AS '债务人ID',
        assign.mission_group_name AS '组别',
        assign.assigned_sys_user_id AS '催员ID',
        assign.director_name '主管',
        IF(SUM(r.实收) > 0,1,0) `是否还款`
FROM
        ods_fox_mission_log assign
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id 
        LEFT JOIN (
        SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
        FROM
                ods_fox_collect_recovery cr 
        WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= '2024-11-18 23:59:59'
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        WHERE 1 = 1 
        AND assign.mission_log_operator = 'assign' 
        AND (assign.mission_group_name like '%IVR%' OR assign.mission_group_name like '%94%')

        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= '2024-11-18 23:59:59')
GROUP BY 1,2,3,4),

拨打数据 AS (
                                SELECT ivr_call_history_business_no '债务人ID',count(1) 拨打次数,sum(if(ivr_call_history_talk_duration>0,1,0)) 接通次数
                                from fox_ods.ods_fox_ivr_call_history
                                where ivr_call_history_call_start_at >= "{0}"
                                AND ivr_call_history_call_start_at <= '2024-11-18 23:59:59'
                                GROUP BY ivr_call_history_business_no),
拨打首通接通 AS(
SELECT DISTINCT
        ivr_call_history_business_no AS '债务人ID',
        IF((FIRST_VALUE(ivr_call_history_talk_duration) OVER(PARTITION BY ivr_call_history_business_no ORDER BY ivr_call_history_call_start_at ASC)) > 0 ,1,0) '是否首通接通'
FROM
        fox_ods.ods_fox_ivr_call_history ich
WHERE
        ivr_call_history_call_start_at >= "{0}"
        AND ivr_call_history_call_start_at <= '2024-11-18 23:59:59')

SELECT
        t.组别,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
FROM
(
SELECT
        a.组别,
        a.催员ID,
        a.债务人ID,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b1.是否首通接通
FROM
        `分案还款` a
        LEFT JOIN `拨打数据` b ON a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`债务人ID` = b1.`债务人ID`
        WHERE a.组别 NOT IN ('cultivate','PKCC','PHCC','MXCC','THCC')) t
GROUP BY 1  
""".format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql



def 团队过程指标IVR_巴基斯坦(first_day, yesterday):
    sql = """
WITH `分案还款` AS (
SELECT
        assign.debtor_id AS '债务人ID',
        assign.mission_group_name AS '组别',
        assign.assigned_sys_user_id AS '催员ID',
        assign.director_name '主管',
        IF(SUM(r.实收) > 0,1,0) `是否还款`
FROM
        ods_fox_mission_log assign
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id 
        LEFT JOIN (
        SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
        FROM
                ods_fox_collect_recovery cr 
        WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        WHERE 1 = 1 
        AND assign.mission_log_operator = 'assign' 
        AND (assign.mission_group_name like '%IVR%' OR assign.mission_group_name like '%94%')

        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
GROUP BY 1,2,3,4),

拨打数据 AS (
                                SELECT debtor_id '债务人ID',count(1) 拨打次数,sum(if(talk_duration>0,1,0)) 接通次数
                                from fox_ods.ods_fox_ivr_call_history
                                where call_start_at >="{0}"
                                AND call_start_at <="{1}"
                                GROUP BY debtor_id),
拨打首通接通 AS(
SELECT DISTINCT
        debtor_id AS '债务人ID',
        IF((FIRST_VALUE(talk_duration) OVER(PARTITION BY debtor_id ORDER BY call_start_at ASC)) > 0 ,1,0) '是否首通接通'
FROM
        fox_ods.ods_fox_ivr_call_history ich
WHERE
        call_start_at >= "{0}"
        AND call_start_at <= "{1}")


select *
from 
(
SELECT
        t.组别,
				case when t.组别='94AI' then '-2-0' end as `账龄`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
FROM
(
SELECT
        a.组别,
        a.催员ID,
        a.债务人ID,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b1.是否首通接通
FROM
        `分案还款` a
        LEFT JOIN `拨打数据` b ON a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`债务人ID` = b1.`债务人ID`
        WHERE a.组别 NOT IN ('cultivate','PKCC','PHCC','MXCC','THCC')) t
GROUP BY 1,2

)aaa
order by aaa.`账龄`
""".format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql



def 团队过程指标IVR_菲律宾(first_day, yesterday):
    sql = """
WITH `分案还款` AS (
SELECT
        assign.debtor_id AS '债务人ID',
        assign.mission_group_name AS '组别',
        assign.assigned_sys_user_id AS '催员ID',
        assign.director_name '主管',
        IF(SUM(r.实收) > 0,1,0) `是否还款`
FROM
        ods_fox_mission_log assign
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id 
        LEFT JOIN (
        SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
        FROM
                ods_fox_collect_recovery cr 
        WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        WHERE 1 = 1 
        AND assign.mission_log_operator = 'assign' 
        AND (assign.mission_group_name like '%IVR%' OR assign.mission_group_name like '%94%')

        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
GROUP BY 1,2,3,4),

拨打数据 AS (
                                SELECT debtor_id '债务人ID',count(1) 拨打次数,sum(if(talk_duration>0,1,0)) 接通次数
                                from fox_ods.ods_fox_ivr_call_history
                                where call_start_at >="{0}"
                                AND call_start_at <="{1}"
                                GROUP BY debtor_id),
拨打首通接通 AS(
SELECT DISTINCT
        debtor_id AS '债务人ID',
        IF((FIRST_VALUE(talk_duration) OVER(PARTITION BY debtor_id ORDER BY call_start_at ASC)) > 0 ,1,0) '是否首通接通'
FROM
        fox_ods.ods_fox_ivr_call_history ich
WHERE
        call_start_at >= "{0}"
        AND call_start_at <= "{1}")


select *
from 
(
SELECT
        t.组别,
				case when t.组别='IVR' then '-2-0'
				     when t.组别='WIZ-IVR' then '-2-0' end as `账龄`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
FROM
(
SELECT
        a.组别,
        a.催员ID,
        a.债务人ID,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b1.是否首通接通
FROM
        `分案还款` a
        LEFT JOIN `拨打数据` b ON a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`债务人ID` = b1.`债务人ID`
        WHERE a.组别 NOT IN ('cultivate','PKCC','PHCC','MXCC','THCC')) t
GROUP BY 1,2

union all

select t.`组别`,
       t.`账龄`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
from
(	
select
a.*,
assign.mission_log_create_at,
assign.assign_debtor_late_days,
assign.assign_asset_late_days,
b.拨打次数,
b.接通次数,
b1.是否首通接通,
r.`实收`,
case when r.`实收`>0 then '是' else '否' end as `是否还款`,
row_number() over(partition by a.asset_item_number order by assign.mission_log_create_at desc) as num

from
(			
select debtor_id as `债务人ID`,
color,late_days,
'辅助拨打IVR_逾期' as `组别`,
'1-7' as `账龄`,
create_at,
asset_item_number
-- select * 
from ods_fox_asset_ranse_log
where color in (
'overdue_asset_auto_call_experiment_aiRudder',
'overdue_asset_auto_call_experiment_wiz')
and create_at>="{0}"
and create_at<="{1}"
-- order by create_at desc
group by debtor_id,color,late_days,'辅助拨打IVR_逾期','1-7',create_at,asset_item_number
)a
LEFT JOIN `拨打数据` b ON a.`债务人ID` = b.`债务人ID`
LEFT JOIN 拨打首通接通 b1 ON a.`债务人ID` = b1.`债务人ID`
left join  ods_fox_asset bb
on (a.asset_item_number=bb.asset_item_number)
left join 
(select * from ods_fox_mission_log
where mission_log_operator = 'assign'
) assign on (assign.mission_log_asset_id=bb.asset_id and assign.mission_log_create_at<=a.create_at)
LEFT JOIN (
        SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
        FROM
                ods_fox_collect_recovery cr 
        WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
-- where a.asset_item_number='P2023071949755154997'
order by mission_log_create_at desc

)t
where t.num=1
group by t.`组别`,
         t.`账龄`
)aaa
order by aaa.`账龄`
""".format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql




def 团队过程指标IVR_泰国(first_day, yesterday):
    sql = """
WITH `分案还款` AS (
SELECT
        assign.debtor_id AS '债务人ID',
        assign.mission_group_name AS '组别',
        assign.assigned_sys_user_id AS '催员ID',
        assign.director_name '主管',
        IF(SUM(r.实收) > 0,1,0) `是否还款`
FROM
        ods_fox_mission_log assign
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id 
        LEFT JOIN (
        SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
        FROM
                ods_fox_collect_recovery cr 
        WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        WHERE 1 = 1 
        AND assign.mission_log_operator = 'assign' 
        AND (assign.mission_group_name like '%IVR%' OR assign.mission_group_name like '%94%')

        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
GROUP BY 1,2,3,4),

拨打数据 AS (
                                SELECT debtor_id '债务人ID',count(1) 拨打次数,sum(if(talk_duration>0,1,0)) 接通次数
                                from fox_ods.ods_fox_ivr_call_history
                                where call_start_at >="{0}"
                                AND call_start_at <="{1}"
                                GROUP BY debtor_id),
拨打首通接通 AS(
SELECT DISTINCT
        debtor_id AS '债务人ID',
        IF((FIRST_VALUE(talk_duration) OVER(PARTITION BY debtor_id ORDER BY call_start_at ASC)) > 0 ,1,0) '是否首通接通'
FROM
        fox_ods.ods_fox_ivr_call_history ich
WHERE
        call_start_at >= "{0}"
        AND call_start_at <= "{1}")



select *
from 
(
SELECT
        t.组别,
		    case when t.组别='IVR_PreRemind' then '-2-0' end as `账龄`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
FROM
(
SELECT
        a.组别,
        a.催员ID,
        a.债务人ID,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b1.是否首通接通
FROM
        `分案还款` a
        LEFT JOIN `拨打数据` b ON a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`债务人ID` = b1.`债务人ID`
        WHERE a.组别 NOT IN ('cultivate','PKCC','PHCC','MXCC','THCC')) t
GROUP BY 1,2

union all

select t.`组别`,
       t.`账龄`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
from
(	
select
a.*,
assign.mission_log_create_at,
assign.assign_debtor_late_days,
assign.assign_asset_late_days,
b.拨打次数,
b.接通次数,
b1.是否首通接通,
r.`实收`,
case when r.`实收`>0 then '是' else '否' end as `是否还款`,
row_number() over(partition by a.asset_item_number order by assign.mission_log_create_at desc) as num

from
(			
select debtor_id as `债务人ID`,
color,
late_days,
case when color='overdue_asset_auto_call_experiment_aiRudder'  then '辅助拨打IVR_逾期'
     when color='deadline_asset_auto_call_experiment_aiRudder' then '辅助拨打IVR_未逾期' end as `组别`,
case when color='overdue_asset_auto_call_experiment_aiRudder'  then '1-7'
     when color='deadline_asset_auto_call_experiment_aiRudder' then '-2-0' end as `账龄`,
create_at,
asset_item_number
from ods_fox_asset_ranse_log
where color in ('overdue_asset_auto_call_experiment_aiRudder','deadline_asset_auto_call_experiment_aiRudder')
and create_at>="{0}"
and create_at<="{1}"
-- order by create_at desc
group by debtor_id,color,late_days,
case when color='overdue_asset_auto_call_experiment_aiRudder'  then '辅助拨打IVR_逾期'
     when color='deadline_asset_auto_call_experiment_aiRudder' then '辅助拨打IVR_未逾期' end,
case when color='overdue_asset_auto_call_experiment_aiRudder'  then '1-7'
     when color='deadline_asset_auto_call_experiment_aiRudder' then '-2-0' end,
		 create_at,
		 asset_item_number
)a
LEFT JOIN `拨打数据` b ON a.`债务人ID` = b.`债务人ID`
LEFT JOIN 拨打首通接通 b1 ON a.`债务人ID` = b1.`债务人ID`
left join  ods_fox_asset bb
on (a.asset_item_number=bb.asset_item_number)
left join 
(select * from ods_fox_mission_log
where mission_log_operator = 'assign'
) assign on (assign.mission_log_asset_id=bb.asset_id and assign.mission_log_create_at<=a.create_at)
LEFT JOIN (
        SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
        FROM
                ods_fox_collect_recovery cr 
        WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
-- where a.asset_item_number='P2023071949755154997'
order by mission_log_create_at desc

)t
where t.num=1
group by t.`组别`,
         t.`账龄`
)aaa
order by aaa.`账龄`
""".format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql




def 团队过程指标IVR_墨西哥(first_day, yesterday):
    sql = """
WITH `分案还款` AS (
SELECT
        assign.debtor_id AS '债务人ID',
        assign.mission_group_name AS '组别',
        assign.assigned_sys_user_id AS '催员ID',
        assign.director_name '主管',
        IF(SUM(r.实收) > 0,1,0) `是否还款`


FROM
        ods_fox_mission_log assign
        LEFT JOIN ods_fox_mission_log_unassign_reason mlur ON mlur.assign_mission_log_id = assign.mission_log_id 
        LEFT JOIN (
        SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
        FROM
                ods_fox_collect_recovery cr 
        WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
        WHERE 1 = 1 
        AND assign.mission_log_operator = 'assign' 
        AND (assign.mission_group_name like '%IVR%' OR assign.mission_group_name like '%94%')

        AND (assign.mission_log_create_at >= "{0}" AND assign.mission_log_create_at <= "{1}")
GROUP BY 1,2,3,4),

拨打数据 AS (
                                SELECT debtor_id '债务人ID',count(1) 拨打次数,sum(if(talk_duration>0,1,0)) 接通次数
                                from fox_ods.ods_fox_ivr_call_history
                                where call_start_at >="{0}"
                                AND call_start_at <="{1}"
                                GROUP BY debtor_id),
拨打首通接通 AS(
SELECT DISTINCT
        debtor_id AS '债务人ID',
        IF((FIRST_VALUE(talk_duration) OVER(PARTITION BY debtor_id ORDER BY call_start_at ASC)) > 0 ,1,0) '是否首通接通'
FROM
        fox_ods.ods_fox_ivr_call_history ich
WHERE
call_start_at >= "{0}"
AND call_start_at <= "{1}")



select *
from 
(
SELECT
        t.组别,
		case when t.组别 in ('IVR-WIZ','IVR') then '-2-0'
		     when t.组别 in ('IVR-D1','WIZ-IVR-D1') then '1-1' end as `账龄`,-------需要定时更改
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
FROM
(
SELECT
        a.组别,
        a.催员ID,
        a.债务人ID,
        a.是否还款,
        b.拨打次数,
        b.接通次数,
        b1.是否首通接通
FROM
        `分案还款` a
        LEFT JOIN `拨打数据` b ON a.`债务人ID` = b.`债务人ID`
        LEFT JOIN 拨打首通接通 b1 ON a.`债务人ID` = b1.`债务人ID`
        WHERE a.组别 NOT IN ('cultivate','PKCC','PHCC','MXCC','THCC')) t
GROUP BY 1,2

union all

select t.`组别`,
       t.`账龄`,
        ROUND(COUNT(IF(t.拨打次数 > 0,1,NULL))/ COUNT(t.债务人ID),4) `拨打覆盖率`,
        ROUND(SUM(t.拨打次数) / COUNT(t.债务人ID),4) `案均拨打次数`,
        ROUND(COUNT(IF(t.是否首通接通 > 0,1,NULL)) / COUNT(t.债务人ID),4) `首通接通率`,
        ROUND(COUNT(IF(t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `案件接通率`,
        ROUND(COUNT(IF(t.是否还款 > 0 AND t.接通次数 > 0,1,NULL)) / COUNT(t.债务人ID),4) `可联还款率`
from
(	
select
a.*,
assign.mission_log_create_at,
assign.assign_debtor_late_days,
assign.assign_asset_late_days,
b.拨打次数,
b.接通次数,
b1.是否首通接通,
r.`实收`,
case when r.`实收`>0 then '是' else '否' end as `是否还款`,
row_number() over(partition by a.asset_item_number order by assign.mission_log_create_at desc) as num

from
(			
select debtor_id as `债务人ID`,
color,
late_days,
case when color='overdue_asset_auto_call_experiment_aiRudder'  then '辅助拨打IVR_逾期'
     when color='overdue_asset_auto_call_experiment_wiz' then '辅助拨打IVR_逾期' end as `组别`,
case when color='overdue_asset_auto_call_experiment_aiRudder'  then '1-7'
     when color='overdue_asset_auto_call_experiment_wiz' then '1-7' end as `账龄`,
create_at,
asset_item_number
from ods_fox_asset_ranse_log
where color in ('overdue_asset_auto_call_experiment_aiRudder','overdue_asset_auto_call_experiment_wiz')
and create_at>="{0}"
and create_at<="{1}"
-- order by create_at desc
group by debtor_id,color,late_days,
case when color='overdue_asset_auto_call_experiment_aiRudder'  then '辅助拨打IVR_逾期'
     when color='overdue_asset_auto_call_experiment_wiz' then '辅助拨打IVR_逾期' end,
case when color='overdue_asset_auto_call_experiment_aiRudder'  then '1-7'
     when color='overdue_asset_auto_call_experiment_wiz' then '1-7' end,
		 create_at,
		 asset_item_number
)a
LEFT JOIN `拨打数据` b ON a.`债务人ID` = b.`债务人ID`
LEFT JOIN 拨打首通接通 b1 ON a.`债务人ID` = b1.`债务人ID`
left join  ods_fox_asset bb
on (a.asset_item_number=bb.asset_item_number)
left join 
(select * from ods_fox_mission_log
where mission_log_operator = 'assign'
) assign on (assign.mission_log_asset_id=bb.asset_id and assign.mission_log_create_at<=a.create_at)
LEFT JOIN (
        SELECT
                cr.mission_log_id,
                SUM(cr.repaid_total_amount)/100 `实收`
        FROM
                ods_fox_collect_recovery cr 
        WHERE
                cr.repay_date >= "{0}" AND  cr.repay_date <= "{1}"
        GROUP BY 1) r ON assign.mission_log_id = r.mission_log_id
-- where a.asset_item_number='P2023071949755154997'
order by mission_log_create_at desc

)t
where t.num=1
group by t.`组别`,
         t.`账龄`
)aaa
order by aaa.`账龄`
""".format(first_day + " 00:00:00", yesterday + " 23:59:59")
    return sql


