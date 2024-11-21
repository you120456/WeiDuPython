import pandas as pd
import sc
sql = """
SELECT a.*,TIMESTAMPDIFF(SECOND,拨打时间,next_dial_time)/60 拨打间隔,if(DAY(拨打时间)=DAY(next_dial_time),'否','是') 是否夸天
from
(SELECT
ch.call_at 拨打时间,
'自营' as "委外自营",
-- IF(suy.role_id ='9bd3e2a1768a4c1cbc79af4e99f59447' ,"委外","自营") as "委外自营",
se.dunner_asset_group_name 业务组,
se.dunner_leader_name 组长,
se.dunner_name 员工,
se.dunner_manager_name 主管,
ch.phone_line 线路号,
ch.`call_time` 接通时长 ,
ch.`dial_time` 拨打时长, 
LEAD( ch.call_at ) OVER( PARTITION BY se.dunner_name ORDER BY ch.call_at) AS next_dial_time
FROM
`ods_audit_call_history` ch
JOIN `ods_audit_call_history_extend` se ON ch.id = se.source_id 
WHERE
ch.`call_at` >= (CURDATE() - INTERVAL 1 DAY)
AND ch.`call_at` < CURDATE()
-- date(ch.`call_at`) = (CURDATE() - INTERVAL 2 DAY)
)a
ORDER BY 员工,拨打时间
"""
df1 = pd.read_sql(sql,sc.tg())
print(df1)