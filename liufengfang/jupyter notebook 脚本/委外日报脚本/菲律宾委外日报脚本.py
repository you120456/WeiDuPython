

import pandas as pd
from sshtunnel import SSHTunnelForwarder
import pymysql
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import excel_beautify as mh
from send_email import send_bulk_emails_with_attachment

# 数据结束日期
data_end_date = datetime.today().date() - timedelta(days=1)
# 数据开始日期
data_start_date = (data_end_date - relativedelta(months=1)).replace(day=26) if data_end_date.day < 26 else data_end_date.replace(day=26)


# 连接菲律宾Bi库 start #
# 配置ssh端口转发 start #

tunnel = SSHTunnelForwarder(
    ('161.117.0.173', 22),
    ssh_username='liufengfang',
    ssh_password='liufengfang',
    # ssh_pkey = r'C:\Users\Administrator\.ssh\id_rsa',
    # ssh_pkey = '/root/.ssh/id_rsa',
    ssh_pkey = r'/root/.ssh/id_rsa_liu',
    remote_bind_address=('rr-t4n1o9w69b029nrh5.mysql.singapore.rds.aliyuncs.com',3306),
    local_bind_address=('127.0.0.1', 0))
tunnel.start()
# 配置ssh端口转发 end #
# 获取连接 start #
connection = pymysql.connect(
    host='127.0.0.1',
    port=tunnel.local_bind_port,
    user='liufengfang',
    password='KVmcFsg@VfgA',
    database='bi'
)
# 获取连接 end #



bi_performance = '''
SELECT
	DATE(bp.stat_time) `统计日期`,
	bp.user_id `员工ID`,
	bp.`name` `指标名称`,
	bp.`value` `指标值`
FROM
	bi_performance bp 
WHERE
	(bp.stat_time BETWEEN '{0}' AND '{1}')
'''


bi_user_organization = '''
SELECT
	DATE(stat_time) `架构日期`,
	user_id `员工ID`,
	SUBSTRING_INDEX( `asset_group_name`, ',',- 1 ) '组别',
	SUBSTRING_INDEX( SUBSTRING_INDEX( `parent_user_names`, ',', 2 ), ',',-1 ) '主管',
	parent_user_name '组长',
    NAME
FROM
	`bi_user_organization` 
WHERE
	stat_time BETWEEN '{0}' AND '{1}' AND asset_group_name IS NOT NULL'''



# 业绩数据
df_perfomance = pd.read_sql_query(bi_performance.format(str(data_start_date)+' 00:00:00',str(data_end_date)+' 23:59:59'),connection)
# 月末架构数据取月末T-1天的
df_uo = pd.read_sql_query(bi_user_organization.format(str(data_start_date)+' 00:00:00',
                                                        str(data_end_date - timedelta(days = 1) if data_end_date.day == 25 else data_end_date)+' 23:59:59'),
                                                        connection)

# 取截至数据日最新的架构
df_uo_new = df_uo
# 转换成日期
df_uo_new["架构日期"] = pd.to_datetime(df_uo_new["架构日期"])
# 分组求最新架构
df_uo_latest = df_uo_new.loc[df_uo_new.groupby('员工ID')['架构日期'].idxmax()]
# 队列值设置
group_a_new = "Group A New"
# 根据业组设置队列值
df_uo_latest["队列"] = np.where(df_uo_latest['组别'].str.contains("A New"),group_a_new,"该队列无委外")
# 剔除无委外队列的数据
df_uo_latest = df_uo_latest[df_uo_latest["队列"] != "该队列无委外"]

# 处理业绩数据
df_perfomance_pivot = df_perfomance.pivot(index=["统计日期","员工ID"],columns="指标名称",values="指标值").reset_index()
# 按 userID 分组，并获取每个组中日期最大的行的索引
idx_max_new = df_perfomance_pivot.groupby('员工ID')['统计日期'].idxmax()
# 选择累计最新数据
df_perfomance_max_new = df_perfomance_pivot.loc[idx_max_new]
# 选择数据列
df_perfomance_result = df_perfomance_max_new[["员工ID","first_online_day","online_days","avg_call_number","avg_call_time","month_mission_principal_amount","month_recovery_principal_amount","month_recovery_total_amount","month_recovery_rate","leave_day"]]


# 合并数据
result_peformance = pd.merge(df_uo_latest,df_perfomance_result,how="left",on="员工ID")
# 剔除上线天数为0的人
co = ["online_days"]
tichu = result_peformance[co].isna() | (result_peformance[co] == 0) | (result_peformance[co] == '0')
result_peformance = result_peformance[ ~ tichu.any(axis = 1)]

# 设置催回率排名
result_peformance["催回率排名"] = result_peformance.groupby("队列")["month_recovery_rate"].rank(ascending=False)

# 设置排名区间
result_peformance["队列最大排名"] = result_peformance.groupby("队列")["催回率排名"].transform("max")
# result_peformance.to_excel("./测试.xlsx")

# 排名区间算法
def calculation_interval(row):
    max_Ranking = row["队列最大排名"]
    if row['催回率排名'] <= round(max_Ranking * 0.05,2):
        return "Top5%"
    elif row['催回率排名'] <= round(max_Ranking * 0.25,2):
        return "5%-25%"
    elif row['催回率排名'] <= round(max_Ranking * 0.5,2):
        return "25%-50%"
    elif row['催回率排名'] <= round(max_Ranking * 0.7,2):
        return "50%-70%"
    elif row['催回率排名'] <= round(max_Ranking * 0.9,2):
        return "70%-90%"
    else:
        return "bottom10%"

# 计算排名区间赋值
result_peformance["催回率排名区间"] = result_peformance.apply(calculation_interval,axis=1)

# 选取列数据导出excle
result = result_peformance[["队列","组别","主管","组长","NAME","first_online_day","online_days","avg_call_number","avg_call_time","month_mission_principal_amount","month_recovery_principal_amount","month_recovery_total_amount","month_recovery_rate","催回率排名","催回率排名区间"]]


# 修改列名称
result.rename(columns={ '组别': '业务组别',
                        'NAME': '催员',
                        'first_online_day': '首次上线日期',
                        'online_days': '当月上线天数',
                        'avg_call_number': '日均拨打次数',
                        'avg_call_time': '日均通话时长',
                        'month_mission_principal_amount': '月累计分案本金',
                        'month_recovery_principal_amount': '月累计回款本金',
                        'month_recovery_total_amount': '月累计总实收',
                        'month_recovery_rate': '月累计催回率'
                       },inplace=True)


# 转换数据类型
result["日均拨打次数"] = pd.to_numeric(result["日均拨打次数"], errors='coerce')
result["日均通话时长"] = pd.to_numeric(result["日均通话时长"], errors='coerce')
result["当月上线天数"] = pd.to_numeric(result["当月上线天数"], errors='coerce')
result["月累计分案本金"] = pd.to_numeric(result["月累计分案本金"], errors='coerce')
result["月累计回款本金"] = pd.to_numeric(result["月累计回款本金"], errors='coerce')
result["月累计总实收"] = pd.to_numeric(result["月累计总实收"], errors='coerce')
result["月累计催回率"] = pd.to_numeric(result["月累计催回率"], errors='coerce')

result[["日均拨打次数", "日均通话时长"]] = result[["日均拨打次数", "日均通话时长"]].round(2)
result["月累计催回率"] = result["月累计催回率"].round(4)
result[["月累计分案本金", "月累计回款本金","月累计总实收"]] = result[["月累计分案本金", "月累计回款本金","月累计总实收"]] / 100

# 排序导出excle
result = result.sort_values(by=["队列","催回率排名"],ascending=[True,True])
result.to_excel('/usr/local/python_script/philippines_daily_outsourcing_ranking/xlsxFile/菲律宾委外排名数据{0}.xlsx'.format(data_end_date),index=False)
mh.beautify_excel('/usr/local/python_script/philippines_daily_outsourcing_ranking/xlsxFile/菲律宾委外排名数据{0}.xlsx'.format(data_end_date))


# 发送邮件
sender_email = 'liufengfang@weidu.ac.cn'
sender_password = 'pp6M89B5RJTdwRwD'
recipients = ['lichongqing@weidu.ac.cn', 'liufengfang@weidu.ac.cn']
subject = '【菲律宾委外排名数据-{0}】'.format(str(data_end_date))
body =r"""<!DOCTYPE html><html><head><style>.indented {margin-left: 20px;}</style></head><body><p>各位好！</p> <p>&nbsp;&nbsp;&nbsp;&nbsp;附件是菲律宾委外排名数据，请查收！谢谢！</p></body></html>"""
attachment_path = '/usr/local/python_script/philippines_daily_outsourcing_ranking/xlsxFile/菲律宾委外排名数据{0}.xlsx'.format(data_end_date)
send_bulk_emails_with_attachment(sender_email, sender_password, recipients, subject, body, attachment_path)

