import pandas as pd
import yaml
import os
import conn
import assembling_data as public



if __name__ == "__main__":
    
    country='墨西哥'

    # 设置日报查询范围 查询日期初始化
    end_day = str(pd.Timestamp.now().date()-pd.Timedelta(days=1))+" 23:59:59"
    start_day = str((pd.Timestamp.now()- pd.Timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')) +' 00:00:00'
    loan_end_day = end_day

    # 数据保存路径
    path = './{1}电销/{1}电销日报({0}).xlsx'.format(pd.to_datetime(end_day).date(),country)

    # 设置工作目录为脚本所在目录
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    # 读取配置文件获取电销业务组
    with open('config.yaml','r',encoding='UTF-8') as file:
        config = yaml.safe_load(file)
    group = ",".join(f"'{cf}'" for cf in config['mexico']['group'])

    # 获取数据数据源
    conn_link = conn.conn_mx()
    performance_df,attendance_df,process_df,dimission_date_df,first_online_df,required_attendance_df,current_month_first_scheduled_df,organization_df,assign_organization_df,no_count_churn_df = public.get_data(start_day,end_day,loan_end_day,group,conn_link)

    # 获取清洗架构后的数据
    get_performance_df,get_process_df,get_attendance_df = public.get_cleaning_data(performance_df,attendance_df,process_df,organization_df,first_online_df)

    #员工数据
    员工数据_result = public.get_staff_result(get_performance_df,get_process_df,get_attendance_df,dimission_date_df,first_online_df,required_attendance_df,organization_df,no_count_churn_df,end_day,current_month_first_scheduled_df,if_exit_application=0)

    # 组长数据
    组长业绩数据_result = public.get_tl_result(get_attendance_df,get_performance_df,no_count_churn_df,员工数据_result,if_applied=0)

    # 主管业绩数据
    主管业绩数据_result = public.get_dr_result(get_attendance_df,get_performance_df,员工数据_result,组长业绩数据_result,no_count_churn_df)

    # 最终美化发送邮件
    # 邮件接收人list
    # recipients = config['domestic']['email']
    recipients = ['liufengfang@weidu.ac.cn','xumingming@weidu.ac.cn']
    # 邮件主题
    subject = '【{1}电销日报数据-{0}】'.format(pd.to_datetime(end_day).date(),country)
    # 邮件内容
    body =r"""<!DOCTYPE html><html><head><style>.indented {margin-left: 20px;}</style></head><body><p>各位好！</p> <p>&nbsp;&nbsp;&nbsp;&nbsp;附件是墨西哥电销日报数据，请查收！谢谢！</p></body></html>"""
    public.beautify_send(员工数据_result,组长业绩数据_result,主管业绩数据_result,path,recipients,subject,body)