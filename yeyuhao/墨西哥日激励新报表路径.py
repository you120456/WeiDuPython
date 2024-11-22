import pymysql
#import mysql.connector
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import datetime
import excelFormat

# 连接fox
#通过SSH跳板来连接到数据库
fox_Mexico_server = SSHTunnelForwarder(
    ('mx-dataprod.mxgbus.com', 36000),  # 这里写入B 跳板机IP、端口
    ssh_username='yeyuhao',  # 跳板机 用户名
    ssh_password='yeyuhao',  # 跳板机 密码
    ssh_pkey=r'D:\唯渡科技\国内\秘钥文件对应\叶雨豪\id_rsa',
    remote_bind_address=('172.20.220.164', 9030),  # 这里写入 C数据库的 IP、端口号
     # local_bind_address=('127.0.0.1', 8080)
     )
fox_Mexico_server.start()

fox_Mexico_engine = pymysql.connect(
    host='127.0.0.1',       #只能写 127.0.0.1，这是固定的，不可更改
    port=fox_Mexico_server.local_bind_port,
    user='u_yeyuhao',      #C数据库 用户名
    password='W6RvuiQhrwDk',   #C数据库 密码
    db='fox_ods',       #填写需要连接的数据库名
    charset='utf8',
)


def jili(fox_Mexico_engine,  a):
    # 字段计算
    fox_input_Mexico = '''
                 SELECT 
                user_id,
                user_name as 'name',
                group_leader_id as 'parent_user_id',
                group_leader_name as 'parent_user_name',
                manager_id as '主管id',
                manager_name as '主管',
                asset_group_name as '组别',
                call_count as '总外呼量',
                talk_duration as '当日通时',
                today_new_assign_repaid_rate/100 as 'D1催回率'          
                FROM fox_dw.`dws_fox_collect_workload_user_day`
                where pt_date = '{}'
            '''.format(a)

    bi_Mexico_df = pd.read_sql(fox_input_Mexico, fox_Mexico_engine)
    # bi_Mexico_df.count()

    onlineday_Mexico = '''
    SELECT
     user_id,
     sum(attendance_status) as '上线天数'
    FROM
        ods_fox_collect_attendance_dtl
        WHERE date(work_day)>= (SELECT min(work_day) from ods_fox_collect_attendance_dtl)
        and  date(work_day)<='{}'
    GROUP BY 1
            '''.format(a)

    onlineday_Mexico_df = pd.read_sql(onlineday_Mexico, fox_Mexico_engine)

    return bi_Mexico_df, onlineday_Mexico_df
    # onlineday_Mexico_df.count()
    # .to_excel(r'C:\Users\Administrator\Desktop\bi_Mexico.xlsx',index=False)

# 定义奖励函数
def reward_function(row):
    if row['组别'] == 'P2':
        if row['D1催回率'] > 0.45:
            return 150
        elif row['D1催回率'] > 0.38:
            return 50
        else:
            return 0
    elif row['组别'] == 'P1':
        if row['D1催回率'] > 0.56:
            return 150
        elif row['D1催回率'] > 0.45:
            return 50
        else:
            return 0
    elif row['组别'] == 'P0':
        if row['D1催回率'] > 0.77:
            return 150
        elif row['D1催回率'] > 0.70:
            return 50
        else:
            return 0
    elif row['组别'] == 'A-1':
        if row['D1催回率'] > 0.72:
            return 150
        elif row['D1催回率'] > 0.64:
            return 50
        else:
            return 0
    elif row['组别'] == 'A-2':
        if row['D1催回率'] > 0.81:
            return 150
        elif row['D1催回率'] > 0.69:
            return 50
        else:
            return 0
    elif row['组别'] == 'A-3':
        if row['D1催回率'] > 0.90:
            return 150
        elif row['D1催回率'] > 0.82:
            return 50
        else:
            return 0
    elif row['组别'] == 'B1':
        if row['D1催回率'] > 0.19:
            return 150
        elif row['D1催回率'] > 0.15:
            return 50
        else:
            return 0

def aa(bi_Mexico_df,onlineday_Mexico_df,a):
    # global df_tl,df_bonus
    match_df = pd.merge(bi_Mexico_df,onlineday_Mexico_df ,how='left',on = 'user_id')
    online_fit=match_df[match_df["上线天数"]>7]
    online_call_fit=online_fit[(online_fit["总外呼量"]>=250) & (online_fit["当日通时"]>=4200)]
    df=online_call_fit[['user_id','name','parent_user_id','parent_user_name','主管id','主管','组别','D1催回率']]
    df['bonus'] = df.apply(reward_function, axis=1)
    df_bonus=df[df["bonus"]>0]
    df_bonus.groupby("组别")["bonus"].count()
    df_bonus['日期']=a
    df_tl = df_bonus.groupby(["parent_user_id","parent_user_name","bonus"]).size().reset_index(name='次数')
    df_tl['组长激励标准']= df_tl['bonus'].apply(lambda x: 15 if x == 50 else 50)
    df_tl['组长激励金额']=df_tl['组长激励标准']*df_tl['次数']
    df_tl['日期'] = a
    return df_tl,df_bonus


# 邮件发送
def send_mail(path,data,title):
    import smtplib
    from email.mime.text import MIMEText
    from email.utils import formataddr
    from email.mime.multipart import MIMEMultipart

    error = '\n异常内容：'

    ##### 配置区  #####
    mail_host = 'smtp.exmail.qq.com'
    mail_port = '465'  # Linux平台上面发
    receiver_lst = ['yeyuhao@weidu.ac.cn' , 'cynthia@massgom.com', 'marco@massgom.com', 'steve@massgom.com', 'johnson@massgom.com', 'stankovic@massgom.com', 'zhusong@weidu.ac.cn', 'lizheng@weidu.ac.cn']


    # 发件人邮箱账号
    login_sender = 'shwd_operation@weidu.ac.cn'
    # 发件人邮箱授权码而不是邮箱密码，授权码由邮箱官网可设置生成
    login_pass = 'Shanghai1015'
    # 邮箱文本内容

    mail_msg = '''

    '''
    # 发送者
    sendName = "dataGroup"
    # 接收者
    resName = ""
    # 邮箱正文标题
    title = title

    ######### end  ##########

    msg = MIMEMultipart()
    msg['From'] = formataddr([sendName, login_sender])
    # 邮件的标题
    msg['Subject'] = title
    msg.attach(MIMEText(mail_msg, 'html', 'utf-8'))  # 添加正文
    att1 = MIMEText(open(path, 'rb').read(), 'base64',
                    'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
    att1["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
    att1.add_header("Content-Disposition", "attachment", filename=("gbk", "", data))
    # att1["Content-Disposition"] = 'attachment;filename="data.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
    msg.attach(att1)
    server = smtplib.SMTP_SSL(mail_host, mail_port)
    server.login(login_sender, login_pass)
    server.sendmail(login_sender, receiver_lst, msg.as_string())
    print("send:\n" + ';\n'.join(receiver_lst))
    server.quit()


today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
# print(today)
last_day = str(today - datetime.timedelta(1))[:10]
# print(last_day)
k= []
k.append(last_day)


# k = ['2024-11-16','2024-11-17','2024-11-18','2024-11-19','2024-11-20']

for a in k:
    df1,df2 = jili(fox_Mexico_engine,a)
    df_tl,df_bonus =aa(df1,df2,a)
    del df_bonus['主管id']
    del df_bonus['parent_user_id']
    del df_tl['parent_user_id']
    filename = f"墨西哥每日激励_催员组长_{a}.xlsx"
    save_path = r'D:\DailyReport\墨西哥\激励'
    full_path = f"{save_path}\\{filename}"
    writer = pd.ExcelWriter(full_path, engine='xlsxwriter')
    df_bonus.to_excel(writer, sheet_name='dunner_bonus', index=False)
    df_tl.to_excel(writer, sheet_name='tl_bonus', index=False)
    writer.close()
    excelFormat.beautify_excel(full_path)
    print("日期:{}成功写入excel".format(a))
    title = '墨西哥每日激励-'+a
    send_mail(full_path,filename,title)
    print("邮件发送成功!", datetime.datetime.now())


# df_ce = []
# df_tl = []
# for a in k:
#     df1_ce = pd.read_excel(
#             r"D:\DailyReport\墨西哥\激励\墨西哥每日激励_催员组长_{}.xlsx".format(a),sheet_name = 'dunner_bonus')
#     df_ce.append(df1_ce)
#     df1_tl = pd.read_excel(
#             r"D:\DailyReport\墨西哥\激励\墨西哥每日激励_催员组长_{}.xlsx".format(a),sheet_name = 'tl_bonus')
#     df_tl.append(df1_tl)
# df_ce = pd.concat(df_ce, ignore_index=True)
# df_tl = pd.concat(df_tl, ignore_index=True)
#
# # 激励总额汇总
# df_ce_sum = df_ce.groupby(by=['user_id'], as_index=False).agg({"bonus": "sum"})
# df_tl_sum = df_tl.groupby(by=['parent_user_id'], as_index=False).agg({"组长激励金额": "sum"})
# df_tl_sum.rename(columns={"组长激励金额": "bonus",'parent_user_id':'user_id'}, inplace=True)
# df_sum = pd.concat([df_tl_sum,df_ce_sum])
#
#
#
# no_sql = '''
# SELECT
#                 su.id as 'user_id',
#                 su.`no` as '工号'
#                 FROM ods_fox_sys_user su
# '''
# no_df = pd.read_sql(no_sql, fox_Mexico_engine)
#
# df_sum = pd.merge(df_sum, no_df, how='left', on='user_id')
#
# out_path = r"D:\DailyReport\墨西哥\激励" + "\\"
# writer1 = pd.ExcelWriter(
#     out_path + "激励汇总2-15.xlsx",engine='xlsxwriter')
# df_sum.to_excel(writer1, sheet_name='for HR', index=False)
# df_ce.to_excel(writer1, sheet_name='dunner_bonus', index=False)
# df_tl.to_excel(writer1, sheet_name='tl_bonus', index=False)
# writer1._save()  # 此语句不可少，否则本地文件未保存


