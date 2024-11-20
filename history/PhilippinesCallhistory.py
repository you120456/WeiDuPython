#!/usr/bin/env python
# coding: utf-8

# ##### -*- coding: utf-8 -*-
# Created : 2023/09/08
# Update : 2023/09/08
# author: yeyuhao
#

import config
import pymysql
import mysql.connector
import datetime
import pandas as pd
import warnings
from sshtunnel import SSHTunnelForwarder
warnings.filterwarnings("ignore")
print("'菲律宾客服外呼明细'\n开始执行!", datetime.datetime.now())

time_end = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
time_start = time_end - datetime.timedelta(7)
print('数据时间范围{}--->{}'.format(str(time_start)[:10],
                                str(time_end - datetime.timedelta(1))[:10]))

### 菲律宾数据

aduit_Philippines_server = SSHTunnelForwarder(
    ('161.117.0.173', 22),  # 这里写入B 跳板机IP、端口
    ssh_username='wd_bi',  # 跳板机 用户名
    ssh_password='wd_bi',  # 跳板机 密码
    ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
    remote_bind_address=('rr-t4n3ss9ht7onwsk39.mysql.singapore.rds.aliyuncs.com', 3306),  # 这里写入 C数据库的 IP、端口号
    # local_bind_address=('127.0.0.1', 8080)
)
aduit_Philippines_server.start()
aduit_Philippines_engine = pymysql.connect(
    host='127.0.0.1',  # 只能写 127.0.0.1，这是固定的，不可更改
    port=aduit_Philippines_server.local_bind_port,
    user='liufengfang',  # C数据库 用户名
    password='s78kWRVLeP#L',  # C数据库 密码
    db='audit',  # 填写需要连接的数据库名
    charset='utf8',
)

# 通时计算
sql_call_time_Philippines = '''
                    SELECT
                        '菲律宾' as  "国家",
                        DATE( ch.call_at ) AS '日期',
                        ch.call_at  as "通话时间",
                        ch.enc_debtor_phone_number as "被叫号码（密文）",
                        ch.dunner_phone_number as '主叫号码',
                        ce.dunner_name as '催员',
                        ch.dial_time AS '振铃时长',
                        ch.call_time '通话时长'

                    FROM
                        call_history ch
                        JOIN call_history_extend ce ON ch.id = ce.source_id 
                    WHERE
                        ch.call_at >= '{}'
                        and ch.call_at < '{}'
                        AND ch.dunner_phone_number in ('63218','63004','63361','63344')
                        and ch.call_channel = 1
                '''.format(str(time_start), str(time_end))
call_time_Philippines_df = pd.read_sql(sql_call_time_Philippines, aduit_Philippines_engine)
aduit_Philippines_engine.close()
aduit_Philippines_server.stop()

# =============================================================================
# 数据输出
# =============================================================================
now = datetime.datetime.now()
print("开始输出", datetime.datetime.now())
out_path = r"C:\自动化数据存放" + "\\"
writer1 = pd.ExcelWriter(out_path + "菲律宾客服外呼通话明细.xlsx", engine='xlsxwriter')
call_time_Philippines_df.to_excel(writer1, sheet_name='菲律宾', index=False)
writer1.save()  # 此语句不可少，否则本地文件未保存


# =============================================================================
# 邮件发送
# =============================================================================

import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart

error = '\n异常内容：'

##### 配置区  #####
mail_host = 'smtp.exmail.qq.com'
mail_port = '465'  # Linux平台上面发
receiver_lst = ['yeyuhao@weidu.ac.cn', "yuzheng@weidu.ac.cn", 'huangwei@weidu.ac.cn']

# 发件人邮箱账号
login_sender = 'shwd_operation@weidu.ac.cn'
# 发件人邮箱授权码而不是邮箱密码，授权码由邮箱官网可设置生成
login_pass = config.login_pass
# 邮箱文本内容

mail_msg = '''
    本数据统计菲律宾客服外呼通话明细，时间范围{}--->{}。
    \n数据请见附件！
    '''.format(str(time_start)[:10], str(time_end - datetime.timedelta(1))[:10])
# 发送者
sendName = "数据组"
# 接收者
resName = ""
# 邮箱正文标题
title = "菲律宾客服外呼通话明细:" + str(time_end)[:10]

######### end  ##########

msg = MIMEMultipart()
msg['From'] = formataddr([sendName, login_sender])
# 邮件的标题
msg['Subject'] = title
msg.attach(MIMEText(mail_msg, 'html', 'utf-8'))   # 添加正文

att1 = MIMEText(open(r'C:\自动化数据存放\菲律宾客服外呼通话明细.xlsx', 'rb').read(), 'base64', 'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att1["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att1.add_header("Content-Disposition", "attachment", filename=("gbk", "", "菲律宾客服外呼通话明细.xlsx"))
# att1["Content-Disposition"] = 'attachment;filename="data.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att1)  # 将附件添加到邮件内容当中

# 服务器
server = smtplib.SMTP_SSL(mail_host, mail_port)
server.login(login_sender, login_pass)
server.sendmail(login_sender, receiver_lst, msg.as_string())
print("已发送到:\n" + ';\n'.join(receiver_lst) + "的邮箱中！")
server.quit()