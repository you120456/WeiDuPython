# 数据库配置
# !/usr/bin/env python
# coding: utf-8

import pymysql
import mysql.connector
import datetime
import pandas as pd
import warnings
from sqlalchemy import create_engine
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from sshtunnel import SSHTunnelForwarder

# ****************************************************************
# 数据库连接配置
# ****************************************************************

import pymysql
import pandas as pd
import warnings
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
def indonesia_bd_engine_read(sql,
                user='u_zhangjinyou',
                password='YBnFwLCoElgf',
                database='fox_ods'):
    # 印尼数仓数据库查询函数
    server = SSHTunnelForwarder(
        ('data-pord.starklotus.com', 38001),  # 这里写入B 跳板机IP、端口
        ssh_username='Patton',  # 跳板机 用户名
        ssh_password='Patton',  # 跳板机 密码
        ssh_pkey=r'D:\唯渡科技\国内\国内秘钥文件',
        remote_bind_address=('192.168.25.206', 9030),  # 这里写入 C数据库的 IP、端口号
        # local_bind_address=('127.0.0.1', 8080)
    )
    server.start()
    engine = pymysql.connect(
        host='127.0.0.1',  # 只能写 127.0.0.1，这是固定的，不可更改
        port=server.local_bind_port,
        user=user,  # C数据库 用户名
        password=password,  # C数据库 密码
        db=database,  # 填写需要连接的数据库名
        charset='utf8',
    )
    temp_df = pd.read_sql(sql, engine)
    engine.close()
    server.stop()
    return temp_df




# ****************************************************************
# 公共邮箱配置
# ****************************************************************

##### 配置区  #####
mail_host = 'smtp.exmail.qq.com'
mail_port = '465'  # Linux平台上面发
# 发件人邮箱账号
login_sender = 'shwd_operation@weidu.ac.cn'
# 发件人邮箱授权码而不是邮箱密码，授权码由邮箱官网可设置生成
login_pass = 'Shanghai0615'