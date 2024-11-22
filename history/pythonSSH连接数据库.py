#!/usr/bin/env python
# coding: utf-8

# ##### -*- coding: utf-8 -*-
# Created : 2023/07/10
# Update : 2023/07/10
# author: yeyuhao
#

# In[1]:


import pymysql
import mysql.connector
import datetime
import pandas as pd
import warnings
from sshtunnel import SSHTunnelForwarder
warnings.filterwarnings("ignore")
print("'稽核数据：每周各国入催量及通时数据'\n开始执行!", datetime.datetime.now())
now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
month_start = datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
time_end = now - datetime.timedelta(0)
time_start = now - datetime.timedelta(7)
print('数据时间范围{}--->{}'.format(str(time_start)[:10],
                                str(time_end - datetime.timedelta(1))[:10]))



#通过SSH跳板来连接到数据库
fox_Mexico_server = SSHTunnelForwarder(
    ('47.88.48.246', 22),       #这里写入B 跳板机IP、端口
    ssh_username='wd_bi',    #跳板机 用户名
    ssh_password='wd_bi',    #跳板机 密码
    ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
    remote_bind_address=('rr-rj93kxoj02916kpp3.mysql.rds.aliyuncs.com', 3306),   #这里写入 C数据库的 IP、端口号
     # local_bind_address=('127.0.0.1', 8080)
     )
fox_Mexico_server.start()
fox_Mexico_engine = pymysql.connect(
    host='127.0.0.1',       #只能写 127.0.0.1，这是固定的，不可更改
    port=fox_Mexico_server.local_bind_port,
    user='xumingming',      #C数据库 用户名
    password='l%!yoPdo$LhdnYX2',   #C数据库 密码
    db='arcticfox',       #填写需要连接的数据库名
    charset='utf8',

)
aduit_Mexico_server = SSHTunnelForwarder(
    ('47.88.48.246', 22),       #这里写入B 跳板机IP、端口
    ssh_username='wd_bi',    #跳板机 用户名
    ssh_password='wd_bi',    #跳板机 密码
    ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
    remote_bind_address=('rr-rj95k0z3i9505yn3o.mysql.rds.aliyuncs.com', 3306),    #这里写入 C数据库的 IP、端口号
     # local_bind_address=('127.0.0.1', 8080)
     )
aduit_Mexico_server.start()
aduit_Mexico_engine = pymysql.connect(
    host='127.0.0.1',       #只能写 127.0.0.1，这是固定的，不可更改
    port=aduit_Mexico_server.local_bind_port,
    user='xumingming',      #C数据库 用户名
    password='Xvx4p1gLEROvjFTy',   #C数据库 密码
    db='audit',       #填写需要连接的数据库名
    charset='utf8',
)

# =============================================================================
# 字段计算
# =============================================================================
sql_input_Mexico = '''
            SELECT
                '墨西哥'  as '国家',
                count( DISTINCT da.debtor_id ) 户数 
            FROM
                `collect_recovery` cr
                JOIN asset a ON cr.asset_id = a.asset_id
                JOIN debtor_asset da ON a.asset_id = da.asset_id 
            WHERE
                cr.`batch_num` IS NULL 
                AND cr.`late_days` = 1 
                AND cr.`create_at`>='{}'
                    AND cr.`create_at`< '{}'
        '''.format(str(time_start), str(time_end))
input_Mexico_df = pd.read_sql(sql_input_Mexico, fox_Mexico_engine)
# 通时计算
sql_call_time_Mexico = '''
            SELECT
                '墨西哥'  as '国家',
                count(ch.call_time ) as '通话时长/s'
                FROM
                    call_history ch
                WHERE
                    ch.`call_at` >= '{}'
                    and ch.`call_at`< '{}'

                    AND ch.call_channel = 1 -- 呼出
                    AND ch.call_time >0
                '''.format(str(time_start), str(time_end))
call_time_Mexico_df = pd.read_sql(sql_call_time_Mexico, aduit_Mexico_engine)

aduit_Mexico_engine.close()
aduit_Mexico_server.stop()
fox_Mexico_engine.close()
fox_Mexico_server.stop()
Mexico_df = pd.merge(input_Mexico_df,call_time_Mexico_df ,on = '国家')

print(Mexico_df)