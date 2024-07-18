# 数据库配置
# !/usr/bin/env python
# coding: utf-8


import pymysql
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

# def fox_engine(sql,
#                user='wdbi',
#                password='C4csu3Gyg%E2k0Tv',
#                host='sh-cdb-2ihwt87c.sql.tencentcdb.com',
#                port=58575,
#                database='qsq_fox'):
#     # 国内fox数据库查询函数
#     engine = mysql.connector.connect(user=user, password=password
#                                      , host=host, port=port, database=database)
#     temp_df = pd.read_sql(sql, engine)
#     engine.close()
#     return temp_df

def fox_engine(sql,
               user='yeyuhao',
               password='xmI6KkSW#qqF',
               database='qsq_fox'):
    # 国内fox数据库查询函数
    server = SSHTunnelForwarder(
        ('212.64.111.115', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='yeyuhao',  # 跳板机 用户名
        ssh_password='yeyuhao',  # 跳板机 密码
        ssh_pkey=r'D:\唯渡科技\国内\秘钥文件对应\叶雨豪\id_rsa',
        remote_bind_address=('10.10.42.123', 3306),  # 这里写入 C数据库的 IP、端口号
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

def jihe_engine(sql,
                user='yeyuhao',
                password='#1wLSCPbixH2',
                database='jihe'):
    # 国内jihe数据库查询函数
    server = SSHTunnelForwarder(
        ('212.64.111.115', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='yeyuhao',  # 跳板机 用户名
        ssh_password='yeyuhao',  # 跳板机 密码
        ssh_pkey=r'D:\唯渡科技\国内\秘钥文件对应\叶雨豪\id_rsa',
        remote_bind_address=('10.10.42.8', 3306),  # 这里写入 C数据库的 IP、端口号
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

def china_bi(sql,
                user='yeyuhao',
                password='%CAPJvfoSzvU',
                database='bi'):
    # 国内jihe数据库查询函数
    server = SSHTunnelForwarder(
        ('212.64.111.115', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='yeyuhao',  # 跳板机 用户名
        ssh_password='yeyuhao',  # 跳板机 密码
        ssh_pkey=r'D:\唯渡科技\国内\秘钥文件对应\叶雨豪\id_rsa',
        remote_bind_address=('10.10.42.12', 3306),  # 这里写入 C数据库的 IP、端口号
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


def local_database(sql,base_name='qsq_fox'):
    host_data = '//root:weidu:001A@172.16.1.250:3306'
    engine = create_engine('mysql+pymysql:{}/{}?charset=utf8'.format(host_data, base_name))
    temp_df = pd.read_sql(sql, engine)

    return temp_df

def indonesia_audit_engine_read(sql,
               user='mystic',
               password='c@dRZLmSWPW6!gOL',
               database='audit'):
    # 菲律宾audit数据库查询函数
    server = SSHTunnelForwarder(
        ('data-pord.starklotus.com', 38002),  # 这里写入B 跳板机IP、端口
        ssh_username='mystic',  # 跳板机 用户名
        ssh_password='mystic',  # 跳板机 密码
        ssh_pkey=r'D:\唯渡科技\国内\秘钥文件对应\叶雨豪\id_rsa',
        remote_bind_address=('10.0.6.231', 3306),  # 这里写入 C数据库的 IP、端口号
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

def indonesia_fox_engine_read(sql,
               user='mystic',
               password='c@dRZLmSWPW6!gOL',
               database='arcticfox'):
    # 泰国fox数据库查询函数
    server = SSHTunnelForwarder(
        ('data-pord.starklotus.com', 38002),  # 这里写入B 跳板机IP、端口
        ssh_username='mystic',  # 跳板机 用户名
        ssh_password='mystic',  # 跳板机 密码
        ssh_pkey=r'D:\唯渡科技\国内\秘钥文件对应\叶雨豪\id_rsa',
        remote_bind_address=('10.0.0.39', 3306),  # 这里写入 C数据库的 IP、端口号
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


def indonesia_bi_engine_read(sql,
               user='mystic',
               password='c@dRZLmSWPW6!gOL',
               database='bi'):
    # 印尼fox数据库查询函数
    server = SSHTunnelForwarder(
        ('data-pord.starklotus.com', 38002),  # 这里写入B 跳板机IP、端口
        ssh_username='mystic',  # 跳板机 用户名
        ssh_password='mystic',  # 跳板机 密码
        ssh_pkey=r'D:\唯渡科技\国内\秘钥文件对应\叶雨豪\id_rsa',
        remote_bind_address=('10.0.10.132', 3306),  # 这里写入 C数据库的 IP、端口号
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




def indonesia_fox_dw_read(sql,
               user='u_yeyuhao',
               password='W6RvuiQhrwDk',
               database='fox_dw'):
    # 印尼fox数据库查询函数
    server = SSHTunnelForwarder(
        ('data-pord.starklotus.com', 38001),  # 这里写入B 跳板机IP、端口
        ssh_username='mystic',  # 跳板机 用户名
        ssh_password='mystic',  # 跳板机 密码
        ssh_pkey=r'D:\唯渡科技\国内\秘钥文件对应\叶雨豪\id_rsa',
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


def chinese_bd_engine_read(sql,
                user='u_yeyuhao',
                password='KPaTrFhKvndH',
                database='fox_ods'):
    # 国内数仓数据库查询函数
    server = SSHTunnelForwarder(
        ('212.64.111.115', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='yeyuhao',  # 跳板机 用户名
        ssh_password='yeyuhao',  # 跳板机 密码
        ssh_pkey=r'D:\唯渡科技\国内\秘钥文件对应\叶雨豪\id_rsa',
        remote_bind_address=('10.10.82.46', 9030),  # 这里写入 C数据库的 IP、端口号
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


import pymysql
import pandas as pd
import warnings
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
def indonesia_bd_engine_read(sql,
                user='u_yeyuhao',
                password='W6RvuiQhrwDk',
                database='fox_ods'):
    # 印尼数仓数据库查询函数
    server = SSHTunnelForwarder(
        ('data-pord.starklotus.com', 38001),  # 这里写入B 跳板机IP、端口
        ssh_username='mystic',  # 跳板机 用户名
        ssh_password='mystic',  # 跳板机 密码
        ssh_pkey=r'D:\唯渡科技\国内\秘钥文件对应\叶雨豪\id_rsa',
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











def to_local_database(df, name, if_exists='append', index=None, base_name='qsq_fox'):
    host_data = '//root:weidu:001A@172.16.1.250:3306'
    engine = create_engine('mysql+pymysql:{}/{}?charset=utf8'.format(host_data, base_name))
    df.to_sql(name=name, con=engine, if_exists=if_exists, index=index)
    print("数据已导入内网数据库-{0}，导入方式：{1}".format(base_name, if_exists))
    return 0


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