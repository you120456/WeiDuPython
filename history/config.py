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

def chinese_fox_engine_read(sql,
               user='yeyuhao',
               password='xmI6KkSW#qqF',
               database='qsq_fox'):
    # 国内fox数据库查询函数
    server = SSHTunnelForwarder(
        ('212.64.111.115', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='yeyuhao',  # 跳板机 用户名
        ssh_password='yeyuhao',  # 跳板机 密码
        ssh_pkey=r'C:\自动化python代码存放\秘钥文件对应\叶雨豪\id_rsa',
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


def chinese_audit_engine_read(sql,
                user='yeyuhao',
                password='#1wLSCPbixH2',
                database='jihe'):
    # 国内audit数据库查询函数
    server = SSHTunnelForwarder(
        ('212.64.111.115', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='yeyuhao',  # 跳板机 用户名
        ssh_password='yeyuhao',  # 跳板机 密码
        ssh_pkey=r'C:\自动化python代码存放\秘钥文件对应\叶雨豪\id_rsa',
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


def chinese_bi_engine_read(sql,
                user='yeyuhao',
                password='%CAPJvfoSzvU',
                database='bi'):
    # 国内bi数据库查询函数
    server = SSHTunnelForwarder(
        ('212.64.111.115', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='zhangjinyou',  # 跳板机 用户名
        ssh_password='zhangjinyou',  # 跳板机 密码
        ssh_pkey=r'C:\唯渡科技\国内\国内秘钥文件',
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
def chinese_bd_engine_read(sql,
                user='u_yeyuhao',
                password='KPaTrFhKvndH',
                database='fox_ods'):
    # 国内数仓数据库查询函数
    server = SSHTunnelForwarder(
        ('212.64.111.115', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='yeyuhao',  # 跳板机 用户名
        ssh_password='yeyuhao',  # 跳板机 密码
        ssh_pkey=r'E:\秘钥文件对应\叶雨豪\id_rsa',
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

def pakistan_fox_engine_read(sql,
               user='liufengfang',
               password='vNeMQ9kz#CA9',
               database='arcticfox'):
    # 巴基斯坦fox数据库查询函数
    server = SSHTunnelForwarder(
        ('161.117.0.173', 22),  # 这里写入B 跳板机IP、端口
        ssh_username='wd_bi',  # 跳板机 用户名
        ssh_password='wd_bi',  # 跳板机 密码
        ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
        remote_bind_address=('rr-t4ne6rfduze0m05e9.mysql.singapore.rds.aliyuncs.com', 3306),  # 这里写入 C数据库的 IP、端口号
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

def pakistan_audit_engine_read(sql,
               user='liufengfang',
               password='ZPZs%dVwRS0V',
               database='audit'):
    # 巴基斯坦audit数据库查询函数
    server = SSHTunnelForwarder(
        ('161.117.0.173', 22),  # 这里写入B 跳板机IP、端口
        ssh_username='wd_bi',  # 跳板机 用户名
        ssh_password='wd_bi',  # 跳板机 密码
        ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
        remote_bind_address=('rr-t4nwe1u55h29b9648.mysql.singapore.rds.aliyuncs.com', 3306)
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

def thailand_fox_engine_read(sql,
               user='lichongqing',
               password='PoJ8InsuEUi#',
               database='arcticfox'):
    # 泰国fox数据库查询函数
    server = SSHTunnelForwarder(
        ('8.219.0.11', 22),  # 这里写入B 跳板机IP、端口
        ssh_username='wd_bi',  # 跳板机 用户名
        ssh_password='wd_bi',  # 跳板机 密码
        ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
        remote_bind_address=('rr-gs534j9ejuu6mz37p.mysql.singapore.rds.aliyuncs.com', 3306),  # 这里写入 C数据库的 IP、端口号
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

def thailand_audit_engine_read(sql,
               user='lichongqing',
               password='PoJ8InsuEUi#',
               database='audit'):
    # 菲律宾audit数据库查询函数
    server = SSHTunnelForwarder(
        ('8.219.0.11', 22),  # 这里写入B 跳板机IP、端口
        ssh_username='wd_bi',  # 跳板机 用户名
        ssh_password='wd_bi',  # 跳板机 密码
        ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
        remote_bind_address=('rr-gs5dz4wv09v8vq0hk.mysql.singapore.rds.aliyuncs.com', 3306),  # 这里写入 C数据库的 IP、端口号
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

def thailand_tmk_engine_read(sql,
               user='zhangjinyou',
               password='$IVb7wIbYgZx',
               database='tmk'):
    # 泰国电销数据库查询函数
    server = SSHTunnelForwarder(
        ('8.219.0.11', 22),  # 这里写入B 跳板机IP、端口
        ssh_username='wd_bi',  # 跳板机 用户名
        ssh_password='wd_bi',  # 跳板机 密码
        ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
        remote_bind_address=('rm-gs5ie2r02391875o5.mysql.singapore.rds.aliyuncs.com', 3306),  # 这里写入 C数据库的 IP、端口号
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

def thailand_BI_engine_read(sql,
               user='zhangjinyou',
               password='$IVb7wIbYgZx',
               database='tmk'):
    # 泰国电销数据库查询函数
    server = SSHTunnelForwarder(
        ('8.219.0.11', 22),  # 这里写入B 跳板机IP、端口
        ssh_username='wd_bi',  # 跳板机 用户名
        ssh_password='wd_bi',  # 跳板机 密码
        ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
        remote_bind_address=('rm-gs5ie2r02391875o5.mysql.singapore.rds.aliyuncs.com', 3306),  # 这里写入 C数据库的 IP、端口号
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

def philippines_audit_engine_read(sql,
               user='liufengfang',
               password='s78kWRVLeP#L',
               database='audit'):
    # 菲律宾audit数据库查询函数
    server = SSHTunnelForwarder(
        ('161.117.0.173', 22),  # 这里写入B 跳板机IP、端口
        ssh_username='wd_bi',  # 跳板机 用户名
        ssh_password='wd_bi',  # 跳板机 密码
        ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
        remote_bind_address=('rr-t4n3ss9ht7onwsk39.mysql.singapore.rds.aliyuncs.com', 3306),  # 这里写入 C数据库的 IP、端口号
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

def philippines_fox_engine_read(sql,
               user='liufengfang',
               password='JaJuYTzTH@qU',
               database='arcticfox'):
    # 泰国fox数据库查询函数
    server = SSHTunnelForwarder(
        ('161.117.0.173', 22),  # 这里写入B 跳板机IP、端口
        ssh_username='wd_bi',  # 跳板机 用户名
        ssh_password='wd_bi',  # 跳板机 密码
        ssh_pkey=r'C:\Users\Administrator\Desktop\数据库配置\虚拟机\id_rsa',
        remote_bind_address=('rr-t4nk148l186ocd072.mysql.singapore.rds.aliyuncs.com', 3306),  # 这里写入 C数据库的 IP、端口号
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



def mexico_audit_engine_read(sql,
               user='yeyuhao',
               password='5%wrUNk7iili',
               database='audit'):
    # 菲律宾audit数据库查询函数
    server = SSHTunnelForwarder(
        ('mx-fox-dataprod.mxgbus.com', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='xumingming',  # 跳板机 用户名
        ssh_password='xumingming',  # 跳板机 密码
        ssh_pkey=r'E:\秘钥文件对应\徐明明\.ssh\.ssh\id_rsa',
        remote_bind_address=('rr-2ev95e7b7n6692r6e.mysql.rds.aliyuncs.com', 3306),  # 这里写入 C数据库的 IP、端口号
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

def mexico_fox_engine_read(sql,
               user='yeyuhao',
               password='3%MSb6EtZoWk',
               database='arcticfox'):
    # 泰国fox数据库查询函数
    server = SSHTunnelForwarder(
        ('mx-fox-dataprod.mxgbus.com', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='xumingming',  # 跳板机 用户名
        ssh_password='xumingming',  # 跳板机 密码
        ssh_pkey=r'E:\秘钥文件对应\徐明明\.ssh\.ssh\id_rsa',
        remote_bind_address=('rr-2evi0han6p7ng6218.mysql.rds.aliyuncs.com', 3306),  # 这里写入 C数据库的 IP、端口号
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



def indonesia_audit_engine_read(sql,
               user='mystic',
               password='c@dRZLmSWPW6!gOL',
               database='audit'):
    # 菲律宾audit数据库查询函数
    server = SSHTunnelForwarder(
        ('data-prod.empoweroceanin.com', 38002),  # 这里写入B 跳板机IP、端口
        ssh_username='mystic',  # 跳板机 用户名
        ssh_password='mystic',  # 跳板机 密码
        ssh_pkey=r'C:\配置文件\id_rsa',
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
    # 印尼fox数据库查询函数
    server = SSHTunnelForwarder(
        ('data-prod.empoweroceanin.com', 38002),  # 这里写入B 跳板机IP、端口
        ssh_username='mystic',  # 跳板机 用户名
        ssh_password='mystic',  # 跳板机 密码
        ssh_pkey=r'C:\配置文件\id_rsa',
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
        ('data-prod.empoweroceanin.com', 38002),  # 这里写入B 跳板机IP、端口
        ssh_username='mystic',  # 跳板机 用户名
        ssh_password='mystic',  # 跳板机 密码
        ssh_pkey=r'E:\秘钥文件对应\叶雨豪\id_rsa',
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


def indonesia_bd_engine_read(sql,
                user='u_yeyuhao',
                password='W6RvuiQhrwDk',
                database='fox_ods'):
    # 印尼数仓数据库查询函数
    server = SSHTunnelForwarder(
        ('data-pord.starklotus.com', 38001),  # 这里写入B 跳板机IP、端口
        ssh_username='mystic',  # 跳板机 用户名
        ssh_password='mystic',  # 跳板机 密码
        ssh_pkey=r'E:\秘钥文件对应\叶雨豪\id_rsa',
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
    # 内网数据库导入
    host_data = '//root:weidu:001A@172.16.1.250:3306'
    engine = create_engine('mysql+pymysql:{}/{}?charset=utf8'.format(host_data, base_name))
    df.to_sql(name=name, con=engine, if_exists=if_exists, index=index)
    print("数据已导入内网数据库-{0}，导入方式：{1}".format(base_name, if_exists))
    return 0

def local_database_engine_read(sql, base_name='qsq_fox'):
    # 内网数据库导入
    host_data = '//root:weidu:001A@172.16.1.250:3306'
    engine = create_engine('mysql+pymysql:{}/{}?charset=utf8'.format(host_data, base_name))
    temp_df = pd.read_sql(sql, engine)

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