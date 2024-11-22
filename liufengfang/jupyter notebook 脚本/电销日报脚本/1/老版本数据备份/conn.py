from sshtunnel import SSHTunnelForwarder
import pymysql

# 菲律宾数仓
def conn_ph():
    tunnel = SSHTunnelForwarder(
    ('161.117.0.173', 22),
    ssh_username='liufengfang',
    ssh_password='liufengfang',
    ssh_pkey = r'C:\Users\Administrator\.ssh\id_rsa',
    remote_bind_address=('192.168.59.29',9030),
    local_bind_address=('127.0.0.1', 0))
    tunnel.start()
    return pymysql.connect(
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        user='u_liufengfang',
        password='UFCi3gIjo7qm',
        database='fox_ods')


# 国内数仓
def conn_dm():
    tunnel = SSHTunnelForwarder(
    ('212.64.111.115', 36000),
    ssh_username='yeyuhao',
    ssh_password='yeyuhao',
    ssh_pkey = r'D:\国内fox密钥\id_rsa',
    remote_bind_address=('10.10.82.46',9030),
    local_bind_address=('127.0.0.1', 0))
    tunnel.start()
    return pymysql.connect(
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        user='u_liufengfang',
        password='vAxXyT1kSmHQ',
        database='fox_ods')

# 墨西哥数仓
def conn_mx():
    tunnel = SSHTunnelForwarder(
    ('mx-dataprod.mxgbus.com', 36000),
    ssh_username='liufengfang',
    ssh_password='liufengfang',
    ssh_pkey = r'C:\Users\Administrator\.ssh\id_rsa',
    remote_bind_address=('172.20.220.164',9030),
    local_bind_address=('127.0.0.1', 0))
    tunnel.start()
    return pymysql.connect(
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        user='u_liufengfang',
        password='UFCi3gIjo7qm',
        database='fox_ods')

# 泰国数仓
def conn_th():
    tunnel = SSHTunnelForwarder(
    ('8.219.0.11', 22),
    ssh_username='liufengfang',
    ssh_password='liufengfang',
    ssh_pkey = r'C:\Users\Administrator\.ssh\id_rsa',
    remote_bind_address=('192.168.100.171',9030),
    local_bind_address=('127.0.0.1', 0))
    tunnel.start()
    return pymysql.connect(
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        user='u_liufengfang',
        password='UFCi3gIjo7qm',
        database='fox_ods')


# 印度尼西亚数据仓库
def conn_in():
    tunnel = SSHTunnelForwarder(
    ('8.219.0.11', 22),
    ssh_username='liufengfang',
    ssh_password='liufengfang',
    ssh_pkey = r'C:\Users\Administrator\.ssh\id_rsa',
    remote_bind_address=('192.168.100.171',9030),
    local_bind_address=('127.0.0.1', 0))
    tunnel.start()
    return pymysql.connect(
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        user='u_liufengfang',
        password='UFCi3gIjo7qm',
        database='fox_ods')