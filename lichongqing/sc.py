from sshtunnel import SSHTunnelForwarder
import pymysql
def bjst():
    server1 = SSHTunnelForwarder(
        ('pak-dataprod.wealthleaptech.com',8003),  # 这里写入B 跳板机IP、端口
        ssh_username='liufengfang',  # 跳板机 用户名
        ssh_password='liufengfang',  # 跳板机 密码
        ssh_pkey="isa/id_rsa_liu",
        remote_bind_address=('192.168.20.7', 9030),  # 这里写入 C数据库的 IP、端口号
    )
    server1.start()
    conn_ods = pymysql.connect(
        host='127.0.0.1',       #只能写 127.0.0.1，这是固定的，不可更改
        port=server1.local_bind_port,
        user='u_zhangjinyou',      #C数据库 用户名
        password='kk^6:nayZs!8d',   #C数据库 密码
        db='fox_ods',       #填写需要连接的数据库名
        charset='utf8',
    )
    return conn_ods
def tg():
    server1 = SSHTunnelForwarder(
        ('8.219.0.11', 22),  # 这里写入B 跳板机IP、端口
        ssh_username='lichongqing',  # 跳板机 用户名
        ssh_password='lichongqing',  # 跳板机 密码
        ssh_pkey=r'isa/id_rsa_li',
        remote_bind_address=('192.168.100.171', 9030),  # 这里写入 C数据库的 IP、端口号
    )
    server1.start()
    conn_ods = pymysql.connect(
        host='127.0.0.1',  # 只能写 127.0.0.1，这是固定的，不可更改
        port=server1.local_bind_port,
        user='u_zhangjinyou',  # C数据库 用户名
        password='sDxJgGNrovEd',  # C数据库 密码
        db='fox_ods',  # 填写需要连接的数据库名
        charset='utf8',
    )
    return conn_ods
def flb():
    server1 = SSHTunnelForwarder(
        ('161.117.0.173', 22),  # 这里写入B 跳板机IP、端口
        ssh_username='liufengfang',  # 跳板机 用户名
        ssh_password='liufengfang',  # 跳板机 密码
        ssh_pkey=r'isa/id_rsa_liu',
        remote_bind_address=('192.168.59.29', 9030),  # 这里写入 C数据库的 IP、端口号
        # local_bind_address=('127.0.0.1', 8080)
    )
    server1.start()
    conn_ods = pymysql.connect(
        host='127.0.0.1',  # 只能写 127.0.0.1，这是固定的，不可更改
        port=server1.local_bind_port,
        user='u_zhangjinyou',  # C数据库 用户名
        password='sDxJgGNrovEd',  # C数据库 密码
        db='fox_ods',  # 填写需要连接的数据库名
        charset='utf8',
    )
    return conn_ods
def mxg():
    server1 = SSHTunnelForwarder(
        ('mx-dataprod.mxgbus.com', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='lichongqing',  # 跳板机 用户名
        ssh_password='lichongqing',  # 跳板机 密码
        ssh_pkey=r'isa/id_rsa_li',
        remote_bind_address=('172.20.220.164', 9030),  # 这里写入 C数据库的 IP、端口号
        # local_bind_address=('127.0.0.1', 8080)
    )
    server1.start()
    conn_ods = pymysql.connect(
        host='127.0.0.1',  # 只能写 127.0.0.1，这是固定的，不可更改
        port=server1.local_bind_port,
        user='u_zhangjinyou',  # C数据库 用户名
        password='sDxJgGNrovEd',  # C数据库 密码
        db='fox_ods',  # 填写需要连接的数据库名
        charset='utf8',
    )
    return conn_ods
def yn1():
    server1 = SSHTunnelForwarder(
        ('data-prod.empoweroceanin.com', 38001),  # 这里写入B 跳板机IP、端口
        ssh_username='Patton',  # 跳板机 用户名
        ssh_password='Patton',  # 跳板机 密码
        ssh_pkey=r'isa/国内秘钥文件',
        remote_bind_address=('192.168.25.206', 9030),  # 这里写入 C数据库的 IP、端口号
        # local_bind_address=('127.0.0.1', 8080)
    )
    server1.start()
    conn_ods = pymysql.connect(
        host='127.0.0.1',  # 只能写 127.0.0.1，这是固定的，不可更改
        port=server1.local_bind_port,
        user='u_lichongqing',  # C数据库 用户名
        password='V7rnmeQqa9eh',  # C数据库 密码
        db='fox_ods',  # 填写需要连接的数据库名
        charset='utf8', )
    return conn_ods
def yn2():
    server1 = SSHTunnelForwarder(
        ('data-prod.empoweroceanin.com', 38001),  # 这里写入B 跳板机IP、端口
        ssh_username='charly',  # 跳板机 用户名
        ssh_password='VupgjY8R3Acm1Rls',  # 跳板机 密码
        ssh_pkey=r'isa/charly_id_rsa',
        remote_bind_address=('192.168.25.206', 9030),  # 这里写入 C数据库的 IP、端口号
        # local_bind_address=('127.0.0.1', 8080)
    )
    server1.start()
    conn_ods = pymysql.connect(
        host='127.0.0.1',  # 只能写 127.0.0.1，这是固定的，不可更改
        port=server1.local_bind_port,
        user='u_lichongqing',  # C数据库 用户名
        password='V7rnmeQqa9eh',  # C数据库 密码
        db='fox_ods',  # 填写需要连接的数据库名
        charset='utf8', )
    return conn_ods

def ch():
    server1 = SSHTunnelForwarder(
        ('212.64.111.115', 36000),  # 这里写入B 跳板机IP、端口
        ssh_username='yeyuhao',  # 跳板机 用户名
        ssh_password='yeyuhao',  # 跳板机 密码
        ssh_pkey=r'isa/id_rsa_ye',
        remote_bind_address=('10.10.82.46', 9030),  # 这里写入 C数据库的 IP、端口号
        # local_bind_address=('127.0.0.1', 8080)
    )
    server1.start()
    conn_ods = pymysql.connect(
        host='127.0.0.1',  # 只能写 127.0.0.1，这是固定的，不可更改
        port=server1.local_bind_port,
        user='u_zhangjinyou',  # C数据库 用户名
        password='GaKpYEi2Bww4',  # C数据库 密码
        db='fox_ods',  # 填写需要连接的数据库名
        charset='utf8', )
    return conn_ods