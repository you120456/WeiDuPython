# -*- coding: utf-8 -*-
"""
Created on  Jan  31 17:22:00 2023

@author: yeyuhao
"""
import pandas as pd
import datetime
import numpy as np

print("各国最大通话时间,开始运行：", datetime.datetime.now())
# =============================================================================
# 数据导入
# =============================================================================
user_path = r"C:\自动化数据存放\稽核\日报" + "\\"  # 运行sql脚本数据存储路径
out_path = r"C:\自动化数据存放\最大通时" + "\\"  # 数据输出路径
chinese_id = pd.read_excel(user_path + r"国内稽核架构.xlsx", usecols=['组别', '主管', '组长', 'name', 'user_id'])
Pakistan_id = pd.read_excel(user_path + r"巴基斯坦稽核架构.xlsx", usecols=['组别', '主管', '组长', 'name', 'user_id'])
India_id = pd.read_excel(user_path + r"印度稽核架构.xlsx", usecols=['组别', '主管', '组长', 'name', 'user_id'])
Thailand_id = pd.read_excel(user_path + r"泰国稽核架构.xlsx", usecols=['组别', '主管', '组长', 'name', 'user_id'])
Philippines_id = pd.read_excel(user_path + r"菲律宾稽核架构.xlsx", usecols=['组别', '主管', '组长', 'name', 'user_id'])
Mexico_id = pd.read_excel(user_path + r"墨西哥稽核架构.xlsx", usecols=['组别', '主管', '组长', 'name', 'user_id'])
chinese_call = pd.read_excel(out_path + r"国内日最大通时.xlsx")
Pakistan_call = pd.read_excel(out_path + r"巴基斯坦日最大通时.xlsx")
India_call = pd.read_excel(out_path + r"印度日最大通时.xlsx")
Thailand_call = pd.read_excel(out_path + r"泰国日最大通时.xlsx")
Philippines_call = pd.read_excel(out_path + r"菲律宾日最大通时.xlsx")
Mexico_call = pd.read_excel(out_path + r"墨西哥日最大通时.xlsx")
chinese_id = chinese_id.rename(columns={'name': "催员", 'user_id': "催员ID"})
Pakistan_id = Pakistan_id.rename(columns={'name': "催员", 'user_id': "催员ID"})
India_id = India_id.rename(columns={'name': "催员", 'user_id': "催员ID"})
Thailand_id = Thailand_id.rename(columns={'name': "催员", 'user_id': "催员ID"})
Philippines_id = Philippines_id.rename(columns={'name': "催员", 'user_id': "催员ID"})
Mexico_id = Mexico_id.rename(columns={'name': "催员", 'user_id': "催员ID"})


def add_name(call, name):
    temp = pd.merge(call, name, on='催员ID', how='left')
    temp = temp[['组别', '主管', '组长', '催员', '催员ID', '拨打时间', '拨打时长', '债务人ID', '资产编号', '债务人电话']]
    return temp


chinese = add_name(chinese_call, chinese_id)
Pakistan = add_name(Pakistan_call, Pakistan_id)
India = add_name(India_call, India_id)
Thailand = add_name(Thailand_call, Thailand_id)
Philippines = add_name(Philippines_call, Philippines_id)
Mexico = add_name(Mexico_call, Mexico_id)


chinese.to_excel(out_path + r"chinese.xlsx")
Pakistan.to_excel(out_path + r"Pakistan.xlsx")
India.to_excel(out_path + r"India.xlsx")
Thailand.to_excel(out_path + r"Thailand.xlsx")
Philippines.to_excel(out_path + r"Philippines.xlsx")
Mexico.to_excel(out_path + r"Mexico.xlsx")

# =============================================================================
# 邮件发送
# =============================================================================
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart

now = datetime.date.today() - datetime.timedelta(1)

##### 配置区  #####
mail_host = 'smtp.exmail.qq.com'
mail_port = '465'

receiver_lst = ['yeyuhao@weidu.ac.cn']


# 发件人邮箱账号
login_sender = 'shwd_operation@weidu.ac.cn'
# 发件人邮箱授权码而不是邮箱密码，授权码由邮箱官网可设置生成
login_pass = 'Shanghai0215'
# 邮箱文本内容

mail_msg = '''
           chinese.xlsx-->国内\n
            Pakistan.xlsx-->巴基斯坦\n
            India.xlsx-->印度\n
            Thailand.xlsx-->泰国\n
           Philippines.xlsx-->菲律宾\n
            Mexico.xlsx-->墨西哥\n
    '''
# 发送者
sendName = "数据组"
# 接收者
resName = ""
# 邮箱正文标题
title = "各国最大通话时长" + str(now)[0:10]

######### end  ##########

msg = MIMEMultipart()
msg['From'] = formataddr([sendName, login_sender])
# 邮件的标题
msg['Subject'] = title
msg.attach(MIMEText(mail_msg, 'html', 'utf-8'))   # 添加正文

att1 = MIMEText(open(out_path + r"chinese.xlsx", 'rb').read(), 'base64', 'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att1["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att1["Content-Disposition"] = 'attachment;filename="chinese.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att1)  # 将附件添加到邮件内容当中

att2 = MIMEText(open(out_path + r"Pakistan.xlsx", 'rb').read(), 'base64', 'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att2["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att2["Content-Disposition"] = 'attachment;filename="Pakistan.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att2)  # 将附件添加到邮件内容当中

att3 = MIMEText(open(out_path + r"India.xlsx", 'rb').read(), 'base64', 'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att3["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att3["Content-Disposition"] = 'attachment;filename="India.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att3)  # 将附件添加到邮件内容当中

att4 = MIMEText(open(out_path + r"Thailand.xlsx", 'rb').read(), 'base64', 'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att4["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att4["Content-Disposition"] = 'attachment;filename="Thailand.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att4)  # 将附件添加到邮件内容当中

att5 = MIMEText(open(out_path + r"Philippines.xlsx", 'rb').read(), 'base64', 'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att5["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att5["Content-Disposition"] = 'attachment;filename="Philippines.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att5)  # 将附件添加到邮件内容当中

att6 = MIMEText(open(out_path + r"Mexico.xlsx", 'rb').read(), 'base64', 'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att6["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att6["Content-Disposition"] = 'attachment;filename="Mexico.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att6)  # 将附件添加到邮件内容当中
# 服务器
server = smtplib.SMTP_SSL(mail_host, mail_port)
server.login(login_sender, login_pass)
server.sendmail(login_sender, receiver_lst, msg.as_string())
print("已发送到:\n" + ';\n'.join(receiver_lst) + "的邮箱中！")
server.quit()





print("运行结束", datetime.datetime.now())

# =============================================================================
# 清空内存
# =============================================================================
import gc

def clean_variables():
    variables = list(globals().keys()).copy()
    cannot_delete = ['gc']
    for key in variables:
        try:
            if (key[:1] != '_') and (key not in cannot_delete):
                globals().pop(key)  # 删除变量
                gc.collect()  # 清理内存
        except:
            pass


clean_variables()  # sys.getsizeof(combine)

