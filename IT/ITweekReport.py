# -*- coding: utf-8 -*-
"""
Created on  Jan  31 17:22:00 2023

@author: yeyuhao
"""
import pandas as pd
import datetime
import numpy as np
import config
print("IT国内周报,开始运行：", datetime.datetime.now())
# =============================================================================
# 数据导入
# =============================================================================
path = r"C:\自动化数据存放\IT\周报" + "\\"   # 运行sql脚本数据存储路径
out_path = r"C:\自动化数据存放\IT\周报" + "\\"   # 数据输出路径
Collection_organization = pd.read_excel(path + r"催员归属.xlsx")  # 导入催员归属
Mail_accept = pd.read_excel(path + "周小手机短信接收.xlsx")  # 导入小手机短信接收
Mail_send = pd.read_excel(path + "周小手机发送明细.xlsx")  # 导入小手机短信发送
Tcall_history = pd.read_excel(path + "周小手机通话明细.xlsx")  # 导入小手机通话明细
Group_online = pd.read_excel(path + "周小组上线天数.xlsx")  # 导入各小组本周上线总天数
area_online = pd.read_excel(path + "周区域上线天数.xlsx")  # 导入各区域本周上线总天数
Collection_online = pd.read_excel(path + "周催员上线天数.xlsx")  # 导入各催员本周上线总天数
call_history = pd.read_excel(path + "周国内通话详单.xlsx")  # 导入国内通话详单
call_batch = pd.read_excel(path + "周国内批呼.xlsx")  # 导入国内批呼
call_batch_summary = pd.read_excel(path + "周国内批呼汇总.xlsx")  # 导入国内批呼

# =============================================================================
# 数据处理
# =============================================================================
call_history = call_history.rename(columns={"开始时间": "起始日期"})
call_history['起始日期'] = call_history['起始日期'].apply(lambda x: str(x)[0:10])
call_history['截止日期'] = call_history['截止日期'].apply(lambda x: str(x)[0:10])
call_batch = call_batch.rename(columns={"date(`create_at`)": "日期"})
call_batch['日期'] = call_batch['日期'].apply(lambda x: str(x)[0:10])
Collection_organization = Collection_organization.rename(columns={"name": "姓名", "user_id": "催员ID",
                                                                  "group_name": "组别", "area_name": "区域",
                                                                  "director_name": "主管", "group_leader_name": "组长"})
Tcall_history = Tcall_history.rename(columns={"call_day": "日期", "dunner_id": "催员ID",
                                              "dunner_name": "姓名"})


# 添加小手机催员属地
def add_area(df1, df2):
    area = df2[['催员ID', '区域', '组别', '组长', '主管']].copy()
    temp_df = pd.merge(df1, area, on='催员ID', how='left')  # 匹配组长编号
    return temp_df


Mail_accept = add_area(Mail_accept, Collection_organization)
Mail_send = add_area(Mail_send, Collection_organization)
Tcall_history = add_area(Tcall_history, Collection_organization)


# 短信数据汇总
def mail_summary(df1):
    temp_df = df1[(df1['区域'] == "武汉") | (df1['区域'] == "合肥")]
    temp_df = temp_df.groupby(by=['区域', '组别'], as_index=False).agg({"数量": "sum"})
    return temp_df


Mail_accept_summary = mail_summary(Mail_accept)
Mail_send_summary = mail_summary(Mail_send)


# 小手机通话数据汇总
def Tcall_summary(df1, df2):
    temp_df = df1[(df1['区域'] == "武汉") | (df1['区域'] == "合肥")]
    temp_df = temp_df.groupby(by=['区域', '组别'], as_index=False).agg({'phone_num 外呼次数': "sum",
                                                                    "answer_num 接通次数": "sum"})
    temp_df["小组接通率"] = round(temp_df["answer_num 接通次数"] / temp_df["phone_num 外呼次数"], 4)
    temp_df = pd.merge(temp_df, df2, how="left", on=['区域', '组别'])
    temp_df["平均每天外呼次数（本周）"] = round(temp_df["phone_num 外呼次数"] / temp_df["本周上线总数"], 0)
    temp_df["平均每天接通次数（本周）"] = round(temp_df["answer_num 接通次数"] / temp_df["本周上线总数"], 0)
    del temp_df["本周上线总数"]
    return temp_df


Tcall_summary = Tcall_summary(Tcall_history, Group_online)


# 国内通话详单制作
def call_detail(df1, df2):
    temp_df = pd.merge(df1, df2[['催员ID', '上线天数']], on='催员ID', how='left')
    temp_df['平均每天外呼次数（本周）'] = round(temp_df["累计外呼次数"] / temp_df["上线天数"], 0)
    temp_df['平均每天接通次数（本周）'] = round(temp_df["累计接通次数(含IVR)"] / temp_df["上线天数"], 0)
    temp_df['平均每天通话时长(S)'] = round(temp_df["本周累计通话时长"] * 3600 / temp_df["上线天数"], 4)
    temp_df['平均每天振铃时长(S)'] = round(temp_df["本周累计振铃时长"] * 3600 / temp_df["上线天数"], 4)
    temp_df['平均每天总通话时长(S)'] = round(temp_df["本周总通话时长"] * 3600 / temp_df["上线天数"], 4)
    temp_df['平均每天通话时长(H)'] = round(temp_df["本周累计通话时长"] / temp_df["上线天数"], 4)
    temp_df['平均每天振铃时长(H)'] = round(temp_df["本周累计振铃时长"] / temp_df["上线天数"], 4)
    del temp_df["上线天数"]
    return temp_df


call_history = call_detail(call_history, Collection_online)
call_history = call_history[call_history['平均每天总通话时长(S)'].notnull()]


# 国内通话区域汇总
def call_summary(df1, df2, df3, df4):
    temp_df = pd.merge(df1, df2[['催员ID', '区域']], on='催员ID', how='left')
    temp_df = pd.merge(temp_df, df4[['催员ID', '上线天数']], on='催员ID', how='left')
    temp_df = temp_df[(temp_df['区域'] == "武汉") | (temp_df['区域'] == "合肥")]
    temp_df = temp_df.groupby(by='区域', as_index=False).agg({'累计外呼次数': "sum",
                                                            "累计接通次数(含IVR)": "sum",
                                                            "累计接通次数": "sum",
                                                            "本周累计振铃时长": "sum",
                                                            "本周总通话时长": "sum",
                                                            "本周累计通话时长": "sum",
                                                            "上线天数": "sum"})
    # temp_df = pd.merge(temp_df, df3[['区域', '上线天数']], on='区域', how='left')
    temp_df['平均每天外呼次数（本周）'] = round(temp_df["累计外呼次数"] / temp_df["上线天数"], 0)
    temp_df['平均每天接通次数（本周）'] = round(temp_df["累计接通次数(含IVR)"] / temp_df["上线天数"], 0)
    temp_df['接通率'] = round(temp_df["累计接通次数(含IVR)"] / temp_df["累计外呼次数"], 4)
    temp_df['平均每天通话时长(S)'] = round(temp_df["本周累计通话时长"] * 3600 / temp_df["上线天数"], 4)
    temp_df['平均每天振铃时长(S)'] = round(temp_df["本周累计振铃时长"] * 3600 / temp_df["上线天数"], 4)
    temp_df['平均每天总通话时长(S)'] = round(temp_df["本周总通话时长"] * 3600 / temp_df["上线天数"], 4)
    temp_df['平均每天通话时长(H)'] = round(temp_df["本周累计通话时长"] / temp_df["上线天数"], 4)
    temp_df['平均每天振铃时长(H)'] = round(temp_df["本周累计振铃时长"] / temp_df["上线天数"], 4)
    temp_df['平均每通电话通话时长(s)'] = round(temp_df["本周累计通话时长"] * 3600 / temp_df["累计接通次数(含IVR)"], 4)
    temp_df['平均每通电话振铃时长(s)'] = round(temp_df["本周累计振铃时长"] * 3600 / temp_df["累计外呼次数"], 4)
    del temp_df["上线天数"]
    return temp_df


call_summary = call_summary(call_history, Collection_organization, area_online, Collection_online)

# =============================================================================
# 数据输出
# =============================================================================
now = datetime.datetime.now()
print("开始输出", datetime.datetime.now())
out_path = r"C:\自动化数据存放\IT\周报" + "\\"
writer1 = pd.ExcelWriter(out_path + "国内小手机.xlsx", engine='xlsxwriter')
# writer1 = pd.ExcelWriter(out_path + "小手机{}.xlsx".format(str(now)[0:10]), engine='xlsxwriter')
Tcall_summary.to_excel(writer1, sheet_name='通话数据汇总', index=False)
Mail_send_summary.to_excel(writer1, sheet_name='短信发送汇总', index=False)
Mail_accept_summary.to_excel(writer1, sheet_name='短信接收汇总', index=False)
Tcall_history.to_excel(writer1, sheet_name='通话明细', index=False)
Mail_send.to_excel(writer1, sheet_name='短信发送明细', index=False)
Mail_accept.to_excel(writer1, sheet_name='短信接收明细', index=False)
Collection_organization.to_excel(writer1, sheet_name='人员匹配', index=False)
writer1.save()  # 此语句不可少，否则本地文件未保存

writer2 = pd.ExcelWriter(out_path + "国内通话详单.xlsx", engine='xlsxwriter')
# writer2 = pd.ExcelWriter(out_path + "国内通话详单{}.xlsx".format(str(now)[0:10]), engine='xlsxwriter')
call_summary.to_excel(writer2, sheet_name='国内-详单-呈现表', index=False)
call_batch_summary.to_excel(writer2, sheet_name='国内批呼-呈现表', index=False)
call_history.to_excel(writer2, sheet_name='国内-详单', index=False)
call_batch.to_excel(writer2, sheet_name='国内批呼数据源', index=False)
Collection_organization[['催员ID', '姓名', '区域']].to_excel(writer2, sheet_name='城市查询', index=False)
writer2.save()  # 此语句不可少，否则本地文件未保存

# =============================================================================
# 邮件发送
# =============================================================================
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart


##### 配置区  #####
mail_host = 'smtp.exmail.qq.com'
mail_port = '465'

receiver_lst = ['yeyuhao@weidu.ac.cn', 'zhangjinyou@weidu.ac.cn'
                , 'fangyuanyuan@weidu.ac.cn', 'liwangchun@weidu.ac.cn', 'lijingyi@weidu.ac.cn', 'weilujie@weidu.ac.cn', 'panwei@weidu.ac.cn']


# 发件人邮箱账号
login_sender = 'shwd_operation@weidu.ac.cn'
# 发件人邮箱授权码而不是邮箱密码，授权码由邮箱官网可设置生成
login_pass = config.login_pass
# 邮箱文本内容

mail_msg = '''
            call_history.xlsx-->国内通话详单
            <br>mobile.xlsx-->国内小手机
    '''
# 发送者
sendName = "数据组"
# 接收者
resName = ""
# 邮箱正文标题
title = "国内IT周报" + str(now)[0:10]

######### end  ##########

msg = MIMEMultipart()
msg['From'] = formataddr([sendName, login_sender])
# 邮件的标题
msg['Subject'] = title
msg.attach(MIMEText(mail_msg, 'html', 'utf-8'))   # 添加正文

att1 = MIMEText(open(out_path + '国内通话详单.xlsx', 'rb').read(), 'base64', 'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att1["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att1["Content-Disposition"] = 'attachment;filename="call_history.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att1)  # 将附件添加到邮件内容当中

att2 = MIMEText(open(out_path + '国内小手机.xlsx', 'rb').read(), 'base64', 'utf-8')  # 添加附件，由于定义了中文编码，所以文件可以带中文
att2["Content-Type"] = 'application/octet-stream'  # 数据传输类型的定义
att2["Content-Disposition"] = 'attachment;filename="mobile.xlsx"'  # 定义文件在邮件中显示的文件名和后缀名，名字不可为中文
msg.attach(att2)  # 将附件添加到邮件内容当中
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
