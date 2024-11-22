# -*- coding: gbk -*-
import pandas as pd
from datetime import date,timedelta,datetime
import saveexcel
import os
import mail
name = os.path.basename(__file__).split(".")[0]
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
try:
    df = pd.read_excel("./泰国日报/{}泰国日报.xlsx".format(yesterday))
    # df = pd.read_excel("./泰国日报/2024-08-08泰国日报.xlsx")
except FileNotFoundError as e:
    print("找不到文件")

df2 = df.groupby('Group',as_index=False).agg(
    总人数=('Collection Executive','count'),
    新人总数=('Newly enrolled Yes/ No',lambda x: x[x == 'YES'].count()),
    b10新人人数=('cbdr', lambda x: ((x == 'bottom10%') & (df.loc[x.index, 'Newly enrolled Yes/ No'] == "YES")).sum())
    )
df2['新人人数占比'] = df2['b10新人人数']/df2['新人总数']
saveexcel.save(name,"./"+name,新人=df2)

df_email = pd.read_csv('./邮箱.txt', sep='=', header=None,encoding='gbk')
df_email.columns = ['key', 'value']
email_variable = df_email.loc[df_email['key'] == '邮箱', 'value'].values[0]
email_password = df_email.loc[df_email['key'] == '密码', 'value'].values[0]
to_email = ["zhongfeng@weidu.ac.cn","zhangjinyou@weidu.ac.cn","lichongqing@weidu.ac.cn"]
title = "{}泰国日报新人监控".format(yesterday)
path =  "./"+name+"/"+name+".xlsx"
filename=name+".xlsx"
mail.send_email(title,path,filename,to_email,email_variable,email_password)