# -*- coding: gbk -*-
import pandas as pd
from datetime import date,timedelta,datetime
import saveexcel
import os
import mail
name = os.path.basename(__file__).split(".")[0]
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
try:
    df = pd.read_excel("./̩���ձ�/{}̩���ձ�.xlsx".format(yesterday))
    # df = pd.read_excel("./̩���ձ�/2024-08-08̩���ձ�.xlsx")
except FileNotFoundError as e:
    print("�Ҳ����ļ�")

df2 = df.groupby('Group',as_index=False).agg(
    ������=('Collection Executive','count'),
    ��������=('Newly enrolled Yes/ No',lambda x: x[x == 'YES'].count()),
    b10��������=('cbdr', lambda x: ((x == 'bottom10%') & (df.loc[x.index, 'Newly enrolled Yes/ No'] == "YES")).sum())
    )
df2['��������ռ��'] = df2['b10��������']/df2['��������']
saveexcel.save(name,"./"+name,����=df2)

df_email = pd.read_csv('./����.txt', sep='=', header=None,encoding='gbk')
df_email.columns = ['key', 'value']
email_variable = df_email.loc[df_email['key'] == '����', 'value'].values[0]
email_password = df_email.loc[df_email['key'] == '����', 'value'].values[0]
to_email = ["zhongfeng@weidu.ac.cn","zhangjinyou@weidu.ac.cn","lichongqing@weidu.ac.cn"]
title = "{}̩���ձ����˼��".format(yesterday)
path =  "./"+name+"/"+name+".xlsx"
filename=name+".xlsx"
mail.send_email(title,path,filename,to_email,email_variable,email_password)