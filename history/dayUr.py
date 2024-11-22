#!/usr/bin/env python
# coding: utf-8

# ##### -*- coding: utf-8 -*-
# Created : 2023/07/17
# Update : 2023/07/17
# author: yeyuhao
#

# In[1]:


import pymysql
import mysql.connector
import datetime
import pandas as pd
from sqlalchemy import create_engine
import warnings
import config
warnings.filterwarnings("ignore")
from sshtunnel import SSHTunnelForwarder
from dateutil.relativedelta import relativedelta
print("'脚本4：每日催员组织架构'\n开始执行!", datetime.datetime.now())
now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
last_two_months = now + relativedelta(months=-2)
print('日期{}'.format(str(now)[:10]))



sql_data = '''SELECT
                    DATE (
                    now()) AS '日期',
                    SUBSTRING_INDEX( uo.`asset_group_name`, ',',- 1 ) '组别',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_names`, ',', 2 ), ',',- 1 ) '主管',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_ids`, ',', 2 ), ',',- 1 ), '_',- 1 ) as '主管ID',
                    uo.parent_user_name '组长',
                    uo.parent_user_id '组长ID',
                    b.co_NO AS '工号',
                    uo.`name` AS '催员',
                    uo.user_id AS '催员ID',
                    b.del_date AS '账号删除日期',
                    c.dimission_date AS '业绩截止日期',
                    b.del_flag AS '离职标志' 
                FROM
                    user_organization uo
                    LEFT JOIN ( SELECT DISTINCT NAME, NO AS TL_NO FROM sys_user ) AS cc ON uo.parent_user_name = cc.
                    NAME LEFT JOIN ( SELECT DISTINCT id, NO AS co_NO, t.del_date, t.del_flag FROM sys_user t ) AS b ON uo.user_id = b.id
                    LEFT JOIN ( SELECT DISTINCT user_id, dimission_date FROM sys_user_extend ) AS c ON uo.user_id = c.user_id
                    LEFT JOIN ( SELECT DISTINCT user_id, min( online_day ) online_day FROM online_days WHERE online_day_flag = 1 GROUP BY user_id ) AS d ON uo.user_id = d.user_id 
                WHERE
                    (
                        c.dimission_date > '{}' 
                    OR b.del_flag = 0)
                    '''.format(last_two_months)

data_df = config.chinese_fox_engine_read(sql_data)



host_data = '//root:weidu:001A@172.16.1.250:3306'
data_engine = create_engine('mysql+pymysql:{}/qsq_fox?charset=utf8'.format(host_data))
# data_df.to_sql(name ='user_organization', con = data_engine, if_exists='append', index=None)
data_df.to_sql(name='day_user_organization', con=data_engine, if_exists='append', index=None)


sql_Pakistan_data = '''SELECT
                    DATE (
                    now()) AS '日期',
                    SUBSTRING_INDEX( uo.`asset_group_name`, ',',- 1 ) '组别',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_names`, ',', 2 ), ',',- 1 ) '主管',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_ids`, ',', 2 ), ',',- 1 ), '_',- 1 ) as '主管ID',
                    uo.parent_user_name '组长',
                    uo.parent_user_id '组长ID',
                    b.co_NO AS '工号',
                    uo.`name` AS '催员',
                    uo.user_id AS '催员ID',
                    b.del_date AS '账号删除日期',
                    c.dimission_date AS '业绩截止日期',
                    b.del_flag AS '离职标志' 
                FROM
                    user_organization uo
                    LEFT JOIN ( SELECT DISTINCT NAME, NO AS TL_NO FROM sys_user ) AS cc ON uo.parent_user_name = cc.
                    NAME LEFT JOIN ( SELECT DISTINCT id, NO AS co_NO, t.del_date, t.del_flag FROM sys_user t ) AS b ON uo.user_id = b.id
                    LEFT JOIN ( SELECT DISTINCT user_id, dimission_date FROM sys_user_extend ) AS c ON uo.user_id = c.user_id
                    LEFT JOIN ( SELECT DISTINCT user_id, min( online_day ) online_day FROM online_days WHERE online_day_flag = 1 GROUP BY user_id ) AS d ON uo.user_id = d.user_id 
                WHERE
                    (
                        c.dimission_date > '{}' 
                    OR b.del_flag = 0)
                    '''.format(last_two_months)

data_Pakistan_df = config.pakistan_fox_engine_read(sql_Pakistan_data)

host_data = '//root:weidu:001A@172.16.1.250:3306'
data_Pakistan_engine = create_engine('mysql+pymysql:{}/pakistan_fox?charset=utf8'.format(host_data))
data_Pakistan_df.to_sql(name='day_user_organization', con=data_Pakistan_engine, if_exists='append', index=None)

## 菲律宾

sql_Philippines_data = '''SELECT
                    DATE (
                    now()) AS '日期',
                    SUBSTRING_INDEX( uo.`asset_group_name`, ',',- 1 ) '组别',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_names`, ',', 2 ), ',',- 1 ) '主管',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_ids`, ',', 2 ), ',',- 1 ), '_',- 1 ) as '主管ID',
                    uo.parent_user_name '组长',
                    uo.parent_user_id '组长ID',
                    b.co_NO AS '工号',
                    uo.`name` AS '催员',
                    uo.user_id AS '催员ID',
                    b.del_date AS '账号删除日期',
                    c.dimission_date AS '业绩截止日期',
                    b.del_flag AS '离职标志' 
                FROM
                    user_organization uo
                    LEFT JOIN ( SELECT DISTINCT NAME, NO AS TL_NO FROM sys_user ) AS cc ON uo.parent_user_name = cc.
                    NAME LEFT JOIN ( SELECT DISTINCT id, NO AS co_NO, t.del_date, t.del_flag FROM sys_user t ) AS b ON uo.user_id = b.id
                    LEFT JOIN ( SELECT DISTINCT user_id, dimission_date FROM sys_user_extend ) AS c ON uo.user_id = c.user_id
                    LEFT JOIN ( SELECT DISTINCT user_id, min( online_day ) online_day FROM online_days WHERE online_day_flag = 1 GROUP BY user_id ) AS d ON uo.user_id = d.user_id 
                WHERE
                    (
                        c.dimission_date > '{}' 
                    OR b.del_flag = 0)
                    '''.format(last_two_months)

data_Philippines_df = config.philippines_fox_engine_read(sql_Philippines_data)
host_data = '//root:weidu:001A@172.16.1.250:3306'
data_Philippines_engine = create_engine('mysql+pymysql:{}/philippines_fox?charset=utf8'.format(host_data))
data_Philippines_df.to_sql(name='day_user_organization', con=data_Philippines_engine, if_exists='append', index=None)

## 墨西哥

sql_Mexico_data = '''SELECT
                    DATE (
                    now()) AS '日期',
                    SUBSTRING_INDEX( uo.`asset_group_name`, ',',- 1 ) '组别',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_names`, ',', 2 ), ',',- 1 ) '主管',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_ids`, ',', 2 ), ',',- 1 ), '_',- 1 ) as '主管ID',
                    uo.parent_user_name '组长',
                    uo.parent_user_id '组长ID',
                    b.co_NO AS '工号',
                    uo.`name` AS '催员',
                    uo.user_id AS '催员ID',
                    b.del_date AS '账号删除日期',
                    c.dimission_date AS '业绩截止日期',
                    b.del_flag AS '离职标志' 
                FROM
                    user_organization uo
                    LEFT JOIN ( SELECT DISTINCT NAME, NO AS TL_NO FROM sys_user ) AS cc ON uo.parent_user_name = cc.
                    NAME LEFT JOIN ( SELECT DISTINCT id, NO AS co_NO, t.del_date, t.del_flag FROM sys_user t ) AS b ON uo.user_id = b.id
                    LEFT JOIN ( SELECT DISTINCT user_id, dimission_date FROM sys_user_extend ) AS c ON uo.user_id = c.user_id
                    LEFT JOIN ( SELECT DISTINCT user_id, min( online_day ) online_day FROM online_days WHERE online_day_flag = 1 GROUP BY user_id ) AS d ON uo.user_id = d.user_id 
                WHERE
                    (
                        c.dimission_date > '{}' 
                    OR b.del_flag = 0)
                    '''.format(last_two_months)

data_Mexico_df = config.mexico_fox_engine_read(sql_Mexico_data)
host_data = '//root:weidu:001A@172.16.1.250:3306'
data_Mexico_engine = create_engine('mysql+pymysql:{}/mexico_fox?charset=utf8'.format(host_data))
data_Mexico_df.to_sql(name='day_user_organization', con=data_Mexico_engine, if_exists='append', index=None)


## 泰国数据

sql_Thailand_data = '''SELECT
                    DATE (
                    now()) AS '日期',
                    SUBSTRING_INDEX( uo.`asset_group_name`, ',',- 1 ) '组别',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_names`, ',', 2 ), ',',- 1 ) '主管',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_ids`, ',', 2 ), ',',- 1 ), '_',- 1 ) as '主管ID',
                    uo.parent_user_name '组长',
                    uo.parent_user_id '组长ID',
                    b.co_NO AS '工号',
                    uo.`name` AS '催员',
                    uo.user_id AS '催员ID',
                    b.del_date AS '账号删除日期',
                    c.dimission_date AS '业绩截止日期',
                    b.del_flag AS '离职标志' 
                FROM
                    user_organization uo
                    LEFT JOIN ( SELECT DISTINCT NAME, NO AS TL_NO FROM sys_user ) AS cc ON uo.parent_user_name = cc.
                    NAME LEFT JOIN ( SELECT DISTINCT id, NO AS co_NO, t.del_date, t.del_flag FROM sys_user t ) AS b ON uo.user_id = b.id
                    LEFT JOIN ( SELECT DISTINCT user_id, dimission_date FROM sys_user_extend ) AS c ON uo.user_id = c.user_id
                    LEFT JOIN ( SELECT DISTINCT user_id, min( online_day ) online_day FROM online_days WHERE online_day_flag = 1 GROUP BY user_id ) AS d ON uo.user_id = d.user_id 
                WHERE
                    (
                        c.dimission_date > '{}' 
                    OR b.del_flag = 0)
                    '''.format(last_two_months)

data_Thailand_df = config.thailand_fox_engine_read(sql_Thailand_data)
host_data = '//root:weidu:001A@172.16.1.250:3306'
data_Thailand_engine = create_engine('mysql+pymysql:{}/thailand_fox?charset=utf8'.format(host_data))
data_Thailand_df.to_sql(name='day_user_organization', con=data_Thailand_engine, if_exists='append', index=None)

## 印尼数据

sql_indonesia_data = '''SELECT
                    DATE (
                    now()) AS '日期',
                    SUBSTRING_INDEX( uo.`asset_group_name`, ',',- 1 ) '组别',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_names`, ',', 2 ), ',',- 1 ) '主管',
                    SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX( uo.`parent_user_ids`, ',', 2 ), ',',- 1 ), '_',- 1 ) as '主管ID',
                    uo.parent_user_name '组长',
                    uo.parent_user_id '组长ID',
                    b.co_NO AS '工号',
                    uo.`name` AS '催员',
                    uo.user_id AS '催员ID',
                    b.del_date AS '账号删除日期',
                    c.dimission_date AS '业绩截止日期',
                    b.del_flag AS '离职标志' 
                FROM
                    user_organization uo
                    LEFT JOIN ( SELECT DISTINCT NAME, NO AS TL_NO FROM sys_user ) AS cc ON uo.parent_user_name = cc.
                    NAME LEFT JOIN ( SELECT DISTINCT id, NO AS co_NO, t.del_date, t.del_flag FROM sys_user t ) AS b ON uo.user_id = b.id
                    LEFT JOIN ( SELECT DISTINCT user_id, dimission_date FROM sys_user_extend ) AS c ON uo.user_id = c.user_id
                    LEFT JOIN ( SELECT DISTINCT user_id, min( online_day ) online_day FROM online_days WHERE online_day_flag = 1 GROUP BY user_id ) AS d ON uo.user_id = d.user_id 
                WHERE
                    (
                        c.dimission_date > '{}' 
                    OR b.del_flag = 0)
                    '''.format(last_two_months)

data_Thailand_df = config.indonesia_fox_engine_read(sql_indonesia_data)
host_data = '//root:weidu:001A@172.16.1.250:3306'
data_Thailand_engine = create_engine('mysql+pymysql:{}/indonesia_fox?charset=utf8'.format(host_data))
data_Thailand_df.to_sql(name='day_user_organization', con=data_Thailand_engine, if_exists='append', index=None)




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

