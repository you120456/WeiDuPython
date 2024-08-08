#!/usr/bin/env python
# coding: utf-8


import pymysql
import mysql.connector
import datetime
import pandas as pd
import warnings
from sqlalchemy import create_engine
import config

warnings.filterwarnings("ignore")
print("脚本2：'国内短假折算'\n开始执行!", datetime.datetime.now())
now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
time_end = now
time_start = time_end - datetime.timedelta(1)
# time_start = now.replace(year=2023, month=10, day=2) # 开始日期
# time_end = now.replace(year=2023, month=10, day=3) # 数据截止日期
month_start = time_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
print('数据时间范围{}'.format(str(time_start)[0:10]))

sql = '''SELECT
                *
            FROM
                `a_card` '''

recovery_df = config.thailand_fox_engine_read(sql)
print(1)



