import pandas as pd
import sql
import os
import mail
import sc
from datetime import date,timedelta,datetime
from saveexcel import save
from dateutil.relativedelta import relativedelta
from excel导出美化 import beautify_excel
dt1 = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
# print(dt1)
tgor = pd.read_sql(sql.架构mail(dt1),sc.tg())
# ynor = pd.read_sql(sql.架构mail(dt1),sc.yn2())
mxgor = pd.read_sql(sql.架构mail(dt1),sc.mxg())
flbor = pd.read_sql(sql.架构mail(dt1),sc.flb())
bjstor = pd.read_sql(sql.架构mail(dt1),sc.bjst())
current_file_name = os.path.basename(__file__).split(".")[0]
path = "./{}".format(current_file_name)
save("{0}{1}".format(dt1, current_file_name), path, 泰国每日架构=tgor,
     # 印尼每日架构=ynor,
     墨西哥每日架构=mxgor, 菲律宾每日架构=flbor,巴基斯坦每日架构=bjstor)