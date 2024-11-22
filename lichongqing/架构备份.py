import sc
import sql
import pandas as pd
from datetime import date,timedelta,datetime
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
# yesterday = "2024-10-10"
try:
    pd.read_sql(sql.备份(yesterday),sc.tg())
    pd.read_sql(sql.备份(yesterday), sc.bjst())
    # pd.read_sql(sql.修改(yesterday), sc.tg())
except Exception as e:
    pass