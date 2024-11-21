import json
import pandas as pd
import sql2
import sql
import mail
import sc
# with open('config.json', 'r', encoding='utf-8') as file:
#     data = json.load(file)
# b = data["10月各国主管"]["泰国"]
# df = pd.DataFrame.from_dict(b, orient='index').transpose()
# df_exploded = df.melt(var_name='Supervisor', value_name='Supervisor Reserve').explode('Supervisor Reserve')
# a = pd.json_normalize(b).transpose()

# a.to_excel("./xads.xlsx")

# print(df_exploded)
# df.to_excel("./xadas.xlsx")
first_day = "2024-11-01"
yesterday = "2024-11-11"
# sql = """
# SELECT *
# FROM fox_tmp.`组别信息`
# WHERE 年 = 2024
#       """
# mail.send_email1(title1, path1 + "/{0}{1}策略监控.xlsx".format(yesterday, current_file_name),
#                  filename1, to_email1, email_variable, email_password)
# print(pd.read_sql(sql.添加主管(yesterday),sc.flb()))
# print(pd.read_sql(sql2.组长过程指标1(first_day,yesterday),sc.bjst()))
print(pd.read_sql(sql.案件量监控2(first_day,yesterday),sc.tg()))