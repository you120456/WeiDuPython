from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
from smtplib import SMTP_SSL
def send_email(title,path,filename,to_email,email_variable,email_password):
# 邮件主题
    mail_title = title
    sender_email = email_variable
    sender_password = email_password
    # 邮件正文


    mail_content = """
        <!DOCTYPE html>
        <html>
        <head>
          <style>
            .indented {{
              margin-left: 20px; /* 设置缩进的距离，可以根据需要调整 */
            }}
          </style>
        </head>
        <body>
          <p>大家好！这是 {} 数据，请查收附件！
          </p>
        </body>
        </html>
        """.format(mail_title)
# 创建邮件对象
    msg = MIMEMultipart()
    msg["Subject"] = Header(mail_title, 'utf-8')
    msg["From"] = sender_email
    msg["To"] = ",".join(to_email)

    # 添加邮件正文
    msg.attach(MIMEText(mail_content, 'html'))

    # 读取附件内容
    with open(path, 'rb') as file1:
        attachment_data1 = file1.read()
    attachment1 = MIMEApplication(attachment_data1)
    attachment1["Content-Type"] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    # attachment1["Content-Disposition"] = f'attachment;filename=("gbk", "", "稽核数据菲律宾.xlsx")}"'
    attachment1.add_header("Content-Disposition", "attachment", filename=("gbk", "",filename))
    # 添加附件到邮件
    msg.attach(attachment1)
    # 发送邮件
    smtp_server = "smtp.exmail.qq.com"
    smtp_port = 465

    with SMTP_SSL(smtp_server, smtp_port) as server:
        server.login(sender_email, sender_password)
        server.send_message(msg)

def send_email1(title,path,filename,to_email,email_variable,email_password):
# 邮件主题
    mail_title = title
    sender_email = email_variable
    sender_password = email_password
    # 邮件正文


    mail_content = """
        <!DOCTYPE html>
        <html>
        <head>
          <style>
            .indented {{
              margin-left: 20px; /* 设置缩进的距离，可以根据需要调整 */
            }}
          </style>
        </head>
        <body>
          <p>大家好！这是 {} 数据，请查收附件！<br>
          【电催】业务组和队列命名规则<br>
          1.账龄段命名<br>
          &nbsp;&nbsp;&nbsp;&nbsp;P P1 P2 P3：预提醒<br>
          &nbsp;&nbsp;&nbsp;&nbsp;A A1 A2 A3：逾期D1-D7<br>
          &nbsp;&nbsp;&nbsp;&nbsp;B B1 B2 B3：逾期D8-D30<br>
          &nbsp;&nbsp;&nbsp;&nbsp;M2：逾期D31-D60<br>
          &nbsp;&nbsp;&nbsp;&nbsp;M3：逾期D61-D90<br><br>
          2.拆分资产包<br>
          &nbsp;&nbsp;&nbsp;&nbsp;若同账龄段拆分了资产包，在账龄段名称后增加 -1 -2 -3区分<br><br>
          3.拆分队列<br>
          &nbsp;&nbsp;&nbsp;&nbsp;若同业务组拆分了委外和内催，在业务组名称后增加 -委外名称区分<br><br>
          tips:在手数据仍在修正中，待核实清楚后会通知大家。<br><br>
          指标口径定义链接:https://collabwiki.com/pages/viewpage.action?pageId=19235661<br>
        </p>
        </body>
        </html>
        """.format(mail_title)
# 创建邮件对象
    msg = MIMEMultipart()
    msg["Subject"] = Header(mail_title, 'utf-8')
    msg["From"] = sender_email
    msg["To"] = ",".join(to_email)

    # 添加邮件正文
    msg.attach(MIMEText(mail_content, 'html'))

    # 读取附件内容
    with open(path, 'rb') as file1:
        attachment_data1 = file1.read()
    attachment1 = MIMEApplication(attachment_data1)
    attachment1["Content-Type"] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    # attachment1["Content-Disposition"] = f'attachment;filename=("gbk", "", "稽核数据菲律宾.xlsx")}"'
    attachment1.add_header("Content-Disposition", "attachment", filename=("gbk", "",filename))
    # 添加附件到邮件
    msg.attach(attachment1)
    # 发送邮件
    smtp_server = "smtp.exmail.qq.com"
    smtp_port = 465

    with SMTP_SSL(smtp_server, smtp_port) as server:
        server.login(sender_email, sender_password)
        server.send_message(msg)