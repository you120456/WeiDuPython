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
        .indented {
          margin-left: 20px; /* 设置缩进的距离，可以根据需要调整 */
        }
      </style>
    </head>
    <body>
      <p>你好，附件为日报数据,请查收附件。
      </p>
    </body>
    </html>
    """

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