import send_email as se

# 示例用法
sender_email = 'liufengfang@weidu.ac.cn'
sender_alias = '"策略数据组" <liufengfang@weidu.ac.cn>'
sender_password = 'pp6M89B5RJTdwRwD'
recipients = ['lichongqing@weidu.ac.cn', 'liufengfang@weidu.ac.cn']
subject = '这是一个测试邮件'
body = ''
attachment_path = 'D:\唯渡脚本\WeiDuPython\liufengfang\jupyter notebook 脚本\委外日报脚本\菲律宾委外排名数据2024-08-04.xlsx'

se.send_bulk_emails_with_attachment(sender_email, sender_password, recipients, subject, body, attachment_path)