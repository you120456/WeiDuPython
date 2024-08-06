import yagmail

def send_bulk_emails_with_attachment(sender_email, sender_password, recipients, subject, body, attachment_path):
    # 初始化 yagmail.SMTP 对象
    yag = yagmail.SMTP(user=sender_email, password=sender_password,host='smtp.exmail.qq.com', port=465)

    # 发送邮件
    for recipient in recipients:
        try:
            yag.send(
                to=recipient,
                subject=subject,
                contents=body,
                attachments=attachment_path
            )
            print(f'邮件已发送到 {recipient}')
        except Exception as e:
            print(f'发送给 {recipient} 时出错: {e}')

# 示例用法
# sender_email = 'your_email@gmail.com'
# sender_password = 'your_password'
# recipients = ['recipient1@example.com', 'recipient2@example.com', 'recipient3@example.com']
# subject = '这是一个测试邮件'
# body = '这是一封测试邮件的内容。'
# attachment_path = '/path/to/your/attachment/file.txt'

# send_bulk_emails_with_attachment(sender_email, sender_password, recipients, subject, body, attachment_path)
