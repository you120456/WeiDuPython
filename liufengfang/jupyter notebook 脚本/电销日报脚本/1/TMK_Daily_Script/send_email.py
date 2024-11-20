import yagmail


def send_bulk_emails_with_attachment(recipients, subject, body, attachment_path):
    sender_email = 'shwd_operation@weidu.ac.cn'
    sender_password = 'Shanghai1015'

    # 初始化 yagmail.SMTP 对象
    yag = yagmail.SMTP(user=sender_email, password=sender_password, host='smtp.exmail.qq.com', port=465)

    # 发送邮件
    yag.send(
        to=recipients,
        subject=subject,
        contents=body,
        attachments=attachment_path
    )
