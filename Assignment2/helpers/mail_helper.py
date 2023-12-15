import smtplib

gmail_user = ""
gmail_app_password = ""

def send_mail(to,subject,message):
    message = f"""From: From Shoaib <>
    To: To Shoaib <{gmail_user}>
    Subject: {subject}
    {message}
    """
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_app_password)
        server.sendmail(gmail_user, to, message)
        server.close()
        print(f'Email sent to {to}!')
    except Exception as exception:
        print("Error: %s!\n\n" % exception)