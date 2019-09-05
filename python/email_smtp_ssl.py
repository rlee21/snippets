import smtplib
import ssl

PORT = 465
SMTP_SERVER = 'smtp.gmail.com'
SENDER_EMAIL = 'example@gmail.com'  # Enter your address
RECEIVER_EMAIL = 'example@gmail.com'  # Enter receiver address
password = input('Type your password and press enter: ')
message = """\
Subject: test email

This is a test email."""

context = ssl.create_default_context()
with smtplib.SMTP_SSL(SMTP_SERVER, PORT, context=context) as server:
    server.login(SENDER_EMAIL, password)
    server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, message)

