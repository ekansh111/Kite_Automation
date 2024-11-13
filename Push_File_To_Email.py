# smtplib provides functionality to send emails using SMTP.
import smtplib
# MIMEMultipart send emails with both text content and attachments.
from email.mime.multipart import MIMEMultipart
# MIMEText for creating body of the email message.
from email.mime.text import MIMEText
# MIMEApplication attaching application-specific data (like CSV files) to email messages.
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from Email_Config import subject, body, sender_email, recipient_email, sender_password
from io import StringIO
import pandas as pd
from email import encoders
from datetime import datetime 

def send_email(csv_data):

    '''#saving the data in csv format
    hist = get_data()
    csv_buffer = StringIO()
    hist.to_csv(csv_buffer)
    csv_data = csv_buffer.getvalue()'''

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    # Get today's date in 'YYYY-MM-DD' format
    today_date = datetime.today().strftime('%Y-%m-%d')

    # Update the email subject to include today's date
    msg['Subject'] = f'Spread Data for Pairs - {today_date}'

    # Email body
    body = 'Ongoing Position data is attached for the pair of stocks.'
    msg.attach(MIMEText(body, 'plain'))

    part = MIMEBase('application', 'octet-stream')
    csv_buffer = StringIO()
    csv_data.to_csv(csv_buffer, index=False)
    part.set_payload(csv_buffer.getvalue().encode('utf-8'))
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename=Ongoing_spread_positions_{today_date}.csv')
    msg.attach(part)

    # Send the email
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        print('Email sent successfully!')
    except Exception as e:
        print(f'Failed to send email: {e}')
    finally:
        server.quit()

if __name__ == "__main__":
    send_email()
    

