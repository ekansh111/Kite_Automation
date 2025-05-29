"""
This script is designed to send an email with a CSV attachment using the SMTP protocol.

**Main Functionalities:**

1. **Email Configuration:**
   - Imports email configuration details such as `subject`, `body`, `sender_email`, `recipient_email`, and `sender_password` from the `Email_Config` module.
   - Ensures that sensitive information like `sender_password` is kept secure and not hard-coded in the script.

2. **Email Composition:**
   - Creates a multipart email message (`MIMEMultipart`) to support both text content and attachments.
   - Sets the email's sender, recipient, and subject line, which includes the current date.

3. **Attachment Handling:**
   - Converts a pandas DataFrame (`csv_data`) into CSV format using an in-memory buffer (`StringIO`).
   - Attaches the CSV data to the email as a file, naming it with the current date for clarity.

4. **Email Sending:**
   - Establishes a secure connection with the Gmail SMTP server using SSL.
   - Logs into the sender's email account using credentials imported from `Email_Config`.
   - Sends the email to the recipient and handles any exceptions that may occur during the process.
   - Closes the server connection after sending the email.

5. **Usage:**
   - The `send_email` function is the main entry point for sending the email. It requires a pandas DataFrame (`csv_data`) as input.
   - When run as the main program, it demonstrates how to call `send_email` with a sample DataFrame.

**Notes:**

- **Dependencies:**
  - Standard libraries: `smtplib`, `email` modules (`MIMEMultipart`, `MIMEText`, `MIMEBase`, `encoders`), `datetime`, `io`, `pandas`.
  - Custom module: `Email_Config` (must contain `subject`, `body`, `sender_email`, `recipient_email`, `sender_password`).
- **Security Considerations:**
  - Ensure that `Email_Config` is secure and does not expose sensitive information.
  - Avoid hard-coding credentials; consider using environment variables or secure credential storage.
  - Be cautious with exception handling to avoid exposing sensitive information in error messages.

"""

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

def SendConfigurableMail(PayloadDataframe,MailDetails):

    '''#saving the data in csv format
    hist = get_data()
    csv_buffer = StringIO()
    hist.to_csv(csv_buffer)
    PayloadDataframe = csv_buffer.getvalue()'''
    print('Mail details below')
    print(MailDetails)
    print(MailDetails['From'])
    msg = MIMEMultipart()
    msg['From'] = MailDetails['From']
    msg['To'] = MailDetails['To']


    # Update the email subject to include today's date
    msg['Subject'] = MailDetails['Subject']

    # Email body
    body = MailDetails['Body']
    msg.attach(MIMEText(body, 'plain'))
   
    for name, df in PayloadDataframe.items():
      part = MIMEBase('application', 'octet-stream')
      csv_buffer = StringIO()
      df.to_csv(csv_buffer, index=False)
      AttachedFileName = name
      part.set_payload(csv_buffer.getvalue().encode('utf-8'))
      encoders.encode_base64(part)
      part.add_header('Content-Disposition', f'attachment; filename={AttachedFileName}')
      msg.attach(part)

    # Send the email
    server = None
    try:
        server = smtplib.SMTP_SSL(MailDetails['SMTPMail'], MailDetails['PortNo'])
        server.login(MailDetails['From'], MailDetails['SenderPassword'])
        server.sendmail(MailDetails['From'], MailDetails['To'], msg.as_string())
        print('Email sent successfully!')
    except Exception as e:
        print(f'Failed to send email: {e}')
    finally:
        server.quit()


if __name__ == "__main__":
    send_email()
    

