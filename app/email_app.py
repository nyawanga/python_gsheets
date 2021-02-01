# requires python 3.6+

import os
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pickle
# from pprint import pprint
#auth_file = pickle.load(open("auth", "rb"))


def send_email(from_addr, to_addr, subject, body, auth_file=None, cc_addr=None, attachment=None):
    """A function to send an email using gmail and python. 
       If you have a 2-Factor authentication set up you need an app password.
    
    Parameters:
        - From address as string
        - To and cc addresses as string separated by comma
        - Attachment is available else is None by default.
        - auth_file should be a pickle file preferrably

    Returns:
        - None
    """

    if auth_file is None:
        password = input("enter sender email password , type N/No to exit:")
        if password.lower() == "n" or password.lower() == "no":
            print("exiting now, goodbye...")
            sys.exit(1)
        else:
            password = str(password)
    else:
        auth = pickle.load(open(auth_file, "rb"))
        password = auth['password']
        password = "".join([i for i in password[::-1]])
    from_addr = from_addr
    to_addr = to_addr                                           # can be a string comma separated
    to_cc = cc_addr                                             # should be string comma separated

    try:
        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['To'] = to_addr
        msg['Cc'] = to_cc
        msg['Subject'] = subject
        if to_cc is not None:
            recepients = [to_addr] + to_cc.split(",")
        else:
            recepients = [to_addr]
        body = body

        msg.attach(MIMEText(body, 'plain'))

        if attachment is not None:
            with open(attachment, "rb") as attachment_file:
                filename = attachment
                part = MIMEBase("application", "octet-stream")
                part.set_payload((attachment_file).read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", "attachment; filename = {filename}")
                msg.attach(part)
    except Exception as err:
        print(f"mime exception {err}")

    try:
        # server = smtplib.SMTP("smtp.gmail.com", 587)
        # server.starttls()
        server = smtplib.SMTP_SSL("smtp.googlemail.com", 465)
        server.login(from_addr, password)
        text = msg.as_string()
        print("sending email now ...")
        server.sendmail(from_addr, recepients, text)
        print("email successfully sent!")
        server.quit()
    except (smtplib.SMTPAuthenticationError, smtplib.SMTPNotSupportedError) as err:
        print(f"connection error please check password/username or internet using {from_addr} \n {err}")
    except (smtplib.SMTPSenderRefused, smtplib.SMTPRecipientsRefused) as err:
        print(f"wrong recepients {to_addr} {cc_addr} or sender {from_addr} please confirm data \n {err}")
    except smtplib.SMTPDataError as err:
        print(f"problem with the attachment or message body.\n {err}")
    except Exception as err:
        print("unknown exception.")
        print(f"{err}")
        sys.exit(1)
    finally:
        print("exiting ...")

def main():
    send_email()


if __name__ == "__main__":
    send_email(from_addr, to_addr, subject, body, auth_file="auth", cc_addr=None, attachment=None)
