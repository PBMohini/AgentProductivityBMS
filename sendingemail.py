# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 16:29:00 2017

@author: Mohini
"""

import smtplib
import mimetypes
import os
import datetime
import sqlConnection

from email.mime.multipart import MIMEMultipart
from email import encoders
#from email.message import Message
#from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
#from email.mime.image import MIMEImage
#from email.mime.text import MIMEText


folderName = 'C:\\TaskFolder\\ProductivityReportBMS\\dist'
fileName = folderName + '\\config' + '\\ResourceConfig.txt'

Config = {}
with open(fileName) as f:
    for line in f:
       (key, val) = line.split()
       Config[key] = val
       
emailto = Config["emailTo"].split(',')
emailfrom = Config["emailFrom"]
username = Config["username"]
password = Config["password"]
filenames = []

folderName += '\\' + datetime.date.today().strftime("%B %d, %Y")
prodList = Config["prodList"].split(',')
for i, val in enumerate(prodList):
    fileName = folderName + Config[val]    
    if(os.path.isfile(fileName) == False):
        sqlConnection.main(int(val))
    filenames.append(fileName)

msg = MIMEMultipart()
msg["From"] = emailfrom
msg["To"] = ", ".join(emailto)
msg["Subject"] = "BMS Agent Productivity report for Yesterday"
msg.preamble = "Agent Productivity report"

ctype, encoding = mimetypes.guess_type(filenames[0])
if ctype is None or encoding is not None:
    ctype = "application/octet-stream"
#
maintype, subtype = ctype.split("/", 1)

#if maintype == "text":
#    fp = open(fileToSend)
#    # Note: we should handle calculating the charset
#    attachment = MIMEText(fp.read(), _subtype=subtype)
#    fp.close()
#elif maintype == "image":
#    fp = open(fileToSend, "rb")
#    attachment = MIMEImage(fp.read(), _subtype=subtype)
#    fp.close()
#elif maintype == "audio":
#    fp = open(fileToSend, "rb")
#    attachment = MIMEAudio(fp.read(), _subtype=subtype)
#    fp.close()
#else:
for fileToSend in filenames:
    fp = open(fileToSend, "rb")
    attachment = MIMEBase(maintype, subtype)
    attachment.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(attachment)
    attachment.add_header("Content-Disposition", "attachment", filename=os.path.basename(fileToSend))
    msg.attach(attachment)

server = smtplib.SMTP("mx9.policybazaar.com")
server.starttls()
server.login(username,password)
server.sendmail(emailfrom, emailto, msg.as_string())
server.quit()