#!/usr/bin/env python

import imaplib
import sys

if (len(sys.argv) < 2):
  """Usage: gmail.py <username@gmail.com> <password>"""

user_name = sys.argv[1]
password = sys.argv[2]

# M = imaplib.IMAP4_SSL('imap.gmail.com', 993)
# M.login('myemailaddress@gmail.com','password')
# status, count = M.select('Inbox')
# status, data = M.fetch(count[0], '(UID BODY[TEXT])')
# 
# print data[0][1]
# M.close()
# M.logout()
