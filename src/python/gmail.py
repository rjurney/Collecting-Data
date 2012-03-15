#!/usr/bin/env python

import imaplib
import sys
from avro import schema, datafile, io
import os
import email
import inspect
import pprint

def init_directory(directory):
  if os.path.exists(directory):
    print '%(directory)s already exists' % {"directory":directory}
  else:
    os.makedirs(directory)
  return directory

def init_avro(output_dir, part_id, schema_path):
  out_filename = '%(output_dir)s/part-%(part_id)s.avro' % \
    {"output_dir": output_dir, "part_id": str(part_id)}
  schema_string = linestring = open(schema_path, 'r').read()
  email_schema = schema.parse(schema_string)
  rec_writer = io.DatumWriter(email_schema)
  df_writer = datafile.DataFileWriter(
    open(out_filename, 'wb'),
    rec_writer,
    email_schema
  )
  return df_writer

def write_email(writer, email):
  writer.append(email)

def get_first_text_part(msg):
  maintype = msg.get_content_maintype()
  if maintype == 'multipart':
    for part in msg.get_payload():
      if part.get_content_maintype() == 'text':
        return part.get_payload()
  elif maintype == 'text':
    return msg.get_payload()

def walk_msg(msg):
  for part in msg.walk():
    if part.get_content_type() == "multipart/alternative":
      continue
    yield part.get_payload(decode=1)

# # MAIN
# if (len(sys.argv) < 4):
#   print """Usage: gmail.py <username@gmail.com> <password> <output_directory>"""
#   exit(0)
#
# user_name = sys.argv[1]
# password = sys.argv[2]
# directory = init_directory(sys.argv[3])

pp = pprint.PrettyPrinter(indent=4)

user_name = 'russell.jurney@gmail.com'
password = 'K4mikazi!'
schema_path = '/me/Collecting-Data/src/avro/email.schema'
output_dir = init_directory('/tmp/python')
avro_writer = init_avro(output_dir, 1, schema_path)
imap_folder = '[Gmail]/All Mail'

imap = imaplib.IMAP4_SSL('imap.gmail.com', 993)
imap.login(user_name,password)
status, count = imap.select(imap_folder)
status, data = imap.fetch(count[0], '(RFC822)')
raw_email = data[0][1]
email = email.message_from_string(raw_email)
text_parts = walk_msg(email)

avro_parts = {
  'message_id': email['Message-ID'],
  'from': email['From'],
  'to': email['To'],
  'cc': email['Cc'],
  'bcc': email['Bcc'],
  'reply_to': email['Reply-To'],
  'in_reply_to': email['In-Reply-To'],
  'subject': email['Subject'],
  'date': email['Date']
}

writer.append({'message_id': '4fwegaeg@daofma.com-7',
               'from': ['russell@gmail.com'],
               'to': ['bob@bob.com'],
               'subject': 'Testing python!',
               'date': '2001'})

imap.close()
imap.logout()


