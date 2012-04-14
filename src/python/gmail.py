#!/usr/bin/env python

import imaplib
import sys, signal
from avro import schema, datafile, io
import os, re
import email
import inspect
import pprint
import time
from lepl.apps.rfc3696 import Email

is_email = Email()

def init_directory(directory):
  if os.path.exists(directory):
    print 'Warning: %(directory)s already exists. Replacing it.' % {"directory":directory}
  else:
    os.makedirs(directory)
  return directory

def init_imap(username, password, folder):
  try:
    imap.shutdown()
  except:
    pass
  imap = imaplib.IMAP4_SSL('imap.gmail.com', 993)
  imap.login(username, password)
  status, count = imap.select(folder)
  return imap, count

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

def fetch_email(imap, id):
  
  def timeout_handler(signum, frame):
    raise TimeoutException()
  
  signal.signal(signal.SIGALRM, timeout_handler) 
  signal.alarm(30) # triger alarm in 30 seconds
  
  avro_parts = {}
  status = 'FAIL'
  try:
    status, data = imap.fetch(id, '(RFC822)')
  except TimeoutException:
    return 'TIMEOUT', {}, None
  except imap.abort, e:
    return 'ABORT', {}, None
  
  if status != 'OK':
    return 'ERROR', {}, None
  else:
    raw_email = data[0][1]
  try:
    msg = email.message_from_string(raw_email)
    avro_parts, charset = process_email(msg)
  except UnicodeDecodeError:
    return 'UNICODE', {}, charset
  except:
    return 'PARSE', {}, charset
    
  if not avro_parts.has_key('from'):
    return 'FROM', {}, charset
    
  # Without a charset we pass bad chars to avro, and it dies. See AVRO-565.
  if charset:
    return status, avro_parts, charset
  else:
    return 'CHARSET', {}, charset

def parse_addrs(addr_string):
  if addr_string:
    addresses = email.utils.getaddresses([addr_string])
    validated = []
    for address in addresses:
      if is_email(address[1].lower()):
        validated.append(address[1].lower())
      else:
        print "Invalid Email: " + address[1]#.lower()
    return validated

def strip_brackets(message_id):
  return str(message_id).strip('<>')

def parse_date(date_string):
  tuple_time = email.utils.parsedate(date_string)
  iso_time = time.strftime("%Y-%m-%dT%H:%M:%S", tuple_time)
  return iso_time

def process_email(msg):
  
  subject = msg['Subject']
  body = get_body(msg)
  
  # Without handling charsets, corrupt avros will get written
  charsets = msg.get_charsets()
  charset = None
  for c in charsets:
    if c != None:
      charset = c
      break
  
  if charset:
    subject = subject.decode(charset)#.encode('utf-8')
    body = body.decode(charset)#.encode('utf-8')
  else:
    return {}, charset
  
  avro_parts = {
    'message_id': strip_brackets(msg['Message-ID']),
    'from': parse_addrs(msg['From']),
    'to': parse_addrs(msg['To']),
    'cc': parse_addrs(msg['Cc']),
    'bcc': parse_addrs(msg['Bcc']),
    'reply_to': parse_addrs(msg['Reply-To']),
    'in_reply_to': strip_brackets(msg['In-Reply-To']),
    'subject': subject,
    'date': parse_date(msg['Date']),
    'body': body
  }
  return avro_parts, charset

def get_body(msg):
  body = ''
  if msg:
    for part in msg.walk():
      if part.get_content_type() == 'text/plain':
        body += part.get_payload()
  return body

class TimeoutException(Exception): 
  """Indicates an operation timed out."""
  pass

# MAIN
mode = None
username = None
password = None
output_dir = None

# If there aren't enough command line variables...
if (len(sys.argv) < 5):
  env_set = 0
  # Count that we have full environment variables setup
  for key in ['GMAIL_USERNAME', 'GMAILPASS', 'OUTPUTDIR']:
    if os.environ[key]:
      env_set += 1
  # If we have complete ENV defaults, we can run...
  if env_set == 3:
    mode = 'interactive'
    username = os.environ['GMAIL_USERNAME']
    password = os.environ['GMAILPASS']
    output_dir = init_directory(os.environ['OUTPUTDIR'])
    print "Interactive IMAP mode setup..."
  # If we don't have ENV, we must have command line arguments
  else:
    print """Usage: gmail.py <mode: interactive|automatic> <username@gmail.com> <password> <output_directory>"""
    exit(0)
# If there are enough command line variables, set em
else:
  mode = sys.argv[1]
  username = sys.argv[2]
  password = sys.argv[3]
  output_dir = init_directory(sys.argv[4])

imap_folder = '[Gmail]/All Mail'
schema_path = 'email.schema'

pp = pprint.PrettyPrinter(indent=4)

avro_writer = init_avro(output_dir, 1, schema_path)
imap, count = init_imap(username, password, imap_folder)
max = int(count[0])
ids = range(1,max)
ids.reverse()

if mode == 'automatic':
  for id in ids:
    status, email_hash, charset = fetch_email(imap, str(id))
    
    if(status == 'OK' and charset):
      print id, charset, str(email_hash['from'])
      avro_writer.append(email_hash)
    elif(status == 'ERROR' or status == 'PARSE' or status == 'UNICODE' or status == 'CHARSET' or status =='FROM'):
      sys.stderr.write(status + "\n")
      continue
    elif (status == 'ABORT' or status == 'TIMEOUT'):
      sys.stderr.write("resetting imap for " + status + "\n")
      imap, count = init_imap(username, password, imap_folder)
      sys.stderr.write("IMAP RESET\n")
    else:
      continue

avro_writer.close()

imap.close()
imap.logout()

