#!/usr/bin/env python

import imaplib
import sys, signal
from avro import schema, datafile, io
import os, re
import email
import inspect, pprint
import getopt
import time
from lepl.apps.rfc3696 import Email

is_email = Email()

class GmailSlurper(object):
  
  def __init__(self):
    """Method docstring."""

  def init_directory(self, directory):
    if os.path.exists(directory):
      print 'Warning: %(directory)s already exists:' % {"directory":directory}
    else:
      os.makedirs(directory)
    return directory
  
  def init_imap(self, username, password):
    self.username = username
    self.password = password
    try:
      imap.shutdown()
    except:
      pass
    self.imap = imaplib.IMAP4_SSL('imap.gmail.com', 993)
    self.imap.login(username, password)
    self.imap.is_readonly = True
  
  # part_id will be helpful one we're splitting files among multiple slurpers
  def init_avro(self, output_path, part_id, schema_path):
    output_dir = None
    if(type(output_path) is str):
      output_dir = self.init_directory(output_path)
    out_filename = '%(output_dir)s/part-%(part_id)s.avro' % \
      {"output_dir": output_dir, "part_id": str(part_id)}
    self.schema = open(schema_path, 'r').read()
    email_schema = schema.parse(self.schema)
    rec_writer = io.DatumWriter(email_schema)
    self.avro_writer = datafile.DataFileWriter(
      open(out_filename, 'wb'),
      rec_writer,
      email_schema
    )
  
  def init_folder(self, folder):
    self.imap_folder = folder
    status, count = self.imap.select(folder)      
    print "FOLDER SELECT STATUS: " + status
    if(status == 'OK'):
      count = int(count[0])
      ids = range(1,count)
      ids.reverse()
      self.id_list = ids
      print "FOLDER COUNT: " + str(count)
      self.folder_count = count
    return status
    
  def fetch_email(self, email_id):
    def timeout_handler(signum, frame):
      raise self.TimeoutException()
    
    signal.signal(signal.SIGALRM, timeout_handler) 
    signal.alarm(30) # triger alarm in 30 seconds
    
    avro_record = {}
    status = 'FAIL'
    try:
      status, data = self.imap.fetch(str(email_id), '(X-GM-THRID RFC822)') # Gmail's X-GM-THRID will get the thread of the message
    except self.TimeoutException:
      return 'TIMEOUT', {}, None
    except:
      return 'ABORT', {}, None
    
    charset = None
    if status != 'OK':
      return 'ERROR', {}, None
    else:
      raw_thread_id = data[0][0]
      encoded_email = data[0][1]
    try:
      charset = self.get_charset(encoded_email)
      raw_email = encoded_email.decode(charset)
      thread_id = self.get_thread_id(raw_thread_id)
      
      avro_record = {'thread_id': thread_id, 'raw_email': raw_email}
      #msg = email.message_from_string(raw_email)
      #avro_parts, charset = process_email(msg, thread_id)
    except UnicodeDecodeError:
      return 'UNICODE', {}, charset
    except:
      return 'PARSE', {}, charset
      
    #if not avro_parts.has_key('froms'):
    #  return 'FROM', {}, charset
      
    # Without a charset we pass bad chars to avro, and it dies. See AVRO-565.
    if charset:
      return status, avro_record, charset
    else:
      return 'CHARSET', {}, charset
  
  def parse_addrs(self, addr_string):
    if addr_string:
      addresses = email.utils.getaddresses([addr_string])
      validated = []
      for address in addresses:
        address_pair = {'real_name': None, 'address': None}
        if address[0]:
          address_pair['real_name'] = address[0]
        if is_email(address[1]):
          address_pair['address'] = address[1]
        if not address[0] and not is_email(address[1]):
          pass
        else:
          validated.append(address_pair)
      if(len(validated) == 0):
        validated = None
      return validated
  
  def strip_brackets(self, message_id):
    return str(message_id).strip('<>')
  
  def parse_date(self, date_string):
    tuple_time = email.utils.parsedate(date_string)
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S", tuple_time)
    return iso_time
  
  def get_charset(self, raw_email):
    if(type(raw_email)) is str:
      raw_email = email.message_from_string(raw_email)
    else:
      raw_email = raw_email
    charset = None
    for c in raw_email.get_charsets():
      if c != None:
        charset = c
        break
    return charset
  
  def process_email(self, msg, thread_id):
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
      'thread_id': get_thread_id(thread_id),
      'in_reply_to': strip_brackets(msg['In-Reply-To']),
      'subject': subject,
      'date': parse_date(msg['Date']),
      'body': body,
      'froms': parse_addrs(msg['From']),
      'tos': parse_addrs(msg['To']),
      'ccs': parse_addrs(msg['Cc']),
      'bccs': parse_addrs(msg['Bcc']),
      'reply_tos': parse_addrs(msg['Reply-To'])
    }
    return avro_parts, charset
  
  # '1011 (X-GM-THRID 1292412648635976421 RFC822 {6499}' --> 1292412648635976421
  def get_thread_id(self, thread_string):
    p = re.compile('\d+ \(X-GM-THRID (.+) RFC822.*')
    m = p.match(thread_string)
    return m.group(1)
  
  def get_body(self, msg):
    body = ''
    if msg:
      for part in msg.walk():
        if part.get_content_type() == 'text/plain':
          body += part.get_payload()
    return body
  
  def shutdown(self):
    self.avro_writer.close()
    self.imap.close()
    self.imap.logout()

  def write(self, record):
    self.avro_writer.append(record)
    self.avro_writer.flush()
  
  def slurp(self):
    if(self.imap and self.imap_folder):
      for email_id in self.id_list:
        status, email_hash, charset = self.fetch_email(email_id)
        
        if(status == 'OK' and charset and email_hash.has_key('thread_id') and email_hash['raw_email']):
          print email_id, charset, email_hash['thread_id']
          self.write(email_hash)
        elif(status == 'ERROR' or status == 'PARSE' or status == 'UNICODE' or status == 'CHARSET' or status =='FROM'):
          sys.stderr.write(status + "\n")
          continue
        elif (status == 'ABORT' or status == 'TIMEOUT'):
          sys.stderr.write("resetting imap for " + status + "\n")
          stat = self.reset()
          sys.stderr.write("IMAP RESET: " + stat + "\n")
        else:
          continue
  
  def reset(self):
    self.init_imap(self.username, self.password)
    status = self.init_folder(self.imap_folder)
    return status
  
  class TimeoutException(Exception): 
    """Indicates an operation timed out."""
    pass

def usage(context):
  print """Usage: gmail.py -m <mode: interactive|automatic> -u <username@gmail.com> -p <password> -s <schema_path> -f <imap_folder> -o <output_path>"""

def does_exist(path_string, name):
  if(os.path.exists(path_string)):
    pass
  else:
    print "Error: " + name + ": " + path_string + " does not exist."
    sys.exit(2)

def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:], 'm:u:p:s:f:o:')
  except getopt.GetoptError, err:
    # print help information and exit:
    print "Error:" + str(err) # will print something like "option -a not recognized"
    usage("getopt")
    sys.exit(2)
  
  mode = None
  username = None
  password = None
  schema_path = None #'../avro/email.schema'
  imap_folder = None #'[Gmail]/All Mail'
  output_path = None
  arg_check = dict()
  
  for o, a in opts:
    if o == "-m":
      mode = a
      if mode in ('automatic', 'interactive'):
        pass
      else:
        usage('opts')
        sys.exit(2)
      arg_check[o] = 1
    elif o in ("-u"):
      username = a
      arg_check[o] = 1
    elif o in ("-p"):
      password = a
      arg_check[o] = 1
    elif o in ("-s"):
      schema_path = a
      does_exist(schema_path, "filename")
      arg_check[o] = 1
    elif o in ("-f"):
      imap_folder = a
      arg_check[o] = 1
    elif o in ("-o"):
      output_path = a
      arg_check[o] = 1
    else:
      assert False, "unhandled option"
    
  if(len(arg_check.keys()) == len(sys.argv[1:])/2):
    pass
  else:
    usage()
    sys.exit(2)
  slurper = GmailSlurper()
  slurper.init_avro(output_path, 1, schema_path)
  slurper.init_imap(username, password)
  status = slurper.init_folder(imap_folder)
  if(status == 'OK'):
    if(mode == 'automatic'):
      print "Connected to folder " + imap_folder + "and downloading " + str(slurper.folder_count) + " emails..."
      slurper.slurp()  
      slurper.shutdown()
  else:
    print "Problem initializing imap connection."

if __name__ == "__main__":
  main()
