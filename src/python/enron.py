import sys

def extract_fields(header_string):
  fields = header_string.split(', ')
  for field in fields:
    t, address = field.split(':')
    return t, address

# raw_emails: {message_id: chararray,date: chararray,from: chararray,to_cc_bcc: chararray,subject: chararray,body: chararray}
#for line in sys.stdin:
#  message_id, date, from_address, headers, subject, body = line.split('\t')
#  try:
#    t, address = extract_fields(headers)
#    sys.stdout.write(message_id + "\t" + t + "\t" + address + "\n")
#  except:
#    sys.stderr.write("Failed to parse field: " + headers + "\n")
#    pass

# Extracts the hour from an iso8601 datetime string
@outputSchema("hour:chararray")
def hour(iso_string):
  tuple_time = time.strptime(iso_string, "%Y-%m-%dT%H:%M:%S")
  return str(tuple_time[2])
