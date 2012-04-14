# helpers.py - a helper library for index.py

# Calculate email offsets for fetchig lists of emails from MongoDB
def get_offsets(offset1, offset2, increment):
  offsets = {}
  offsets['Next'] = {'top': offset2 + increment, 'bottom': offset1 + increment}
  offsets['Previous'] = {'top': offset2 - increment, 'bottom': offset1 - increment}
  print offsets
  return offsets

# Process hits and return email records
def process_results(results):
  emails = []
  if results['hits'] and results['hits']['hits']:
    hits = results['hits']['hits']
    for hit in hits:
      email = hit['_source']
      emails.append(hit['_source'])
  return emails
