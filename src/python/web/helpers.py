# helpers.py - a helper library for index.py

# Calculate email offsets for fetchig lists of emails from MongoDB
def get_offsets(offset1, offset2, increment):
  offsets = {}
  offsets['Next'] = {'top': offset2 + increment, 'bottom': offset1 + increment}
  offsets['Previous'] = {'top': offset2 - increment, 'bottom': offset1 - increment}
  return offsets