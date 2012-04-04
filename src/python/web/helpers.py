# helpers.py - a helper library for index.py

def get_offsets(offset1, offset2, increment):
  offsets = {}
  offsets['next'] = {'top': offset2 + increment, 'bottom': offset1 + increment}
  offsets['previous'] = {'top': offset2 - increment, 'bottom': offset2 - increment}
  return offsets