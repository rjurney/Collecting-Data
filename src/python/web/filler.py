class Filler():
  def __init__(self):
    self
  
  def fill_in_blanks(self, in_data):
    out_data = list()
    hours = [ '%02d' % i for i in range(24) ]
    for hour in hours:
      entry = [x for x in in_data if x['sent_hour'] == hour]
      if entry:
        out_data.append(entry[0])
      else:
        out_data.append({'sent_hour': hour, 'total': 0})
    return out_data

