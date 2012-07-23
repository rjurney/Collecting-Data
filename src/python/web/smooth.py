import numpy as np

class Smoother():
  
  """Takes an array of objects as input, and the data key of the object for access."""
  def __init__(self, raw_data, data_key):
    self.raw_data = raw_data
    print self.raw_data
    self.data = self.to_array(raw_data, data_key)
  
  """Given an array of objects with values, return a numpy array of values."""
  def to_array(self, in_data, data_key):
    data_array = list()
    for datum in in_data:
      print datum
      data_array.append(datum[data_key])
    return np.array(data_array)
  
  """Smoothing method from SciPy SignalSmooth Cookbook: http://www.scipy.org/Cookbook/SignalSmooth"""
  def smooth(self, window_len=10, window='blackman'):
    x = self.data
    s=np.r_[2*x[0]-x[window_len:1:-1], x, 2*x[-1]-x[-1:-window_len:-1]]
    w = getattr(np, window)(window_len)
    y = np.convolve(w/w.sum(), s, mode='same')
    self.smoothed = y[window_len-1:-window_len+1]
  
  def to_objects(self):
    objects = list()
    hours = [ '%02d' % i for i in range(24) ]
    for idx, val in enumerate(hours):
      objects.append({"sent_hour": val, "total": round(self.smoothed[idx], 0)})
    return objects

# raw_data = [{"sent_hour":"00","total":0},{"sent_hour":"01","total":0},{"sent_hour":"02","total":0},{"sent_hour":"03","total":0},{"sent_hour":"04","total":0},{"sent_hour":"05","total":0},{"sent_hour":"06","total":0},{"sent_hour":"07","total":0},{"total":16,"sent_hour":"08"},{"total":16,"sent_hour":"09"},{"total":24,"sent_hour":"10"},{"total":14,"sent_hour":"11"},{"total":6,"sent_hour":"12"},{"total":22,"sent_hour":"13"},{"total":32,"sent_hour":"14"},{"total":14,"sent_hour":"15"},{"total":10,"sent_hour":"16"},{"total":10,"sent_hour":"17"},{"total":4,"sent_hour":"18"},{"sent_hour":"19","total":0},{"total":8,"sent_hour":"20"},{"total":6,"sent_hour":"21"},{"total":20,"sent_hour":"22"},{"total":2,"sent_hour":"23"}]
# 
# s = Smoother(raw_data, 'total')
# 
# smoothed = s.smooth()
# 
# hours = [ '%02d' % i for i in range(24) ]
# print hours
# print hours.__class__
# answer = list()
# # ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']
# 
# 
# print answer
# 
# # MongoDB and BSON util
# import bson
# import pymongo
# from pymongo import Connection
# 
# import json
# 
# connection = Connection()
# db = connection.agile_data
# 
# db.sent_dist.update({"email": "billgraham@gmail.com"}, {"$set": {"sent_dist": answer}})
# 
# db.sent_dist.find_one({"email":"billgraham@gmail.com"})
# 
# 
# def gauss_kern(size, sizey=None):
#   """ Returns a normalized 2D gauss kernel array for convolutions """
#   size = int(size)
#   if not sizey:
#       sizey = size
#   else:
#       sizey = int(sizey)
#   x, y = mgrid[-size:size+1, -sizey:sizey+1]
#   g = exp(-(x**2/float(size)+y**2/float(sizey)))
#   return g / g.sum()
# 
# def blur_image(im, n, ny=None) :
#     """ blurs the image by convolving with a gaussian kernel of typical
#         size n. The optional keyword argument ny allows for a different
#         size in the y direction.
#     """
#     g = gauss_kern(n, sizey=ny)
#     improc = signal.convolve(im,g, mode='valid')
#     return(improc)
# 
# blur_image(ary_data, 5)