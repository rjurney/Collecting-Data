from pymongo import Connection
import json
from bottle import route, run
connection = Connection()
db = connection.agile_data

@route('/sent_counts/<from_address>/<to_address>')
def sent_counts(from_address, to_address):
  sent_count = db['sent_counts'].find_one({'from': from_address, 'to': to_address})
  plain = {'from': sent_count['from'], 'to': sent_count['to'], 'total': sent_count['total']}
  return json.dumps(plain)

run(host='localhost', port=8080)