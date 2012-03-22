#!/usr/bin/env python
# Flask:
from pymongo import Connection
import json
from flask import Flask, render_template

app = Flask(__name__)
connection = Connection()
db = connection.agile_data

@app.route("/<input>")
def echo(input):
    return input

@app.route("/sent_counts/<ego1>/<ego2>")
def sent_counts(ego1, ego2):
    sent_count = db['sent_counts'].find_one({'ego1': ego1, 'ego2': ego2})
    data = {}
    data['keys'] = '_id', 'ego1', 'ego2', 'total'
    data['values'] = sent_count['_id'], sent_count['ego1'], sent_count['ego2'], sent_count['total']
    return render_template('table.html', data=data)

if __name__ == "__main__":
    app.run(debug=True)

# bottle.py:
# from pymongo import Connection
# import json
# from bottle import route, run
# connection = Connection()
# db = connection.agile_data
# 
# @route('/sent_counts/<from_address>/<to_address>')
# def sent_counts(from_address, to_address):
#   sent_count = db['sent_counts'].find_one({'from': from_address, 'to': to_address})
#   plain = {'from': sent_count['from'], 'to': sent_count['to'], 'total': sent_count['total']}
#   return json.dumps(plain)
# 
# run(host='localhost', port=8080)