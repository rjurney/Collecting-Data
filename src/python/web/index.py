#!/usr/bin/env python

# Flask:
from flask import Flask, render_template

# MongoDB and BSON util
from pymongo import Connection, json_util

# ElasticSearch
import json, pyelasticsearch

app = Flask(__name__)
connection = Connection()
db = connection.agile_data
emaildb = db.emails
elastic = pyelasticsearch.ElasticSearch('http://localhost:9200/')

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

@app.route("/email/<message_id>")
def email(message_id):
  email = emaildb.find_one({"message_id": message_id})
  return render_template('partials/email.html', email=email)

# Enable /emails and /emails/ to serve the last 20 emaildb in our inbox unless otherwise specified
default_offsets={'offset1': 0, 'offset2': 20}
@app.route('/emails', defaults=default_offsets)
@app.route('/emails/', defaults=default_offsets)
@app.route("/emails/<offset1>/<offset2>")
def list_emaildb(offset1, offset2):
  offset1 = int(offset1)
  offset2 = int(offset2)
  emails = emaildb.find()[offset1:offset2] # Uses a MongoDB cursor
  return render_template('partials/emails.html', emails=emails)

@app.route("/emails/search/<query>")
def search_email(query):
  result = elastic.search(query, indexes=["emaildb"])
  hits = result['hits']['hits']
  jstring = json.dumps(hits, sort_keys=True, indent=4)
  return jstring, 200, {'Content-Type': 'application/json; charset=utf-8'}

if __name__ == "__main__":
  app.run(debug=True)
