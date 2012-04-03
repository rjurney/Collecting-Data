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
emails = db.emails
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
  email = emails.find_one({"message_id": message_id})
  return render_template('partials/email.html', email=email)

@app.route("/email/search/<query>")
def search_email(query):
  result = elastic.search(query, indexes=["emails"])
  hits = result['hits']['hits']
  jstring = json.dumps(hits, sort_keys=True, indent=4)
  return jstring, 200, {'Content-Type': 'application/json; charset=utf-8'}

if __name__ == "__main__":
  app.run(debug=True)
