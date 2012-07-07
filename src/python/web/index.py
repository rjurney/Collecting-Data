#!/usr/bin/env python

# Flask:
from flask import Flask, render_template, request, redirect

# MongoDB and BSON util
import bson
import pymongo
from pymongo import Connection

# ElasticSearch
import json, pyelasticsearch

# Simple configuration and helpers
import config, helpers
from helpers import *

# Setup Flask
app = Flask(__name__)

connection = Connection()
db = connection.agile_data
emaildb = db.emails
elastic = pyelasticsearch.ElasticSearch(config.ELASTIC_URL)

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
default_offsets={'offset1': 0, 'offset2': 0 + config.EMAIL_RANGE}
@app.route('/', defaults=default_offsets)
@app.route('/emails', defaults=default_offsets)
@app.route('/emails/', defaults=default_offsets)
@app.route("/emails/<int:offset1>/<int:offset2>")
def list_emaildb(offset1, offset2):
  offset1 = int(offset1)
  offset2 = int(offset2)
  emails = emaildb.find()[offset1:offset2] # Uses a MongoDB cursor
  nav_offsets = get_offsets(offset1, offset2, config.EMAIL_RANGE)
  data = {'emails': emails, 'nav_offsets': nav_offsets, 'nav_path': '/emails/'}
  return render_template('partials/emails.html', data=data)

default_search={'offset1': 0, 'offset2': 0 + config.EMAIL_RANGE, 'query': False}
@app.route("/emails/search", defaults=default_search)
@app.route("/emails/search/", defaults=default_search)
@app.route("/emails/search/<string:query>", defaults=default_search)
@app.route("/emails/search/<string:query>/<int:offset1>/<int:offset2>")
def search_email(query, offset1, offset2):
  if query == False:
    query = request.args.get('query')
    return redirect('/emails/search/' + query + '/' + str(offset1) + '/' + str(offset2))
  doc_count = offset2 - offset1
  results = elastic.search(query, {'from': offset1, 'size': doc_count}, indexes=["email"])
  emails = process_results(results)
  nav_offsets = get_offsets(offset1, offset2, config.EMAIL_RANGE)
  data = {'emails': emails, 'nav_offsets': nav_offsets, 'nav_path': '/emails/search/', 'query': query}
  return render_template('partials/emails.html', data=data)

@app.route("/address/<string:email_address>")
def address(email_address):
  record = db['ids_per_address'].find_one({'email_address': email_address})
  data = {'values': record['email_address_messages'], 'fields': ['email_address', 'message_id'], 'email_address': email_address, 'nav_path': '/address/'}
  return render_template('partials/cloud.html', data=data)

# default_offsets={'offset1': 0, 'offset2': 0 + config.EMAIL_RANGE, 'email': False}
# @app.route("/address/<string:email>", defaults=default_offsets)
# @app.route("/address/<string:email>/", defaults=default_offsets)
# @app.route("/address/<string:email>/<int:offset1>/<int:offset2>")
# def address(email, offset1, offset2):
#   print "email: " + str(email)
#   # if email == False:
#   #   email = request.args.get('email')
#   #   return redirect('/address/' + email + '/' + str(offset1) + '/' + str(offset2))
#   doc_count = offset2 - offset1
#   query = {'query': {'match_all': {}}, 'facets': {'froms': {'from': {'address': email}}}}
#   results = elastic.search(query, {'from': offset1, 'size': doc_count, 'sort': {'date': {'order': 'desc'}}}, indexes=["email"])
#   print "results"
#   print results
#   emails = process_results(results)
#   nav_offsets = get_offsets(offset1, offset2, config.EMAIL_RANGE)
#   data = {'emails': emails, 'nav_offsets': nav_offsets, 'nav_path': '/address/' + email + '/'}
#   #print data
#   return render_template('partials/emails.html', data=data)

if __name__ == "__main__":
  app.run(debug=True)
