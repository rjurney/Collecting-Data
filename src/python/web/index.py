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

# For smoothing and filling empty holes in our time series data
from filler import Filler
from smooth import Smoother

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

@app.route("/sent_distributions/<email_address>")
def sent_distributions(email_address):
  data = db.sent_dist.find_one({'email': email_address})
  chart_json = json.dumps(data['sent_dist'])
  print chart_json
  return render_template('partials/distribution.html', data=data, chart_json=chart_json)

@app.route("/email/<message_id>")
def email(message_id):
  email = emaildb.find_one({"message_id": message_id})
  print email
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

"""
Email Address Entity
"""
@app.route("/address/<string:email_address>")
def address(email_address):
  sent_dist = db.sent_dist.find_one({'email': email_address})
  smitty = Smoother(sent_dist['sent_dist'], 'total')
  smitty.smooth()
  smoothed_dist = smitty.to_objects()
  chart_json = json.dumps(smoothed_dist)
  top_friends = db.top_friends.find_one({'email': email_address})['top_20'][0:5]
  reply_ratio = db.reply_ratios.find_one({'from': 'russell.jurney@gmail.com', 'to': email_address})
  return render_template('partials/address.html', email_address=email_address,
                                                  sent_dist={"sent_dist": smoothed_dist}, 
                                                  chart_json=chart_json, 
                                                  top_friends=top_friends,
                                                  reply_ratio=reply_ratio)

"""
Display all email addresses associated with a message ID.
"""
@app.route("/address_cloud/<string:message_id>")
def address_cloud(message_id):
  record = db['addresses_per_id'].find_one({'message_id': message_id})
  data = {'values': record['email_address_messages'], 'fields': ['email_address']}
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
