#!/usr/bin/env python2.7

# Import os to get environment variables
import os

# Flask:
from flask import Flask, render_template, request, redirect

# Setup Flask
app = Flask(__name__)

# Simple echo controller
@app.route("/<input>")
def echo(input):
  return input

# Run the Flask app
if __name__ == '__main__':
  # Bind to PORT if defined, otherwise default to 5000.
  port = int(os.environ.get('PORT', 5000))
  app.run(host='0.0.0.0', port=port)
