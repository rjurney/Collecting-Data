Collecting Data
===============

This is a HOWTO for collecting data in Ruby and Python applications.

Scraping your Gmail Inbox
-------------------------

### Define a schema for our emails

While it is cumbersome to define static schemas, we need only do so once with Avro.  Our data is then accessible by any of the languages or tools we will use.

Email's format is defined in [RFC-5322](http://tools.ietf.org/html/rfc5322).  A corresponding Avro schema for email looks like this:

    {
          "namespace": "agile.data.avro",
          "name": "Email",
          "type": "record",
          "fields": [
              {"name":"from", "type": "string"},
              {"name":"to","type": [{"type":"array", "items":"string"}, "null"]},
              {"name":"cc","type": [{"type":"array", "items":"string"}, "null"]},
              {"name":"bcc","type": [{"type":"array", "items":"string"}, "null"]},
              {"name":"reply-to", "type": ["string", "null"]},
              {"name":"subject", "type": ["string", "null"]},
              {"name":"body", "type": ["string", "null"]},
              {"name":"message-id", "type": ["string", "null"]}
              ]
    }

### Get an access token via xoauth.py

We first use [xoauth.py](http://google-mail-xoauth-tools.googlecode.com/svn/trunk/python/xoauth.py) to get access tokens to access our inbox via Xoauth.  Good instructions for this are [here](http://code.google.com/p/google-mail-xoauth-tools/wiki/XoauthDotPyRunThrough).

Once you follow these instructions, you'll have two values:

    oauth_token: 2/cG1NvgxA30c1xZOpPS-kWMxBqiZJ5QsH4cNlropLHt8
    oauth_token_secret: 9mk8v0qNs20Dw2zSoX6UheAn

### Scrape your Inbox

