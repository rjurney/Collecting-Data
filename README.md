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

### Scrape your Inbox

Setup:
    cd src/ruby
    sudo gem install bundler
    bundle install
    


Execute!:
    bundle exec bin/scrape_mail <email_address> <password> <message_count> <output_filename>
