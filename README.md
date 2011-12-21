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
            {"name":"message_id", "type": ["string", "null"]},
            {"name":"from","type": ["string", "null"]},
            {"name":"to","type": [{"type":"array", "items":"string"}, "null"]},
            {"name":"cc","type": [{"type":"array", "items":"string"}, "null"]},
            {"name":"bcc","type": [{"type":"array", "items":"string"}, "null"]},
            {"name":"reply_to", "type": [{"type":"array", "items":"string"}, "null"]},
            {"name":"subject", "type": ["string", "null"]},
            {"name":"body", "type": ["string", "null"]},
            {"name":"date", "type": ["string", "null"]}
        ]
    }

### Scrape your Inbox

Setup:

    cd src/ruby
    sudo gem install bundler
    bundle install
    

Execute:

    bundle exec bin/scrape_mail <email_address> <password> <message_count> <output_filename>

### Processing via Pig

Generating to/from pairs from all emails:

    REGISTER /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
    REGISTER /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
    REGISTER /me/pig/contrib/piggybank/java/piggybank.jar
    REGISTER /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
    REGISTER /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar

    DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
    rmf '/tmp/mail_pairs.avro'

    messages = LOAD '/tmp/10000_emails.avro' USING AvroStorage();
    smaller = FOREACH messages GENERATE from, to;
    pairs = FOREACH smaller GENERATE from, FLATTEN(to) AS to:chararray;

    STORE pairs INTO '/tmp/mail_pairs.avro' USING AvroStorage();

