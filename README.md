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

Generating to/from pair sent counts from all emails:

    REGISTER /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
    REGISTER /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
    REGISTER /me/pig/contrib/piggybank/java/piggybank.jar
    REGISTER /me/pig/build/ivy/lib/Pig/jackson-core-asl-1.7.3.jar
    REGISTER /me/pig/build/ivy/lib/Pig/jackson-mapper-asl-1.7.3.jar

    DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
    sh rm -rf '/tmp/sent_counts.avro' /* Workaround for PIG-2441 */

    messages = LOAD '/tmp/10000_emails.avro' USING AvroStorage();
    messages = FILTER messages BY from IS NOT NULL AND to IS NOT NULL;
    smaller = FOREACH messages GENERATE from, to;
    pairs = FOREACH smaller GENERATE from, FLATTEN(to) AS to:chararray;
    pairs = FOREACH pairs GENERATE LOWER(from) AS from, LOWER(to) AS to;

    froms = GROUP pairs BY (from, to);
    sent_counts = FOREACH froms GENERATE FLATTEN(group) AS (from, to), SIZE(pairs) AS total;
    STORE sent_counts INTO '/tmp/sent_counts.avro' USING AvroStorage();

### Publishing with MongoDB

    MongoDB (from "humongous") is an open source, high-performance, schema-free, document-oriented database written in the C++ programming language
    
MongoDB is available [here](http://www.mongodb.org/downloads).  Once you install it and get it running, 
