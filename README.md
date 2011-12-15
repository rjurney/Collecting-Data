Collecting Data
===============

This is a HOWTO for collecting data in Ruby and Python applications and sending it to S3 via Kafka.

Installation & Setup
--------------------

### Kafka Server Setup

Note: You will have to enter the root password for sudo to install some components.

Setup may take some time, as components are installed in lib/.

    ./setup.sh

### Ruby Client Setup

    sudo gem install bundler
    cd src/ruby
    bundle install
    cd ../..

### Python Client Setup

Python isn't working yet, owing to a problem building python-snappy for Avro support :(

Sending Avro Messages to Kafka
------------------------------

### Ruby

    cd src/ruby
    cd ../..

### Python

Consuming Kafka Messages to S3
------------------------------

