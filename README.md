Collecting Data
===============

This is a HOWTO for collecting data in Ruby and Python applications.

Scraping your Gmail Inbox
-------------------------

### Define a schema for our emails

While it is cumbersome to define static schemas, we need only do so once with Thrift.  Our data is then accessible by any of the languages or tools we will use.

Email's format is defined in [RFC-5322](http://tools.ietf.org/html/rfc5322).  A corresponding thrift schema for email looks like this:

    namespace java com.datasyndrome.thrift
    namespace rb DataSyndrome
    namespace py datasyndrome

    struct EmailAddress {
      1: string address,
      2: string name,
    }

    struct Email {
      1: required EmailAddress from,
      2: list<EmailAddress> to,
      3: list<EmailAddress> cc,
      4: list<EmailAddress> bcc,
      5: string reply_to,
      6: string subject,
      7: string date,
      8: string message_id,
      9: string body,
    }


### Setup elephant-bird and Thrift

Twitter provides a library called [elephant-bird](https://github.com/kevinweil/elephant-bird) that includes several serialization format integration for Pig, including Thrift.  To install it, we need to first install (or downgrade to) [thrift 0.5](http://archive.apache.org/dist/incubator/thrift/0.5.0-incubating/).  There may be additional dependencies for these libraries on your platform.  Note that you can build elephant-bird without protobufs via:

    ant noproto release-jar

A good tutorial for using Thrift with Ruby is [here](http://saravani.wordpress.com/2011/05/03/thrift-ruby-tutorial/).

### Get an access token via xoauth.py

We first use [xoauth.py](http://google-mail-xoauth-tools.googlecode.com/svn/trunk/python/xoauth.py) to get access tokens to access our inbox via Xoauth.  Good instructions for this are [here](http://code.google.com/p/google-mail-xoauth-tools/wiki/XoauthDotPyRunThrough).

Once you follow these instructions, you'll have two values:

    oauth_token: 2/cG1NvgxA30c1xZOpPS-kWMxBqiZJ5QsH4cNlropLHt8
    oauth_token_secret: 9mk8v0qNs20Dw2zSoX6UheAn

### Scrape your Inbox

