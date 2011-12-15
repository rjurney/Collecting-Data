Collecting Data
===============

This is a HOWTO for collecting data in Ruby and Python applications.

Scraping your Gmail Inbox
-------------------------

### Get an access token via xoauth.py

We first use xoauth.py to get access tokens to access our inbox via Xoauth.  Good instructions for this are [here](http://code.google.com/p/google-mail-xoauth-tools/wiki/XoauthDotPyRunThrough).

Download xoauth.py:
    wget http://google-mail-xoauth-tools.googlecode.com/svn/trunk/python/xoauth.py

Run xoauth.py for your gmail address:
    python xoauth.py --generate_oauth_token --user=my.name@gmail.com

### 

