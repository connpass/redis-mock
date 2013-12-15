redis-mock
==========

An in-memory mock redis client for Python. This mock should be a drop-in
replacement for the [redis.py](https://github.com/andymccurdy/redis-py) client.

Why?
---------

Sometimes you might like a mock that behaves somewhat like the redis server for
development or testing purposes. Though for testing purposes you probably want to
just use [mock](http://www.voidspace.org.uk/python/mock/).

Development Status
---------------------

Not all commands are currently supported and some may not support all options. Use at your own risk.
