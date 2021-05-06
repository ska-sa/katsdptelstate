History
=======

0.11 (2021-05-06)
-----------------
* Add asynchronous RDBWriter class (#108)
* Use a transaction for get_range instead of Lua: faster on server side (#110)
* Multiplex aio pubsubs over pool's standard connection (#113)
* Require hiredis for speed and aioredis<2 for compatibility (#114, #118)
* Improve unit tests, especially async memory backend (#107, #111, #112, #116)
* Update Docker image to use Redis 6.x instead of 4.x (#109)
* Requirements cleanup (#115, #117, #120)

0.10 (2020-05-25)
-----------------
* Remove Python 2 support. At least Python 3.5 is required.
* Remove support for old versions of redis-py (#100)
* Use redis-py health checks to improve robustness (#99)
* Add "indexed" keys (#98)
* Add an asyncio interface (#103)
* No longer throw InvalidKeyError when setting a key that shadows a method (#102)
* Add type annotations for mypy (#101)

0.9 (2020-05-25)
----------------
* Deprecate Python 2 support: this is the last release that will support Python 2 (#94)
* Remove ``get_message`` and ``send_message``, which were never used (#89)
* Publish the documentation on https://katsdptelstate.readthedocs.io (#90)
* Disable pickles by default for security (#92)

0.8 (2019-05-06)
----------------
* The default encoding is now msgpack; warn on loading pickles (#75, #79)
* The default backend is now in-memory (#76)
* Add the ability to dump in-memory backend to an RDB file (#77)
* Construct from RDB file-like objects and Redis URLs (#80, #82)
* Report keys and prefixes to the user as strings (#73)
* Add IPython tab completion (#83)
* RDB reader and writer cleanup (#85, #86)

0.7 (2019-02-12)
----------------
* Introduce encodings and add msgpack encoding as alternative to pickle (#64, #65)
* Introduce backends and add in-memory backend as alternative to redis (#71, #72)
* Simplify setting attributes via `__setitem__` (#68)
* Let keys be bytes internally, but allow specification as unicode strings (#63)
* The GitHub repository is now public as well

0.6 (2018-05-10)
----------------
* Initial release of katsdptelstate
