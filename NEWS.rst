History
=======

0.13 (2023-03-15)
-----------------
* Upgrade to redis-py>=4.2 and fakeredis>=2, fix test warnings (#132)
* This now requires at least Python 3.7 and no more aioredis

0.12 (2023-03-13)
-----------------
* Switch to aioredis 2.x (#124)
* Add async (and synchronous) `RedisBackend.from_url` constructor (#125, #126)
* Switch from nose to pytest (#129)
* Remove `Endpoint.multicast_subscribe` and `netifaces` dependency (#130)
* Fix Lua script for `set_indexed` so that `wait_indexed` actually works (#127)
* Make aio `wait_key` more robust (#128)
* General cleanup (#122, #123)

0.11 (2021-05-07)
-----------------
* Add asynchronous RDBWriter class (#108)
* Use a transaction for get_range instead of Lua: faster on server side (#110)
* Multiplex aio pubsubs over pool's standard connection (#113)
* Require hiredis for speed and aioredis<2 for compatibility (#114, #118)
* Improve `wait_keys` responsiveness for `MemoryBackend` (#111, #116)
* Avoid blocking the Redis server on telstate.clear (#112)
* Update Docker image to use Redis 6.x instead of 4.x (#109)
* Support older HMSET Redis command in unit tests (#107)
* Requirements cleanup (#115, #117, #119, #120)

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
