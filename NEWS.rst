History
=======

0.8 (2019-05-03)
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
