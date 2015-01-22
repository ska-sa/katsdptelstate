SDP Telescope State
===================

A client package to allow connection to the redis db that stores telescope state information.

Operates as a key/value store which timestamps incoming values and inserts them into an ordered set (non-exclusive).

Immutables can be declared which function as strict one time set attributes.

Keys can be access via attribute or dict syntax.

Getting Started
---------------

You will need a recent version of redis installed (2.8.9 or newer).

OSX: `brew install redis`
Ubuntu: source download and install is best

Then `pip install redis`

Then run a local `redis-server`

Simple Example
--------------

```python
import time
import telescope_model
tm = telescope_model.TelescopeModel()
 # connect to a local redis instance

tm.add('n_chans',32768)
tm.list_keys()
print tm.n_chans
 # keys work as attributes and return latest values
print tm['n_chans']

st = time.time()
tm.add('n_chans',4096)
et = time.time()
tm.add('n_chans',16384)
tm.get('n_chans')
 # everything is actually timestamped underneath

tm.get_range('n_chans',st=st,et=et)
 # time ranges can be used and are really fast

tm.add('n_chans',1024,ts=time.time()-10)
 # add an item 10 seconds back

tm.add('no_change',1234,immutable=True)
 # this cannot be changed, only deleted

tm.add('no_change',456)
 # will raise katsdptelstate.ImmutableKeyError

```
