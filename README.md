simonr-telescope-model
======================

First pass at telescope model. This will need to be integrated in kattelmod at some stage, hence it is not
a fully fledged package as yet.

Getting Started
---------------

You will need a recent version of redis installed (2.8.9 or newer).

OSX: `brew install redis`
Ubuntu: source download and install is best

Then `pip install redis`

Then run a local `redis-server`

Detailed usage example is shown in the notebook.

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

st = time.time()
tm.add('n_chans',4096)
et = time.time()
tm.add('n_chans',16384)
tm.get('n_chans')
 # everything is actually timestamped underneath

tm.get('n_chans',st=st,et=et)
 # time ranges can be used and are really fast

tm.add('n_chans',1024,ts=time.time()-10)
 # add an item 10 seconds back
```
