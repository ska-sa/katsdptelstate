SDP Telescope State
===================

This is a client package that allows connection to the redis database that
stores telescope state information for the Science Data Processor.

It operates as a key/value store that timestamps incoming values and inserts
them into an ordered set (non-exclusive).

Keys are strings and values are arbitrary Python objects stored as pickles.

Keys can be declared as immutable, which turns them into attributes without
timestamps and with values that cannot be changed.

Keys can be accessed using attribute syntax or dict syntax.

Getting Started
---------------

You will need a recent version of redis installed (2.8.9 or newer).

macOS: ``brew install redis``
Ubuntu: source download and install is best

Then ``pip install redis``

Then run a local ``redis-server``

A Simple Example
----------------

.. code:: python

  import time
  import katsdptelstate

  # Connect to an actual redis server
  telstate = katsdptelstate.TelescopeState('localhost:6379')
  # Or use a fake redis instance (key/values stored in memory, useful for testing)
  telstate = katsdptelstate.TelescopeState()
  # Load dump file into redis if katsdptelstate is installed with [rdb] option
  telstate.load_from_file('dump.rdb')

  # Attribute / dict access returns the latest value
  telstate.add('n_chans', 32768)
  print(telstate.n_chans)
  print(telstate['n_chans'])

  # List all keys (attributes and sensors)
  telstate.keys()

  # Everything is actually timestamped underneath
  st = time.time()
  telstate.add('n_chans', 4096)
  et = time.time()
  telstate.add('n_chans', 16384)
  # Time ranges can be used and are really fast
  telstate.get_range('n_chans', st=st, et=et)
  # Add an item 10 seconds back
  telstate.add('n_chans', 1024, ts=time.time() - 10)

  # This attribute cannot be changed, only deleted
  telstate.add('no_change', 1234, immutable=True)
  # Adding it again is OK as long as the value doesn't change
  telstate.add('no_change', 1234, immutable=True)
  # Will raise katsdptelstate.ImmutableKeyError
  telstate.add('no_change', 456)
