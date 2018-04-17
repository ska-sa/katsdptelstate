MeerKAT Science Data Processor Telescope State
==============================================

This is a client package that allows connection to the Redis database that
stores telescope state information for the Science Data Processor of the
MeerKAT radio telescope.

The Redis database acts as a key-value store. Each key is either a *sensor* or
an *attribute*. A sensor has multiple timestamped values organised into an
ordered set. An attribute (or *immutable* key) has a single value without a
timestamp that is not allowed to change.

The keys are strings and values are arbitrary Python objects stored as pickles.
Keys can be accessed using attribute syntax or dict syntax.

.. warning::

  The standard warning about Python pickles apply. The pickle module is not
  secure against erroneous or maliciously constructed data. Never unpickle data
  received from an untrusted or unauthenticated source.

Getting Started
---------------

You will need a relatively recent version of Redis installed (2.8.9 or newer).

macOS: ``brew install redis``
Ubuntu: ``apt-get install redis-server``

Then ``pip install katsdptelstate`` and run a local ``redis-server``.

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

  # Attribute / dict style access returns the latest value
  telstate.add('n_chans', 32768)
  print(telstate.n_chans)
  print(telstate['n_chans'])

  # List all keys (attributes and sensors)
  telstate.keys()

  # Sensors are timestamped underneath
  st = time.time()
  telstate.add('n_chans', 4096)
  et = time.time()
  telstate.add('n_chans', 16384)
  # Time ranges can be used and are really fast
  telstate.get_range('n_chans', st=st, et=et)
  # Add an item 10 seconds back
  telstate.add('n_chans', 1024, ts=time.time() - 10)

  # Attributes cannot be changed (only deleted)
  telstate.add('no_change', 1234, immutable=True)
  # Adding it again is OK as long as the value doesn't change
  telstate.add('no_change', 1234, immutable=True)
  # Will raise katsdptelstate.ImmutableKeyError
  telstate.add('no_change', 456)
