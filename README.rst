MeerKAT Science Data Processor Telescope State
==============================================

This is a client package that allows connection to the Redis database that
stores telescope state information for the Science Data Processor of the
MeerKAT radio telescope. This database is colloquially known as *telstate*.

The Redis database acts as a key-value store. There are three types of keys:

immutables (aka *attributes*)
  Stores a single value that is not allowed to change once set.

mutables (aka *sensors*)
  Stores multiple timestamps values organised into an ordered set.

indexed
  Stores a dictionary of key-value pairs, each of which behaves like an
  immutable. This is useful to avoid the main key-space becoming too large,
  allows the entire dictionary to be fetched as a single operation, and allows
  more general keys than just strings.

The keys are strings and the values (and the sub-keys of indexed keys) are
Python objects serialised via MessagePack_, which has been extended to support
tuples, complex numbers and NumPy arrays. Older versions of the database stored
the values as pickles, and the package warns the user if that's the case. Keys
can be retrieved from the telstate object using attribute syntax or dict
syntax.

.. _MessagePack: http://www.msgpack.org/

Databases can be accessed via one of two backends: a Redis client backend
that allows shared access to an actual Redis server over the network (or a
simulated server via fakeredis) and a simplified in-memory backend for
stand-alone access. Both backends support loading and saving a Redis snapshot
in the form of an RDB dump file.

It is possible to have multiple *views* on the same database (one per telstate
instance). A view is defined as a list of *prefixes* acting as namespaces that
group keys. When reading from the database, each prefix is prepended to the key
in turn until a match is found. When writing to the database, the first prefix
is prepended to the key. The first prefix therefore serves as the primary
namespace while the rest are supplementary read-only namespaces.

.. warning::

  **WARNING**: The standard warning about Python pickles applies. Never
  retrieve data from an untrusted telstate database with values encoded as
  pickles, or connect to such a database over an untrusted network. Pickle
  support is disabled by default, but can be enabled for trusted databases
  by setting the environment variable KATSDPTELSTATE_ALLOW_PICKLE=1.

Getting Started
---------------

The simplest way to test out `katsdptelstate` is to use the in-memory backend.
If you want to run a real Redis server you will need to install Redis (version
4.0 or newer) on a suitable machine on the network. For example, do this:

- macOS: ``brew install redis``
- Ubuntu: ``apt-get install redis-server``

Then ``pip install katsdptelstate`` and run a local ``redis-server``. If you
also want to load RDB files, do ``pip install katsdptelstate[rdb]``.

A Simple Example
----------------

.. code:: python

  import time
  import katsdptelstate

  # Connect to an actual Redis server via an endpoint or an URL
  telstate = katsdptelstate.TelescopeState('localhost:6379')
  telstate = katsdptelstate.TelescopeState('redis://localhost')
  # Or use the in-memory backend (useful for testing)
  telstate = katsdptelstate.TelescopeState()
  # Load RDB file into Redis if katsdptelstate is installed with [rdb] option
  telstate.load_from_file('dump.rdb')

  # Attribute / dict style access returns the latest value
  telstate.add('n_chans', 32768)
  print(telstate.n_chans)  # -> 32768
  print(telstate['n_chans'])  # -> 32768

  # List all keys (attributes and sensors)
  print(telstate.keys())  # -> ['n_chans']

  # Sensors are timestamped underneath
  st = time.time()
  telstate.add('n_chans', 4096)
  et = time.time()
  telstate.add('n_chans', 16384)
  # Time ranges can be used and are really fast
  telstate.get_range('n_chans', st=st, et=et)  # -> [(4096, 1556112474.453495)]
  # Add an item 10 seconds back
  telstate.add('n_chans', 1024, ts=time.time() - 10)

  # Attributes cannot be changed (only deleted)
  telstate.add('no_change', 1234, immutable=True)
  # Adding it again is OK as long as the value doesn't change
  telstate.add('no_change', 1234, immutable=True)
  # Simpler notation for setting attributes
  telstate['no_change'] = 1234
  # Will raise katsdptelstate.ImmutableKeyError
  telstate['no_change'] = 456

  # Create a new view with namespace 'ns' and standard underscore separator
  view = telstate.view('ns')
  # Insert a new attribute in this namespace and retrieve it
  view['x'] = 1
  print(view['x'])  # -> 1
  print(view.prefixes)  # -> ('ns_', '')
  print(view.keys())  # -> ['n_chans', 'no_change', 'ns_x']
