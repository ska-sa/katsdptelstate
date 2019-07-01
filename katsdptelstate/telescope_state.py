################################################################################
# Copyright (c) 2015-2019, National Research Foundation (Square Kilometre Array)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from __future__ import print_function, division, absolute_import
import six
from six.moves import cPickle as pickle

import struct
import time
import math
import logging
import contextlib
import functools
import io
import os
import warnings
import threading

import redis
import msgpack
import numpy as np

from .endpoint import Endpoint, endpoint_parser


logger = logging.getLogger(__name__)
PICKLE_PROTOCOL = 2

# Header byte indicating a pickle-encoded value. This is present in pickle
# v2+ (it is the PROTO opcode, which is required to be first). Values below
# 0x80 are assumed to be pickle as well, since v0 uses only ASCII, and v1
# was never used in telstate. Values above 0x80 are reserved for future
# encoding forms.
ENCODING_PICKLE = b'\x80'

#: Header byte indicating a msgpack-encoded value
ENCODING_MSGPACK = b'\xff'

#: Default encoding for :func:`encode_value`
ENCODING_DEFAULT = ENCODING_MSGPACK

#: All encodings that can be used with :func:`encode_value`
ALLOWED_ENCODINGS = frozenset([ENCODING_PICKLE, ENCODING_MSGPACK])

MSGPACK_EXT_TUPLE = 1
MSGPACK_EXT_COMPLEX128 = 2
MSGPACK_EXT_NDARRAY = 3           # .npy format
MSGPACK_EXT_NUMPY_SCALAR = 4      # dtype descriptor then raw value

_INF = float('inf')

_allow_pickle = False
_warn_on_pickle = False
_pickle_lock = threading.Lock()       # Protects _allow_pickle, _warn_on_pickle


PICKLE_WARNING = ('The telescope state contains pickled values. This is a security risk, '
                  'but you have enabled it with set_allow_pickle.')
PICKLE_ERROR = ('The telescope state contains pickled values. This is a security risk, '
                'so is disabled by default. If you trust the source of the data, you '
                'can allow the pickles to be loaded by setting '
                'KATSDPTELSTATE_ALLOW_PICKLE=1 in the environment. This is needed for '
                'MeerKAT data up to March 2019.')


def set_allow_pickle(allow, warn):
    """Control whether pickles are allowed.

    This overrides the defaults which are determined from the environment.

    Parameters
    ----------
    allow : bool
        If true, allow pickles to be loaded.
    warn : bool
        If true, warn the user the next time a pickle is loaded (after which
        it becomes false). This has no effect if `allow` is false.
    """
    global _allow_pickle, _warn_on_pickle

    with _pickle_lock:
        _allow_pickle = allow
        _warn_on_pickle = warn


def _init_allow_pickle():
    env = os.environ.get('KATSDPTELSTATE_ALLOW_PICKLE')
    allow = False
    if env == '1':
        allow = True
    elif env == '0':
        allow = False
    elif env is not None:
        warnings.warn('Unknown value {!r} for KATSDPTELSTATE_ALLOW_PICKLE'.format(env))
    set_allow_pickle(allow, False)


_init_allow_pickle()


class TelstateError(RuntimeError):
    """Base class for errors from this module"""


class ConnectionError(TelstateError):
    """The initial connection to the Redis server failed."""


class RdbParseError(TelstateError):
    """Error parsing RDB file."""
    def __init__(self, filename=None):
        self.filename = filename

    def __str__(self):
        name = repr(self.filename) if self.filename else 'object'
        return 'Invalid RDB file {}'.format(name)


class InvalidKeyError(TelstateError):
    """A key collides with a class attribute"""


class InvalidTimestampError(TelstateError):
    """Negative or non-finite timestamp"""


class ImmutableKeyError(TelstateError):
    """An attempt was made to modify an immutable key"""


class TimeoutError(TelstateError):
    """A wait for a key timed out"""


class CancelledError(TelstateError):
    """A wait for a key was cancelled"""


class DecodeError(ValueError, TelstateError):
    """An encoded value found in telstate could not be decoded"""


class EncodeError(ValueError, TelstateError):
    """A value could not be encoded"""


if six.PY2:
    _pickle_loads = pickle.loads
    _ensure_str = six.ensure_str
    _ensure_binary = six.ensure_binary
else:
    # See https://stackoverflow.com/questions/11305790
    _pickle_loads = functools.partial(pickle.loads, encoding='latin1')
    # Behave gracefully in case someone uses non-UTF-8 binary in a key on PY3
    _ensure_str = functools.partial(six.ensure_str, errors='surrogateescape')
    _ensure_binary = functools.partial(six.ensure_binary, errors='surrogateescape')


def _display_str(s):
    """Return most human-readable and yet accurate version of *s*."""
    try:
        return '{!r}'.format(six.ensure_str(s))
    except UnicodeDecodeError:
        return '{!r}'.format(s)


def _encode_ndarray(value):
    fp = io.BytesIO()
    try:
        np.save(fp, value, allow_pickle=False)
    except ValueError as error:
        # Occurs if value has object type
        raise EncodeError(str(error))
    return fp.getvalue()


def _decode_ndarray(data):
    fp = io.BytesIO(data)
    try:
        return np.load(fp, allow_pickle=False)
    except ValueError as error:
        raise DecodeError(str(error))


def _encode_numpy_scalar(value):
    if value.dtype.hasobject:
        raise EncodeError('cannot encode dtype {} as it contains objects'
                          .format(value.dtype))
    descr = np.lib.format.dtype_to_descr(value.dtype)
    return _msgpack_encode(descr) + value.tobytes()


def _decode_numpy_scalar(data):
    try:
        descr = _msgpack_decode(data)
        raw = b''
    except msgpack.ExtraData as exc:
        descr = exc.unpacked
        raw = exc.extra
    dtype = np.dtype(descr)
    if dtype.hasobject:
        raise DecodeError('cannot decode dtype {} as it contains objects'
                          .format(dtype))
    value = np.frombuffer(raw, dtype, 1)
    return value[0]


def _msgpack_default(value):
    if isinstance(value, tuple):
        return msgpack.ExtType(MSGPACK_EXT_TUPLE, _msgpack_encode(list(value)))
    elif isinstance(value, np.ndarray):
        return msgpack.ExtType(MSGPACK_EXT_NDARRAY, _encode_ndarray(value))
    elif isinstance(value, np.generic):
        return msgpack.ExtType(MSGPACK_EXT_NUMPY_SCALAR, _encode_numpy_scalar(value))
    elif isinstance(value, complex):
        return msgpack.ExtType(MSGPACK_EXT_COMPLEX128,
                               struct.pack('>dd', value.real, value.imag))
    else:
        raise EncodeError('do not know how to encode type {}'
                          .format(value.__class__.__name__))


def _msgpack_ext_hook(code, data):
    if code == MSGPACK_EXT_TUPLE:
        content = _msgpack_decode(data)
        if not isinstance(content, list):
            raise DecodeError('incorrectly encoded tuple')
        return tuple(content)
    elif code == MSGPACK_EXT_COMPLEX128:
        if len(data) != 16:
            raise DecodeError('wrong length for COMPLEX128')
        return complex(*struct.unpack('>dd', data))
    elif code == MSGPACK_EXT_NDARRAY:
        return _decode_ndarray(data)
    elif code == MSGPACK_EXT_NUMPY_SCALAR:
        return _decode_numpy_scalar(data)
    else:
        raise DecodeError('unknown extension type {}'.format(code))


def _msgpack_encode(value):
    return msgpack.packb(value, use_bin_type=True, strict_types=True,
                         default=_msgpack_default)


def _msgpack_decode(value):
    # The max_*_len prevent a corrupted or malicious input from consuming
    # memory significantly in excess of the input size before it determines
    # that there isn't actually enough data to back it.
    max_len = len(value)
    return msgpack.unpackb(value, raw=False, ext_hook=_msgpack_ext_hook,
                           max_str_len=max_len,
                           max_bin_len=max_len,
                           max_array_len=max_len,
                           max_map_len=max_len,
                           max_ext_len=max_len)


def encode_value(value, encoding=ENCODING_DEFAULT):
    """Encode a value to a byte array for storage in redis.

    Parameters
    ----------
    value
        Value to encode
    encoding
        Encoding method to use, one of the values in :const:`ALLOWED_ENCODINGS`

    Raises
    ------
    ValueError
        If `encoding` is not a recognised encoding
    EncodeError
        EncodeError if the value was not encodable with the chosen encoding.
    """
    if encoding == ENCODING_PICKLE:
        return pickle.dumps(value, protocol=PICKLE_PROTOCOL)
    elif encoding == ENCODING_MSGPACK:
        return ENCODING_MSGPACK + _msgpack_encode(value)
    else:
        raise ValueError('Unknown encoding {:#x}'.format(ord(encoding)))


def decode_value(value, allow_pickle=None):
    """Decode a value encoded with :func:`encode_value`.

    The encoded value is self-describing, so it is not necessary to specify
    which encoding was used.

    Parameters
    ----------
    value : bytes
        Encoded value to decode
    allow_pickle : bool, optional
        If false, :const:`ENCODING_PICKLE` is disabled. This is useful for
        security as pickle decoding can execute arbitrary code. If the default
        of ``None`` is used, it is controlled by the
        KATSDPTELSTATE_ALLOW_PICKLE environment variable. If that is not set,
        the default is false. The default may also be overridden with
        :func:`set_allow_pickle`.

    Raises
    ------
    DecodeError
        if `allow_pickle` is false and `value` is a pickle
    DecodeError
        if there was an error in the encoding of `value`
    """
    global _warn_on_pickle

    if not value:
        raise DecodeError('empty value')
    elif value[:1] == ENCODING_MSGPACK:
        try:
            return _msgpack_decode(value[1:])
        except Exception as error:
            raise DecodeError(str(error))
    elif value[:1] <= ENCODING_PICKLE:
        if allow_pickle is None:
            with _pickle_lock:
                allow_pickle = _allow_pickle
                if _warn_on_pickle:
                    warnings.warn(PICKLE_WARNING, FutureWarning)
                    _warn_on_pickle = False
        if allow_pickle:
            try:
                return _pickle_loads(value)
            except Exception as error:
                raise DecodeError(str(error))
        else:
            raise DecodeError(PICKLE_ERROR)
    else:
        raise DecodeError('value starts with unrecognised header byte {!r} '
                          '(katsdptelstate may need to be updated)'.format(value[:1]))


def equal_encoded_values(a, b):
    """Test whether two encoded values represent the same/equivalent objects.

    This is not a complete implementation. Mostly, it just checks that the
    encoded representation is the same, but to ease the transition to Python 3,
    it will also compare the values if both arguments decode to either
    ``bytes`` or ``str`` (and allows bytes and strings to be equal assuming
    UTF-8 encoding). However, it will not do this recursively in structured
    data.
    """
    if a == b:
        return True
    a = decode_value(a)
    b = decode_value(b)
    try:
        return _ensure_binary(a) == _ensure_binary(b)
    except TypeError:
        return False


class Backend(object):
    """Low-level interface for telescope state backends.

    The backend interface does not deal with namespaces or encodings, which are
    handled by the frontend :class:`TelescopeState` class. A backend must be
    able to store

    - immutables: key-value pairs (both :class:`bytes`);

    - mutables: a key associated with a set of (timestamp, value) pairs, where
      the timestamps are non-negative finite floats and the values are
      :class:`bytes`.
    """
    def load_from_file(self, file):
        """Implements :meth:`TelescopeState.load_from_file`."""
        raise NotImplementedError

    def __contains__(self, key):
        """Return if `key` is in the backend."""
        raise NotImplementedError

    def keys(self, filter):
        """Return all keys matching `filter`.

        The filter is a redis pattern. Backends might only support ``b'*'`` as
        a filter.
        """
        raise NotImplementedError

    def delete(self, key):
        """Delete a key (no-op if it does not exist)"""
        raise NotImplementedError

    def clear(self):
        """Remove all keys"""
        raise NotImplementedError

    def is_immutable(self, key):
        """Whether `key` is an immutable key in the backend.

        Raises
        ------
        KeyError
            If `key` is not present at all
        """
        raise NotImplementedError

    def set_immutable(self, key, value):
        """Set the value of an immutable key.

        If the key already exists (and is immutable), returns the existing
        value and does not update it. Otherwise, returns ``None``.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is mutable
        """
        raise NotImplementedError

    def get_immutable(self, key):
        """Get the value of an immutable key.

        Returns ``None`` if the key does not exist.

        Raises
        ------
        ImmutableKeyError
            If the key is mutable
        """
        raise NotImplementedError

    def add_mutable(self, key, value, timestamp):
        """Set a (timestamp, value) pair in a mutable key.

        The `timestamp` will be a non-negative float value.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is immutable
        """
        raise NotImplementedError

    def get_range(self, key, start_time, end_time, include_previous, include_end):
        """Obtain a range of values from a mutable key.

        If the key does not exist, returns None.

        Parameters
        ----------
        key : bytes
            Key to search
        start_time : float
            Start of the range (inclusive).
        end_time : float
            End of the range. It is guaranteed to be non-negative.
        include_previous : bool
            If true, also return the last entry prior to `start_time`.
        include_end : bool
            If true, treat `end_time` as inclusive, otherwise exclusive.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is immutable
        """
        raise NotImplementedError

    def dump(self, key):
        """Return a key in the same format as the Redis DUMP command, or None if not present"""
        raise NotImplementedError

    def monitor_keys(self, keys):
        """Report changes to keys in `keys`.

        Returns a generator. The first yield from the generator is a no-op.
        After that, the caller sends a timeout and gets back an update event.
        Each update event is a tuple, of the form (key, value) or (key, value,
        timestamp) depending on whether the update is to an immutable or a
        mutable key; or of the form (key,) to indicate that a key may have
        been updated but there is insufficient information to provide the
        latest value. If there is no event within the timeout, returns
        ``None``.

        It is acceptable (but undesirable) for this function to miss the
        occasional update e.g. due to a network connection outage. The caller
        takes care not use a low timeout and retry rather than blocking for
        long periods.

        The generator runs until it is closed.
        """
        # This is a valid but usually suboptimal implementation
        timeout = yield None
        while True:
            time.sleep(timeout)
            timeout = yield (keys[0],)

    @staticmethod
    def pack_query_timestamp(time, is_end, include_end=False):
        """Create a query value for a ZRANGEBYLEX query.

        When packing the time for the start of a range, set `is_end` and
        `include_end` to False. When packing the time for the end of a range,
        set `is_end` to True, and `include_end` indicates whether the endpoint
        is inclusive. The latter is implemented by incrementing the time by the
        smallest possible amount and then treating it as exclusive.
        """
        if time == _INF:
            # The special positively infinite string represents the end of time
            return b'+'
        elif time < 0.0 or (time == 0.0 and not include_end):
            # The special negatively infinite string represents the dawn of time
            return b'-'
        else:
            packed_time = Backend.pack_timestamp(time)
            if include_end:
                # Increment to the next possible encoded value. Note that this
                # cannot overflow because the sign bit is initially clear.
                packed_time = struct.pack('>Q', struct.unpack('>Q', packed_time)[0] + 1)
            return (b'(' if is_end else b'[') + packed_time

    @staticmethod
    def pack_timestamp(timestamp):
        """Encode a timestamp to a bytes that sorts correctly"""
        assert timestamp >= 0
        # abs forces -0 to +0, which encodes differently
        return struct.pack('>d', abs(timestamp))

    @staticmethod
    def split_timestamp(packed):
        """Split out the value and timestamp from a packed item.

        The item contains 8 bytes with the timestamp in big-endian IEEE-754
        double precision, followed by the value.
        """
        assert len(packed) >= 8
        timestamp = struct.unpack('>d', packed[:8])[0]
        return (packed[8:], timestamp)


class TelescopeState(object):
    """Interface to attributes and sensors stored in a Redis database.

    There are two types of keys permitted: single immutable values, and mutable
    keys where the full history of values is stored with timestamps. These are
    mapped to the Redis string and zset types. A Redis database used with this
    class must *only* be used with this class, as it does not deal with other
    types of values.

    Each instance of this class has an associated list of prefixes. Lookups
    try each key in turn until a match is found. Writes use the first prefix in
    the list. Conventionally, keys are arranged into a hierarchy, separated by
    underscores. A :meth:`view` convenience method helps with constructing
    prefix lists by automatically adding the trailing underscore to prefixes.

    Care should be used when attributes share a suffix. They may shadow
    shadow each other for some views, causing the attribute to appear to have
    changed value. This class does not prevent it, because there is no way to
    know which namespaces may be shared in a view, and because doing it in a
    race-free way would be prohibitively expensive.

    Parameters
    ----------
    endpoint : str, :class:`~katsdptelstate.endpoint.Endpoint` or :class:`Backend`
        It can be

        - an endpoint: specifies the address of the Redis server

        - an URL (i.e., contains ``://``): passed to :meth:`redis.StrictRedis.from_url`

        - an empty string: a :class:`~katsdptelstate.memory.MemoryBackend` is created

        - any other string: passed to :class:`~katsdptelstate.endpoint.Endpoint` to
          create an endpoint

        - a :class:`Backend`: used directly.
    db : int
        Database number within the Redis server.
    prefixes : tuple of str/bytes
        Prefixes that will be tried in turn for key lookup. While this can be
        specified directly for advanced cases, it is normally generated by
        :meth:`view`. Writes are made using the first prefix in the list.
    base : :class:`~katsdptelstate.telescope_state.TelescopeState`
        Existing telescope state instance, from which the backend will be
        taken. This allows new views to be created by specifying `prefixes`,
        without creating new backends.

    Raises
    ------
    ConnectionError
        If the initial connection to the (real) Redis server fails
    ValueError
        If a `base` is specified and either `endpoint` or `db` is non-default
    ValueError
        If `endpoint` is a :class:`Backend` and `db` is non-default
    """
    SEPARATOR = '_'
    SEPARATOR_BYTES = b'_'

    def __init__(self, endpoint='', db=0, prefixes=(b'',), base=None):
        if base is not None:
            if endpoint != '':
                raise ValueError('Cannot specify both base and endpoint')
            if db != 0:
                raise ValueError('Cannot specify both base and db')
            self._backend = base._backend
        elif isinstance(endpoint, Backend):
            if db != 0:
                raise ValueError('Cannot specify both a backend and a db')
            self._backend = endpoint
        elif not endpoint:
            from .memory import MemoryBackend
            if db != 0:
                raise ValueError('Cannot specify a db when using the default backend')
            self._backend = MemoryBackend()
        else:
            from .redis import RedisBackend
            if isinstance(endpoint, str) and '://' in endpoint:
                r = redis.StrictRedis.from_url(endpoint, db=db, socket_timeout=5)
            else:
                if not isinstance(endpoint, Endpoint):
                    endpoint = endpoint_parser(default_port=None)(endpoint)
                redis_kwargs = dict(host=endpoint.host, db=db, socket_timeout=5)
                # If no port is provided, redis will pick its default port
                if endpoint.port is not None:
                    redis_kwargs['port'] = endpoint.port
                r = redis.StrictRedis(**redis_kwargs)
            self._backend = RedisBackend(r)
        # Ensure all prefixes are bytes internally for consistency
        self._prefixes = tuple(_ensure_binary(prefix) for prefix in prefixes)

    @property
    def prefixes(self):
        """The active key prefixes as a tuple of strings."""
        return tuple(_ensure_str(prefix) for prefix in self._prefixes)

    @property
    def backend(self):
        return self._backend

    def load_from_file(self, file):
        """Load keys from a Redis-compatible RDB snapshot file.

        Redis keys are extracted sequentially from the RDB file and inserted
        directly into the backend without any checks and ignoring the view.
        It is therefore a bad idea to insert keys that already exist in telstate
        and this will lead to undefined behaviour. The standard approach is
        to call this method on an empty telstate.

        If there is an error reading or parsing the RDB file (indicating either
        a broken file or a non-RDB file), an `RdbParseError` is raised. Errors
        raised while opening the file (like `OSError`) and errors raised by the
        backend itself (like redis errors) can also occur.

        Parameters
        ----------
        file : str or file object
            Filename or file object representing RDB file

        Returns
        -------
        keys_loaded : int
            Number of keys loaded from RDB file into telstate

        Raises
        ------
        ImportError
            If the rdbtools package is not installed
        RdbParseError
            If the file could not be parsed (truncated / malformed / not RDB)
        """
        keys_loaded = self._backend.load_from_file(file)
        logger.info("Loading {} keys from {}".format(keys_loaded, file))
        return keys_loaded

    @classmethod
    def join(cls, *names):
        """Join string components of key with supported separator."""
        return cls.SEPARATOR.join(names)

    def view(self, name, add_separator=True, exclusive=False):
        """Create a view with an extra name in the list of namespaces.

        Returns a new view with `name` added as the first prefix, or the
        only prefix if `exclusive` is true. If `name` is non-empty and does not
        end with the separator, it is added (unless `add_separator` is
        false).
        """
        name = _ensure_binary(name)
        if name != b'' and name[-1:] != self.SEPARATOR_BYTES and add_separator:
            name += self.SEPARATOR_BYTES
        if exclusive:
            prefixes = (name,)
        else:
            prefixes = (name,) + self._prefixes
        return self.__class__(prefixes=prefixes, base=self)

    def root(self):
        """Create a view containing only the root namespace."""
        return self.__class__(base=self)

    def __getattr__(self, key):
        try:
            return self._get(key)
        except KeyError as error:
            raise AttributeError(str(error))

    def __getitem__(self, key):
        return self._get(key)

    def __setattr__(self, key, value):
        if key.startswith('_'):
            super(TelescopeState, self).__setattr__(key, value)
        elif key in self.__class__.__dict__:
            raise AttributeError("The specified key already exists as a "
                                 "class method and thus cannot be used.")
        else:
            self.add(key, value, immutable=True)

    def __setitem__(self, key, value):
        self.add(key, value, immutable=True)

    def __contains__(self, key):
        """Check to see if the specified key exists in the database."""
        key = _ensure_binary(key)
        for prefix in self._prefixes:
            if prefix + key in self._backend:
                return True
        return False

    def is_immutable(self, key):
        """Check to see if the specified key is an immutable."""
        key = _ensure_binary(key)
        for prefix in self._prefixes:
            try:
                return self._backend.is_immutable(prefix + key)
            except KeyError:
                pass
        return False

    def keys(self, filter='*'):
        """Return a list of keys currently in the model.

        This function ignores the prefix list and returns all keys with
        fully-qualified names.

        Parameters
        ----------
        filter : str or bytes, optional
            Wildcard string passed to Redis to restrict keys

        Returns
        -------
        keys : list of str
            The key names, in sorted order
        """
        keys = self._backend.keys(_ensure_binary(filter))
        return sorted(_ensure_str(key) for key in keys)

    def _ipython_key_completions_(self):
        """List of keys used in IPython (version >= 5) tab completion.

        This respects the prefix list and presents keys with prefixes removed.
        """
        keys = []
        keys_b = self._backend.keys(b'*')
        for prefix_b in self._prefixes:
            keys.extend(_ensure_str(k[len(prefix_b):])
                        for k in keys_b if k.startswith(prefix_b))
        return keys

    def delete(self, key):
        """Remove a key, and all values, from the model.

        The key is deleted from every namespace in the prefix list.

        .. note::

            This function should be used rarely, ideally only in tests, as it
            violates the immutability of keys added with ``immutable=True``.
        """
        key = _ensure_binary(key)
        for prefix in self._prefixes:
            self._backend.delete(prefix + key)

    def clear(self):
        """Remove all keys in all namespaces.

        .. note::

            This function should be used rarely, ideally only in tests, as it
            violates the immutability of keys added with ``immutable=True``.
        """
        return self._backend.clear()

    def add(self, key, value, ts=None, immutable=False, encoding=ENCODING_DEFAULT):
        """Add a new key / value pair to the model.

        If `immutable` is true, then either the key must not previously have
        been set, or it must have been previously set immutable with exactly the
        same value (see :meth:`equal_encoded_values`). Thus, immutable keys only
        ever have one value for the lifetime of the telescope state. They also
        have no associated timestamp.

        Parameters
        ----------
        key : str or bytes
            Key name, which must not collide with a class attribute
        value : object
            Arbitrary value (must be encodable with `encoding`)
        ts : float, optional
            Timestamp associated with the update, ignored for immutables. If not
            specified, defaults to ``time.time()``.
        immutable : bool, optional
            See description above.
        encoding : bytes
            See :func:`encode_value`

        Raises
        ------
        InvalidKeyError
            if `key` collides with a class member name
        ImmutableKeyError
            if an attempt is made to change the value of an immutable or to
            change a mutable key to immutable or vice versa
        redis.ResponseError
            if there is some other error from the Redis server
        """
        if key in self.__class__.__dict__:
            raise InvalidKeyError("The specified key already exists as a "
                                  "class method and thus cannot be used.")
         # check that we are not going to munge a class method
        key = _ensure_binary(key)
        full_key = self._prefixes[0] + key
        key_str = _display_str(full_key)
        str_val = encode_value(value, encoding)
        if immutable:
            try:
                old = self._backend.set_immutable(full_key, str_val)
            except ImmutableKeyError:
                raise ImmutableKeyError('Attempt to overwrite mutable key {} '
                                        'with immutable'.format(key_str))
            if old is not None:
                # The key already exists. Check if the value is the same.
                try:
                    if not equal_encoded_values(str_val, old):
                        raise ImmutableKeyError(
                            'Attempt to change value of immutable key {} from '
                            '{!r} to {!r}'.format(key_str, decode_value(old), value))
                    else:
                        logger.info('Attribute {} updated with the same value'
                                    .format(key_str))
                        return True
                except DecodeError as error:
                    raise ImmutableKeyError(
                        'Attempt to set value of immutable key {} to {!r} but '
                        'failed to decode the previous value to compare: {}'
                        .format(key_str, value, error))
        else:
            ts = float(ts) if ts is not None else time.time()
            if math.isnan(ts) or math.isinf(ts):
                raise InvalidTimestampError('Non-finite timestamps ({}) are not supported'
                                            .format(ts))
            if ts < 0.0:
                raise InvalidTimestampError('Negative timestamps ({}) are not supported'
                                            .format(ts))
            try:
                self._backend.add_mutable(full_key, str_val, ts)
            except ImmutableKeyError:
                raise ImmutableKeyError('Attempt to overwrite immutable key {}'
                                        .format(key_str))

    def _check_condition(self, key, condition, message=None):
        """Check whether key exists and satisfies a condition (if any).

        Parameters
        ----------
        key : str or bytes
            Unqualified key name to check
        condition : callable, optional
            See :meth:`wait_key`'s docstring for the details
        message : tuple, optional
            A tuple of the form (key, value) or (key, value, timestamp).
            The first indicates an update to an immutable, and the third an
            update to a mutable.

            If specified, this is used to find the latest value and timestamp
            (if available) of the key instead of retrieving it from the backend.
        """
        if condition is None:
            return message is not None or key in self

        for prefix in self._prefixes:
            full_key = prefix + _ensure_binary(key)
            if message is not None and full_key == message[0]:
                value = message[1]
                timestamp = None if len(message) == 2 else message[2]
            else:
                try:
                    value = self._backend.get_immutable(full_key)
                    if value is None:
                        continue      # Key does not exist, so try the next one
                    timestamp = None
                except ImmutableKeyError:
                    values = self._backend.get_range(full_key, _INF, _INF, True, False)
                    if not values:
                        continue      # Could happen if key is deleted in between
                    value = values[0][0]
                    timestamp = values[0][1]
            return condition(decode_value(value), timestamp)
        return False    # Key does not exist

    def wait_key(self, key, condition=None, timeout=None, cancel_future=None):
        """Wait for a key to exist, possibly with some condition.

        Parameters
        ----------
        key : str or bytes
            Key name to monitor
        condition : callable, signature `bool = condition(value, ts)`, optional
            If not specified, wait until the key exists. Otherwise, the
            callable should have the signature `bool = condition(value, ts)`
            where `value` is the latest value of the key, `ts` is its
            associated timestamp (or None if immutable), and the return value
            indicates whether the condition is satisfied.
        timeout : float, optional
            If specified and the condition is not met within the time limit,
            an exception is thrown.
        cancel_future : future, optional
            If not ``None``, a future object (e.g.
            :class:`concurrent.futures.Future` or :class:`trollius.Future`). If
            ``cancel_future.done()`` is true before the timeout, raises
            :exc:`CancelledError`. In the current implementation, it is only
            polled once a second, rather than waited for.

        Raises
        ------
        TimeoutError
            if a timeout was specified and was exceeded
        CancelledError
            if a cancellation future was specified and done
        """
        key = _ensure_binary(key)
        # First check if condition is already satisfied, in which case we
        # don't need to create a pubsub connection.
        if self._check_condition(key, condition):
            return
        key_str = _display_str(key)

        def check_cancelled():
            if cancel_future is not None and cancel_future.done():
                raise CancelledError('Wait for {} cancelled'.format(key_str))

        check_cancelled()
        monitor = self._backend.monitor_keys([prefix + key
                                              for prefix in self._prefixes])
        with contextlib.closing(monitor):
            monitor.send(None)   # Just to start the generator going
            start = time.time()
            while True:
                # redis-py automatically reconnects to the server if the connection
                # goes down, but we might miss messages in that case. So rather
                # than waiting an arbitrarily long time, we make sure to poll from
                # time to time. This also allows the cancellation future to be
                # polled.
                check_cancelled()
                get_timeout = 1.0
                if timeout is not None:
                    remain = (start + timeout) - time.time()
                    if remain <= 0:
                        raise TimeoutError('Timed out waiting for {} after {}s'
                                           .format(key_str, timeout))
                    get_timeout = min(get_timeout, remain)
                message = monitor.send(get_timeout)
                if message is None:
                    continue
                if len(message) <= 1:
                    # The monitor thinks it's worth checking again, but doesn't
                    # have enough information to be useful.
                    message = None
                if self._check_condition(key, condition, message):
                    return

    def _get_immutable(self, full_key, return_encoded=False):
        """Return a fully-qualified key of string type."""
        str_val = self._backend.get_immutable(full_key)
        if str_val is None:
            raise KeyError
        if return_encoded:
            return str_val
        return decode_value(str_val)

    def _get(self, key, return_encoded=False):
        key = _ensure_binary(key)
        for prefix in self._prefixes:
            full_key = prefix + key
            try:
                return self._get_immutable(full_key, return_encoded)
                 # assume simple string type for immutable
            except KeyError:
                pass     # Key does not exist at all - try next prefix
            except ImmutableKeyError:
                # It's a mutable; get the latest value
                values = self._backend.get_range(full_key, _INF, _INF, True, False)
                assert values is not None, "key was deleting during _get"
                assert values, "key is mutable but has no value???"
                value = values[0][0]
                if return_encoded:
                    return value
                return decode_value(value)
        raise KeyError('{} not found'.format(_display_str(key)))

    def get(self, key, default=None, return_encoded=False):
        """Get a single value from the model.

        Parameters
        ----------
        default : object, optional
            Object to return if key not found
        return_encoded : bool, optional
            Default 'False' - return values are first decoded from internal storage
            'True' - return values are retained in encoded form.

        Returns
        -------
        value
            for non-immutable key return the most recent value
        """
        try:
            return self._get(key, return_encoded)
        except KeyError:
            return default

    def get_range(self, key, st=None, et=None,
                  include_previous=None, include_end=False, return_encoded=False):
        """Get the range of values specified by the key and timespec from the model.

        Parameters
        ----------
        key : str or bytes
            Database key to extract
        st : float, optional
            Start time, default returns the most recent value prior to et
        et: float, optional
            End time, defaults to the end of time
        include_previous : bool, optional
            If True, the method also returns the last value
            prior to the start time (if any). This defaults to False if st is
            specified and True if st is unspecified.
        include_end : bool, optional
            If False (default), returns values in [st, et), otherwise [st, et].
        return_encoded : bool, optional
            Default 'False' - return values are first decoded from internal storage
            'True' - return values are retained in encoded form.

        Returns
        -------
        list
            list of (value, time) records in specified time range

        Raises
        ------
        KeyError
            if `key` does not exist (with any prefix)
        ImmutableKeyError
            if `key` refers to an immutable key

        Notes
        -----
        By default, timestamps exactly equal to the start time are included,
        while those equal to the end time are excluded.

        Usage examples:

        get_range('key')
            returns most recent record

        get_range('key',st=0)
            returns list of all records in the telescope state database

        get_range('key',st=0,et=t1)
            returns list of all records before time t1

        get_range('key',st=t0,et=t1)
            returns list of all records in the range [t0,t1)

        get_range('key',st=t0)
            returns list of all records after time t0

        get_range('key',et=t1)
            returns the most recent record prior to time t1
        """
        # set up include_previous and st default values
        if include_previous is None:
            include_previous = True if st is None else False
        if et is None:
            et = _INF
        else:
            et = float(et)
        if st is None:
            st = et
        else:
            st = float(st)
        if math.isnan(st) or math.isnan(et):
            raise InvalidTimestampError('cannot use NaN start or end time')

        key = _ensure_binary(key)
        for prefix in self._prefixes:
            full_key = prefix + key
            try:
                ret_vals = self._backend.get_range(full_key, st, et,
                                                   include_previous, include_end)
            except ImmutableKeyError:
                raise ImmutableKeyError('{} is immutable, cannot use get_range'
                                        .format(_display_str(full_key)))
            if ret_vals is not None:
                if not return_encoded:
                    ret_vals = [(decode_value(value), timestamp)
                                for value, timestamp in ret_vals]
                return ret_vals
        raise KeyError('{} not found'.format(_display_str(key)))
