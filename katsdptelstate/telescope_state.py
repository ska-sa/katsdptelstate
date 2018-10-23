from __future__ import print_function, division, absolute_import

import struct
import time
try:
    import cPickle as pickle
except ImportError:
    import pickle
import logging
import contextlib
import functools
import sys
import io

import redis
import msgpack
import numpy as np

from .endpoint import Endpoint, endpoint_parser
from .tabloid_redis import TabloidRedis
try:
    from . import rdb_reader
except ImportError as _rdb_reader_import_error:
    rdb_reader = None


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
ENCODING_DEFAULT = ENCODING_PICKLE

#: All encodings that can be used with :func:`encode_value`
ALLOWED_ENCODINGS = frozenset([ENCODING_PICKLE, ENCODING_MSGPACK])

MSGPACK_EXT_TUPLE = 1
MSGPACK_EXT_COMPLEX128 = 2
MSGPACK_EXT_NDARRAY = 3           # .npy format
MSGPACK_EXT_NUMPY_SCALAR = 4      # dtype descriptor then raw value


class TelstateError(RuntimeError):
    """Base class for errors from this module"""


class ConnectionError(TelstateError):
    """The initial connection to the Redis server failed."""


class InvalidKeyError(TelstateError):
    """A key collides with a class attribute"""


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


if sys.version_info.major >= 3:
    # See https://stackoverflow.com/questions/11305790
    _pickle_loads = functools.partial(pickle.loads, encoding='latin1')
    _text_type = str
else:
    _pickle_loads = pickle.loads
    _text_type = unicode


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
        raise EncodeError('cannot encode dtype {} as it contains objects'.format(value.dtype))
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
        raise DecodeError('cannot decode dtype {} as it contains objects'.format(dtype))
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
        return msgpack.ExtType(MSGPACK_EXT_COMPLEX128, struct.pack('>dd', value.real, value.imag))
    else:
        raise EncodeError('do not know how to encode type {}'.format(value.__class__.__name__))


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
    return msgpack.packb(value, use_bin_type=True, strict_types=True, default=_msgpack_default)


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
        Encoding method to use. It must be one of the values in :const:`ALLOWED_ENCODINGS`

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


def decode_value(value, allow_pickle=True):
    """Decode a value encoded with :func:`encode_value`.

    The encoded value is self-describing, so it is not necessary to specify
    which encoding was used.

    Parameters
    ----------
    value : bytes
        Encoded value to decode
    allow_pickle : bool, optional
        If false, :const:`ENCODING_PICKLE` is disabled. This may be useful for
        security as pickle decoding can execute arbitrary code. The default is
        true but may change to false in future once other encodings are
        supported and in common usage.

    Raises
    ------
    DecodeError
        if `allow_pickle` is false and `value` is a pickle
    DecodeError
        if there was an error in the encoding of `value`
    """
    if not value:
        raise DecodeError('empty value')
    elif value[:1] == ENCODING_MSGPACK:
        try:
            return _msgpack_decode(value[1:])
        except Exception as error:
            raise DecodeError(str(error))
    elif value[:1] <= ENCODING_PICKLE:
        if allow_pickle:
            try:
                return _pickle_loads(value)
            except Exception as error:
                raise DecodeError(str(error))
        else:
            raise DecodeError('value is a pickle but unpickling is disabled')
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
    if isinstance(a, _text_type) and isinstance(b, _text_type):
        return a == b      # Avoid cost of encoding both to bytes
    elif isinstance(a, (bytes, _text_type)) and isinstance(b, (bytes, _text_type)):
        if isinstance(a, _text_type):
            a = a.encode('utf-8')
        if isinstance(b, _text_type):
            b = b.encode('utf-8')
        return a == b
    else:
        return False


def _as_bytes(value):
    """Force unicode values to bytes by UTF-8 encoding.

    This is intended to mimic redis-py behaviour
    """
    if isinstance(value, _text_type):
        return value.encode('utf-8')
    else:
        return value


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
    endpoint : str or :class:`~katsdptelstate.endpoint.Endpoint`
        The address of the Redis server (if a string, it is passed to the
        :class:`~katsdptelstate.endpoint.Endpoint` constructor). If empty, a
        :class:`tabloid_redis.TabloidRedis` instance is used instead.
    db : int
        Database number within the Redis server
    prefixes : tuple of str/bytes
        Prefixes that will be tried in turn for key lookup. While this can be
        specified directly for advanced cases, it is normally generated by
        :meth:`view`. Writes are made using the first prefix in the list.
    base : :class:`~katsdptelstate.telescope_state.TelescopeState`
        Existing telescope state instance, from which the underlying Redis
        connection will be taken. This allows new views to be created by
        specifying `prefixes`, without creating new Redis connections.
        If specified, `endpoint` and `db` are ignored.

    Raises
    ------
    ConnectionError
        If the initial connection to the (real) Redis server fails
    """
    SEPARATOR = '_'
    SEPARATOR_BYTES = b'_'

    def __init__(self, endpoint='', db=0, prefixes=(b'',), base=None):
        if base is not None:
            self._r = base._r
            self._ps = base._ps
        else:
            self._r = None
            if not isinstance(endpoint, Endpoint):
                endpoint = endpoint_parser(default_port=None)(endpoint)
            if not endpoint.host:
                self._r = TabloidRedis(db=db, singleton=False)
            else:
                redis_kwargs = dict(host=endpoint.host, db=db, socket_timeout=5)
                # If no port is provided, redis will pick its default port
                if endpoint.port is not None:
                    redis_kwargs['port'] = endpoint.port
                self._r = redis.StrictRedis(**redis_kwargs)
            self._ps = self._r.pubsub(ignore_subscribe_messages=True)
             # subscribe to the telescope model info channel
            self._default_channel = 'tm_info'
            try:
                # This is the first command to the server and therefore
                # the first test of its availability
                self._ps.subscribe(self._default_channel)
            except (redis.TimeoutError, redis.ConnectionError) as e:
                # redis.TimeoutError: bad host
                # redis.ConnectionError: good host, bad port
                raise ConnectionError("[{}] {}".format(endpoint, e))
        # Ensure all prefixes are bytes for consistency
        self._prefixes = tuple(_as_bytes(prefix) for prefix in prefixes)

    @property
    def prefixes(self):
        return self._prefixes

    def load_from_file(self, filename):
        """Load keys from a Redis-compatible RDB file.

        Will raise ImportError if the rdbtools package is not installed.
        """
        if rdb_reader is None:
            raise _rdb_reader_import_error
        keys_loaded = rdb_reader.load_from_file(self._r, filename)
        logger.info("Loading {} keys from {}".format(keys_loaded, filename))
        return keys_loaded

    def view(self, name, add_separator=True, exclusive=False):
        """Create a view with an extra name in the list of namespaces.

        Returns a new view with `name` added as the first prefix, or the
        only prefix if `exclusive` is true. If `name` is non-empty and does not
        end with the separator, it is added (unless `add_separator` is
        false).
        """
        name = _as_bytes(name)
        if name != b'' and name[-1:] != self.SEPARATOR_BYTES and add_separator:
            name += self.SEPARATOR_BYTES
        if exclusive:
            prefixes = (name,)
        else:
            prefixes = (name,) + self._prefixes
        return self.__class__(None, None, prefixes=prefixes, base=self)

    def root(self):
        """Create a view containing only the root namespace."""
        return self.__class__(base=self)

    def _strip(self, str_val, return_encoded=False):
        if len(str_val) < 8:
            return None
        ts = struct.unpack('>d', str_val[:8])[0]
        ret_val = str_val[8:]
        if not return_encoded:
            ret_val = decode_value(ret_val)
        return (ret_val, ts)

    def __getattr__(self, key):
        try:
            return self._get(key)
        except KeyError as error:
            raise AttributeError(str(error))

    def __getitem__(self, key):
        return self._get(key)

    def __contains__(self, key_name):
        """Check to see if the specified key exists in the database."""
        key_name = _as_bytes(key_name)
        for prefix in self._prefixes:
            if self._r.exists(prefix + key_name):
                return True
        return False

    def send_message(self, data, channel=None):
        """Broadcast a message to all telescope model users."""
        if channel is None:
            channel = self._default_channel
        return self._r.publish(channel, data)

    def get_message(self, channel=None):
        """Get the oldest unread telescope model message."""
        if channel is None:
            channel = self._default_channel
        msg = self._ps.get_message(channel)
        if msg is not None:
            msg = msg['data']
        return msg

    def is_immutable(self, key):
        """Check to see if the specified key is an immutable."""
        key = _as_bytes(key)
        for prefix in self._prefixes:
            type_ = self._r.type(prefix + key)
            if type_ != b'none':
                return type_ == b'string'
        return False

    def keys(self, filter='*'):
        """Return a list of keys currently in the model.

        This function ignores the prefix list, returns all keys with
        fully-qualified names.

        Parameters
        ----------
        filter : str or bytes, optional
            Wildcard string passed to Redis to restrict keys

        Returns
        -------
        keys : list of bytes
            The key names, in sorted order.
        """
        return sorted(self._r.keys(filter))

    def delete(self, key):
        """Remove a key, and all values, from the model.

        The key is deleted from every namespace in the prefix list.

        .. note::

            This function should be used rarely, ideally only in tests, as it
            violates the immutability of keys added with ``immutable=True``.
        """
        key = _as_bytes(key)
        for prefix in self._prefixes:
            self._r.delete(prefix + key)

    def clear(self):
        """Remove all keys in all namespaces.

        .. note::

            This function should be used rarely, ideally only in tests, as it
            violates the immutability of keys added with ``immutable=True``.
        """
        return self._r.flushdb()

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
        key = _as_bytes(key)
        full_key = self._prefixes[0] + key
        str_val = encode_value(value, encoding)
        if immutable:
            ret = self._r.setnx(full_key, str_val)
            if not ret:
                # The key already exists. Check if the value is the same.
                try:
                    old = self._r.get(full_key)
                except redis.ResponseError as error:
                    if not error.args[0].startswith('WRONGTYPE '):
                        raise
                    raise ImmutableKeyError(
                        'Attempt to overwrite mutable key {} with immutable'.format(full_key))
                try:
                    if not equal_encoded_values(str_val, old):
                        raise ImmutableKeyError(
                            'Attempt to change value of immutable key {} from {!r} to {!r}.'.format(
                                full_key, decode_value(old), value))
                    else:
                        logger.info('Attribute {} updated with the same value'.format(full_key))
                        return True
                except DecodeError as error:
                    raise ImmutableKeyError(
                        'Attempt to set value of immutable key {} to {!r} but failed to '
                        'decode the previous value to compare: {}'
                        .format(full_key, value, error))
        else:
            ts = float(ts) if ts is not None else time.time()
            packed_ts = struct.pack('>d', ts)
            str_val = packed_ts + str_val
            try:
                ret = self._r.zadd(full_key, 0, str_val)
            except redis.ResponseError as error:
                if not error.args[0].startswith('WRONGTYPE '):
                    raise
                raise ImmutableKeyError('Attempt to overwrite immutable key'.format(full_key))

        self._r.publish(b'update/' + full_key, str_val)
        return ret

    def _check_condition(self, key, condition, message=None):
        """Check whether key exists and satisfies a condition (if any).

        Parameters
        ----------
        key : str
            Unqualified key name to check
        condition : callable, optional
            See :meth:`wait_key`'s docstring for the details
        message : dict, optional
            A pubsub message of type 'message'.
            If specified, this is used to find the latest value and timestamp
            (if available) of the key instead of retrieving it from the backend.
        """
        if condition is None:
            return message is not None or key in self

        if message is not None:
            assert message['channel'].startswith(b'update/')
            message_key = message['channel'][7:]
            message_value = message['data']
        else:
            message_key = None
            message_value = None

        for prefix in self._prefixes:
            full_key = prefix + _as_bytes(key)
            match = full_key == message_key
            type_ = self._r.type(full_key)
            if type_ == b'string':
                # Immutable
                if match:
                    value = message_value
                else:
                    value = self._r.get(full_key)
                return condition(decode_value(value), None)
            elif type_ != b'none':
                # Mutable
                if match:
                    value_ts = message_value
                else:
                    value_ts = self._r.zrange(full_key, -1, -1)[0]
                return condition(*self._strip(value_ts))
        return False    # Key does not exist

    def wait_key(self, key, condition=None, timeout=None, cancel_future=None):
        """Wait for a key to exist, possibly with some condition.

        Parameters
        ----------
        key : str
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
        def check_cancelled():
            if cancel_future is not None and cancel_future.done():
                raise CancelledError('wait for {} cancelled'.format(key))

        key = _as_bytes(key)
        # First check if condition is already satisfied, in which case we
        # don't need to create a pubsub connection.
        if self._check_condition(key, condition):
            return
        check_cancelled()
        p = self._r.pubsub()
        for prefix in self._prefixes:
            p.subscribe(b'update/' + prefix + key)
        with contextlib.closing(p):
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
                        raise TimeoutError(
                            'Timed out waiting for {} after {}s'.format(key, timeout))
                    get_timeout = min(get_timeout, remain)
                message = p.get_message(timeout=get_timeout)
                if message is None:
                    continue
                if message['type'] == 'subscribe':
                    # An update may have happened between our first check and our
                    # subscription taking effect, so check again.
                    if self._check_condition(key, condition):
                        return
                elif message['type'] == 'message':
                    if self._check_condition(key, condition, message):
                        return

    def _get_immutable(self, full_key, return_encoded=False):
        """Return a fully-qualified key of string type."""
        str_val = self._r.get(full_key)
        if str_val is None:
            raise KeyError
        if return_encoded:
            return str_val
        return decode_value(str_val)

    def _get(self, key, return_encoded=False):
        key = _as_bytes(key)
        for prefix in self._prefixes:
            full_key = prefix + key
            try:
                return self._get_immutable(full_key, return_encoded)
                 # assume simple string type for immutable
            except KeyError:
                pass     # Key does not exist at all - try next prefix
            except redis.ResponseError as error:
                if not error.args[0].startswith('WRONGTYPE '):
                    raise
                return self._strip(self._r.zrange(full_key, -1, -1)[0], return_encoded)[0]
        raise KeyError('{} not found'.format(key))

    def get(self, key, default=None, return_encoded=False):
        """Get a single value from the model.

        Parameters
        ----------
        default : object, optional
            Object to return if key not found
        return_encoded : bool, optional
            Default 'False' - return values are decoded from internal storage before returning
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

    @classmethod
    def _pack_query_time(cls, time, include_end=False):
        """Create a query value for a ZRANGEBYLEX query.

        If include_end is true, the time is incremented by the smallest possible
        amount, so that when used as an end it will be inclusive rather than
        exclusive.

        A time of None indicates +infinity, while any negative value is
        equivalent to -infinity (since all actual timestamps are non-negative).
        """
        if time is None:
            # The special positively infinite string represents the end of time
            return b'+'
        elif time < 0.0 or (time == 0.0 and not include_end):
            # The special negatively infinite string represents the dawn of time
            return b'-'
        else:
            # abs ensures that -0.0 becomes +0.0, which encodes differently
            packed_time = struct.pack('>d', abs(float(time)))
            if include_end:
                # Increment to the next possible encoded value. Note that this
                # cannot overflow because the sign bit is initially clear.
                packed_time = struct.pack('>Q', struct.unpack('>Q', packed_time)[0] + 1)
            # Values are encoded and hence always non-empty, so [ vs ( is irrelevant
            return b'[' + packed_time

    def get_range(self, key, st=None, et=None,
                  include_previous=None, include_end=False, return_encoded=False):
        """Get the range of values specified by the key and timespec from the model.

        Parameters
        ----------
        key : string
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
            Default 'False' - return values are decoded from internal storage before returning
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

        get_range('key_name')
            returns most recent record

        get_range('key_name',st=0)
            returns list of all records in the telescope state database

        get_range('key_name',st=0,et=t1)
            returns list of all records before time t1

        get_range('key_name',st=t0,et=t1)
            returns list of all records in the range [t0,t1)

        get_range('key_name',st=t0)
            returns list of all records after time t0

        get_range('key_name',et=t1)
            returns the most recent record prior to time t1
        """
        key = _as_bytes(key)
        for prefix in self._prefixes:
            full_key = prefix + key
            type_ = self._r.type(full_key)
            if type_ != b'none':
                if type_ != b'zset':
                    raise ImmutableKeyError('{} is immutable, cannot use get_range'.format(full_key))
                else:
                    break
        else:
            raise KeyError('{} not found'.format(key))

        # set up include_previous and st default values
        if include_previous is None:
            include_previous = True if st is None else False
        if st is None:
            st = et

        packed_st = self._pack_query_time(st)
        packed_et = self._pack_query_time(et, include_end)
        ret_vals = []
        if include_previous and packed_st != b'-':
            ret_vals += self._r.zrevrangebylex(full_key, packed_st, b'-', 0, 1)

        # Avoid talking to Redis if it is going to be futile
        if packed_st != packed_et:
            ret_vals += self._r.zrangebylex(full_key, packed_st, packed_et)
        return [self._strip(str_val, return_encoded) for str_val in ret_vals]
