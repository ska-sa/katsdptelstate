from __future__ import print_function, division, absolute_import
import struct
import time
try:
    import cPickle as pickle
except ImportError:
    import pickle
import logging
import argparse
import contextlib
import functools

import numpy as np
import redis
import fakenewsredis as fakeredis

from .endpoint import Endpoint, endpoint_parser


logger = logging.getLogger(__name__)
PICKLE_PROTOCOL = 0         #: Version of pickle protocol to use


class TelstateError(RuntimeError):
    """Base class for errors from this module"""

class InvalidKeyError(TelstateError):
    """A key collides with a class attribute"""

class ImmutableKeyError(TelstateError):
    """An attempt was made to modify an immutable key"""

class TimeoutError(TelstateError):
    """A wait for a key timed out"""

class CancelledError(TelstateError):
    """A wait for a key was cancelled"""


class TelescopeState(object):
    """Interface to a collection of attributes and sensors stored in a redis database.

    There are two types of keys permitted: single immutable values, and mutable
    keys where the full history of values is stored with timestamps. These are
    mapped to the redis string and zset types. A redis database used with this
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
        The address of the redis server (if a string, it is passed to the
        :class:`~katsdptelstate.endpoint.Endpoint` constructor). If empty, a
        :class:`fakeredis.FakeStrictRedis` instance is used instead.
    db : int
        Database number within the redis server
    prefixes : tuple of str
        Prefixes that will be tried in turn for key lookup. While this can be
        specified directly for advanced cases, it is normally generated by
        :meth:`view`. Writes are made using the first prefix in the list.
    base : :class:`~katsdptelstate.telescope_state.TelescopeState`
        Existing telescope state instance, from which the underlying redis
        connection will be taken. This allows new views to be created by
        specifying `prefixes`, without creating new redis connections.
        If specified, `endpoint` and `db` are ignored.
    """
    SEPARATOR = '_'

    def __init__(self, endpoint='', db=0, prefixes=('',), base=None):
        if base is not None:
            self._r = base._r
            self._ps = base._ps
        else:
            if not isinstance(endpoint, Endpoint):
                endpoint = endpoint_parser(default_port=None)(endpoint)
            if not endpoint.host:
                self._r = fakeredis.FakeStrictRedis(db=db)
            elif endpoint.port is not None:
                self._r = redis.StrictRedis(host=endpoint.host, port=endpoint.port,
                                            db=db, socket_timeout=5)
            else:
                self._r = redis.StrictRedis(host=endpoint.host,
                                            db=db, socket_timeout=5)
            self._ps = self._r.pubsub(ignore_subscribe_messages=True)
            # subscribe to the telescope model info channel
            self._default_channel = 'tm_info'
            self._ps.subscribe(self._default_channel)
        # Force to tuple, in case it is some other iterable
        self._prefixes = tuple(prefixes)

    @property
    def prefixes(self):
        return self._prefixes

    def view(self, name, add_separator=True, exclusive=False):
        """Create a view with an extra name in the list of namespaces.

        Returns a new view with `name` added as the first prefix, or the
        only prefix if `exclusive` is true. If `name` is non-empty and does not
        end with the separator, it is added (unless `add_separator` is
        false).
        """
        if name != '' and name[-1] != self.SEPARATOR and add_separator:
            name += self.SEPARATOR
        if exclusive:
            prefixes = (name,)
        else:
            prefixes = (name,) + self._prefixes
        return self.__class__(None, None, prefixes=prefixes, base=self)

    def root(self):
        """Create a view containing only the root namespace."""
        return self.__class__(base=self)

    def _strip(self, str_val, return_pickle=False):
        if len(str_val) < 8: return None
        ts = struct.unpack('>d', str_val[:8])[0]
        if return_pickle: return (str_val[8:], ts)
        try:
            ret_val = pickle.loads(str_val[8:])
        except pickle.UnpicklingError:
            ret_val = str_val[8:]
        return (ret_val, ts)

    def __getattr__(self, key):
        try:
            return self._get(key)
        except KeyError as error:
            raise AttributeError(str(error))

    def __getitem__(self, key):
        return self._get(key)

    def __contains__(self, x):
        return self.has_key(x)

    def send_message(self, data, channel=None):
        """Broadcast a message to all telescope model users."""
        if channel is None: channel = self._default_channel
        return self._r.publish(channel, data)

    def get_message(self, channel=None):
        """Get the oldest unread telescope model message."""
        if channel is None: channel = self._default_channel
        msg = self._ps.get_message(channel)
        if msg is not None: msg = msg['data']
        return msg

    def has_key(self, key_name):
        """Check to see if the specified key exists in the database."""
        for prefix in self._prefixes:
            if self._r.exists(prefix + key_name):
                return True
        return False

    def is_immutable(self, key):
        """Check to see if the specified key is an immutable."""
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
        filter : str, optional
            Wildcard string passed to redis to restrict keys

        Returns
        -------
        keys : list
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
        for prefix in self._prefixes:
            self._r.delete(prefix + key)

    def clear(self):
        """Remove all keys in all namespaces.

        .. note::

            This function should be used rarely, ideally only in tests, as it
            violates the immutability of keys added with ``immutable=True``.
        """
        return self._r.flushdb()

    def add(self, key, value, ts=None, immutable=False):
        """Add a new key / value pair to the model.

        If `immutable` is true, then either the key must not previously have
        been set, or it must have been previous set immutable with exactly the
        same value (same pickle). Thus, immutable keys only ever have one value
        for the lifetime of the telescope state. They also have no associated
        timestamp.

        Parameters
        ----------
        key : str
            Key name, which must not collide with a class attribute
        value : object
            Arbitrary value (must be picklable)
        ts : float, optional
            Timestamp associated with the update, ignored for immutables. If not
            specified, defaults to ``time.time()``.
        immutable : bool, optional
            See description above.

        Raises
        ------
        InvalidKeyError
            if `key` collides with a class member name
        ImmutableKeyError
            if an attempt is made to change the value of an immutable
        redis.ResponseError
            if `key` already exists with a different mutability
        """
        if key in self.__class__.__dict__:
            raise InvalidKeyError("The specified key already exists as a class method and thus cannot be used.")
         # check that we are not going to munge a class method
        full_key = self._prefixes[0] + key
        str_val = pickle.dumps(value, protocol=PICKLE_PROTOCOL)
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
                if str_val != old:
                    raise ImmutableKeyError(
                        'Attempt to change value of immutable key {} from {!r} to {!r}.'.format(
                            full_key, pickle.loads(old), value))
                else:
                    logger.info('Attribute {} updated with the same value'.format(full_key))
                    return True
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

        self._r.publish('update/' + full_key, str_val)
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
            full_key = prefix + key
            match = full_key.encode('utf-8') == message_key
            encoded_key = full_key.encode('utf-8')
            type_ = self._r.type(prefix + key)
            if type_ == b'string':
                # Immutable
                if match:
                    value = message_value
                else:
                    value = self._r.get(full_key)
                return condition(pickle.loads(value), None)
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

        # First check if condition is already satisfied, in which case we
        # don't need to create a pubsub connection.
        if self._check_condition(key, condition):
            return
        check_cancelled()
        p = self._r.pubsub()
        for prefix in self._prefixes:
            p.subscribe('update/' + prefix + key)
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

    def _get_immutable(self, full_key, return_pickle=False):
        """Return a fully-qualified key of string type."""
        str_val = self._r.get(full_key)
        if str_val is None:
            raise KeyError
        if return_pickle:
            return str_val
        return pickle.loads(str_val)

    def _get(self, key, return_pickle=False):
        for prefix in self._prefixes:
            full_key = prefix + key
            try:
                return self._get_immutable(full_key, return_pickle)
                 # assume simple string type for immutable
            except KeyError:
                pass     # Key does not exist at all - try next prefix
            except redis.ResponseError as error:
                if not error.args[0].startswith('WRONGTYPE '):
                    raise
                return self._strip(self._r.zrange(full_key, -1, -1)[0], return_pickle)[0]
        raise KeyError('{} not found'.format(key))

    def get(self, key, default=None, return_pickle=False):
        """Get a single value from the model.

        Parameters
        ----------
        default : object, optional
            Object to return if key not found
        return_pickle : bool, optional
            Default 'False' - return values are unpickled from internal storage before returning
            'True' - return values are retained in pickled form.

        Returns
        -------
        value
            for non-immutable key return the most recent value
        """
        try:
            return self._get(key, return_pickle)
        except KeyError:
            return default

    def get_range(self, key, st=None, et=None, return_format=None, include_previous=None, return_pickle=False):
        """Get the range of values specified by the key and timespec from the model.

        Parameters
        ----------
        key : string
            Database key to extract
        st : float, optional
            Start time, default returns the most recent value prior to et
        et: float, optional
            End time, defaults to the end of time
        return_format : string, optional
            'recarray' returns values and times as numpy recarray with keys 'value' and 'time'
            'None' returns values and times as 2D list of elements format (value, time)
        include_previous : bool, optional
            If True, the method returns [st, et) as well as the last value
            prior to the start time (if any). This defaults to False if st is
            specified and True if st is unspecified.
        return_pickle : bool, optional
            Default 'False' - return values are unpickled from internal storage before returning
            'True' - return values are retained in pickled form.

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
        Timestamps exactly equal to the start time are included, while those equal to the
        end time are excluded.

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

        # set up include_previous default values
        if include_previous is None:
            include_previous = True if st is None else False

        if et is None:
            # The special positively infinite string represents the end of time
            packed_et = b'+'
        elif et <= 0.0:
            # The special negatively infinite string represents the dawn of time
            packed_et = b'-'
        else:
            packed_et = b'(' + struct.pack('>d', float(et))

        if st is None:
            packed_st = packed_et
        elif st <= 0.0:
            packed_st = b'-'
        else:
            packed_st = b'[' + struct.pack('>d', float(st))

        ret_vals = []
        if include_previous and packed_st != b'-':
            ret_vals += self._r.zrevrangebylex(full_key, packed_st, b'-', 0, 1)
        # Avoid talking to redis if it is going to be futile
        if packed_st != packed_et:
            ret_vals += self._r.zrangebylex(full_key, packed_st, packed_et)
        ret_list = [self._strip(str_val, return_pickle) for str_val in ret_vals]

        if return_format is None:
            return ret_list
        elif return_format == 'recarray':
            val_shape, val_type = None, None
            if ret_list != []:
                val_shape = np.array(ret_list[0][0]).shape
                val_type = np.array(ret_list[0][0]).dtype
                if val_type.type is np.str_ or val_type.type is np.bytes_:
                    val_type = max([d.dtype for d in np.atleast_2d(ret_list)[:, 0]])
            return np.array(ret_list, dtype=[('value', val_type, val_shape), ('time', np.float)])
        else:
            raise ValueError('Unknown return_format {}'.format(return_format))


class _HelpAction(argparse.Action):
    """Class modelled on argparse._HelpAction that prints help for the
    main parser."""
    def __init__(self,
                 option_strings,
                 parser,
                 dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS,
                 help=None):
        super(_HelpAction, self).__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help)
        self._parser = parser

    def __call__(self, parser, namespace, values, option_string=None):
        self._parser.print_help()
        self._parser.exit()
