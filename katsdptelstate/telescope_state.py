from __future__ import print_function, division, absolute_import
import redis
import fakeredis
import struct
import time
try:
    import cPickle as pickle
except ImportError:
    import pickle
import logging
import argparse
import numpy as np
import contextlib

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

class NamespaceError(TelstateError):
    """An attempt was made to add a key to a child namespace when already in the parent"""


# XXX This is a crude workaround for fakeredis 0.8.2 which crashes when trying
# to send the packed timestamp as part of a pubsub message on Python 2.
# It insists on encoding the string payload to bytes, leading to error message
# "UnicodeDecodeError: 'ascii' codec can't decode byte 0xd6 in position 1:
# ordinal not in range(128)". It does not affect Python 3 as the payload is
# then already in bytes format (struct + pickle ensures that).
# This has been reported as https://github.com/jamesls/fakeredis/issues/146
# and once it is resolved this patch can go away.
def _monkeypatched_fakeredis_send(self, message_type, pattern, channel, data):
    """This avoids encoding channel and data strings as they are bytes already."""
    msg = {'type': message_type, 'pattern': pattern,
           'channel': channel, 'data': data}
    self._q.put(msg)
    return 1
fakeredis.FakePubSub._send = _monkeypatched_fakeredis_send


class TelescopeState(object):
    """Interface to a collection of attributes and sensors stored in a redis database.

    There are two types of keys permitted: single immutable values, and mutable
    keys where the full history of values is stored with timestamps. These are
    mapped to the redis string and zset types. A redis database used with this
    class must *only* be used with this class, as it does not deal with other
    types of values.

    Keys are arranged into a hierarchy, separated by dots. For convenience, an
    instance of this class can be in a "namespace", which is a position in this
    hierarchy. Name lookups walk upwards in the hierarchy until the key is
    found. New keys are added into the current namespace.

    It is an error to add a key when it already exists in a parent's namespace,
    because it can change the return value when another client queries an
    immutable key. This will raise :exc:`NamespaceError`, although the check is
    not yet atomic so it is possible for this to be violated.

    Note that it is *not* an error to add a key that already exists in a child
    namespace. This is partly because it does not violate immutability in the
    same way, and partly because it would be prohibitively expensive to
    check. Nevertheless, it is highly recommended that this is avoided.

    Parameters
    ----------
    endpoint : str or :class:`~katsdptelstate.endpoint.Endpoint`
        The address of the redis server (if a string, it is passed to the
        :class:`~katsdptelstate.endpoint.Endpoint` constructor). If empty, a
        :class:`fakeredis.FakeStrictRedis` instance is used instead.
    db : int
        Database number within the redis server
    prefixes : list of str
        Prefixes that will be tried in turn for key lookup. This should not be
        used directly: it is intended for use by :meth:`namespace`.
    base : :class:`~katsdptelstate.telescope_state.TelescopeState`
        Existing telescope state instance, from which the underlying redis
        connection will be taken. This should not be used directly: it is
        intended for use by :meth:`namespace`. If specified, `endpoint` and
        `db` are ignored.
    """
    def __init__(self, endpoint='', db=0, prefixes=None, base=None):
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
                self._r = redis.StrictRedis(host=endpoint.host, db=db)
            self._ps = self._r.pubsub(ignore_subscribe_messages=True)
            # subscribe to the telescope model info channel
            self._default_channel = 'tm_info'
            self._ps.subscribe(self._default_channel)
        if not prefixes:
            prefixes = ['']
        self._prefixes = prefixes

    @property
    def prefixes(self):
        return self._prefixes

    def namespace(self, name):
        """Obtain a view with a child namespace."""
        if '.' in name:
            # Work recursively, so that each level gets into prefixes
            head, tail = name.split('.', 1)
            return self.namespace(head).namespace(tail)
        else:
            new_prefix = self._prefixes[0] + name + '.'
            return self.__class__(None, None, prefixes=[new_prefix] + self._prefixes, base=self)

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
        val = self._get(key)
        if val is None: raise AttributeError('{} not found'.format(key))
        return val

    def __getitem__(self, key):
        val = self._get(key)
        if val is None: raise KeyError('{} not found'.format(key))
        return val

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

    def keys(self, filter='*', show_counts=False):
        """Return a list of keys currently in the model.

        This function ignores the namespace, and returns all keys with
        fully-qualified names.

        Parameters
        ----------
        filter : str, optional
            Wildcard string passed to redis to restrict keys
        show_counts : bool, optional
            If true, return the number of entries for each mutable key.

        Returns
        -------
        keys : list
            If `show_counts` is ``False``, each entry is a key name;
            otherwise, it is a tuple of `name`, `count`, where `count` is 1 for
            immutable keys. The keys are returned in sorted order.
        """
        key_list = []
        if show_counts:
            keys = self._r.keys(filter)
            for k in keys:
                # Ideally we'd just try zcard and fall back otherwise, but
                # fakeredis doesn't yet handle this correctly.
                type_ = self._r.type(k)
                if type_ == b'zset':
                    kcount = self._r.zcard(k)
                else:
                    kcount = 1
                key_list.append((k, kcount))
        else:
            key_list = self._r.keys(filter)
        key_list.sort()
        return key_list

    def delete(self, key):
        """Remove a key, and all values, from the model.

        The key is deleted from the namespace and any ancestor namespace.

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
        NamespaceError
            if the key exists but in an ancestor namespace
        redis.ResponseError
            if `key` already exists with a different mutability
        """
        if key in self.__class__.__dict__:
            raise InvalidKeyError("The specified key already exists as a class method and thus cannot be used.")
         # check that we are not going to munge a class method
        full_key = self._prefixes[0] + key
        existing_type = self._r.type(full_key)
        if existing_type == b'none':
            # Check that we're not going to shadow an existing key
            for prefix in self._prefixes[1:]:
                if self._r.exists(prefix + key):
                    raise NamespaceError('Key {} would shadow {}'.format(full_key, prefix + key))
        elif existing_type == b'string':
            if immutable and pickle.dumps(value, protocol=PICKLE_PROTOCOL) == self._r.get(full_key):
                logger.info('Attribute {} updated with the same value'.format(full_key))
                return True
            raise ImmutableKeyError("Attempt to overwrite immutable key {}.".format(full_key))
        str_val = pickle.dumps(value, protocol=PICKLE_PROTOCOL)
        if immutable:
            ret = self._r.set(full_key, str_val)
        else:
            ts = float(ts) if ts is not None else time.time()
            packed_ts = struct.pack('>d', ts)
            str_val = packed_ts + str_val
            ret = self._r.zadd(full_key, 0, str_val)
        self._r.publish('update/' + full_key, str_val)
        return ret

    def _check_condition(self, key, condition=None, str_val=None):
        """Check whether key exists and satisfies condition (if any).

        Parameters
        ----------
        key : str
            Key name to monitor
        condition : callable, optional
            See :meth:`wait_key`'s docstring for the details
        str_val : str, optional
            If specified, this is used to find the latest value and timestamp
            (if available) of the key instead of connecting to the backend.
            It has the same format as the string values stored in the backend.
        """
        if str_val is None and key not in self:
            return False
        if condition is None:
            return True
        if self.is_immutable(key):
            value = self._get(key) if str_val is None else \
                pickle.loads(str_val)
            return condition(value, None)
        else:
            value, ts = self.get_range(key)[0] if str_val is None else \
                self._strip(str_val)
            return condition(value, ts)

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
                    if self._check_condition(key, condition, message['data']):
                        return

    def _get_immutable(self, full_key, return_pickle=False):
        """Return a fully-qualified key of string type."""
        str_val = self._r.get(full_key)
        if return_pickle:
            return str_val
        try:
            return pickle.loads(str_val)
        except pickle.UnpicklingError:
            return str_val

    def _get(self, key, return_pickle=False):
        for prefix in self._prefixes:
            full_key = prefix + key
            if self._r.exists(full_key):
                try:
                    return self._get_immutable(full_key, return_pickle)
                     # assume simple string type for immutable
                except redis.ResponseError:
                    return self._strip(self._r.zrange(full_key, -1, -1)[0], return_pickle)[0]
        return None

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
        value - for non immutable key return the most recent value

        """
        val = self._get(key, return_pickle)
        if val is None: return default
        return val

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
        list of (value, time) or value (for immutables) records between specified time range

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
                if type_ == b'string':
                    # immutables return value with no timestamp information
                    return self._get_immutable(full_key, return_pickle=return_pickle)
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


class ArgumentParser(argparse.ArgumentParser):
    """Argument parser that can load defaults from a telescope state. It can be
    used as a drop-in replacement for `argparse.ArgumentParser`. It adds the
    options `--telstate` and `--name`. The first takes the hostname of a
    telescope state repository (optionally with a port). If specified,
    `parse_args` will first connect to this host and fetch defaults (which
    override the defaults specified by `add_argument`). The telescope state
    will also be available in the returned `argparse.Namespace`.

    If `name` is specified, it consists of a dot-separated list of
    identifiers, specifying a path through a tree of dictionaries of config.
    For example, `foo.0` will cause configuration to be searched in
    `config`, `config["foo"]`, `config["foo"]["0"]`, `config.foo` and
    `config.foo.0`. If configuration is found in multiple places, the most
    specific location will be used first, breaking ties to use `config.*`
    in preference to embedded dictionaries within `config`. It is not an error
    for one of these dictionaries not to exist, but it is an error if a name is
    found but is not a dictionary.

    New code generating config dictionaries is advised to use the `config.*`
    form, as it is more scalable (no need to fetch unrelated config).
    Sub-config embedded within `config` is supported for backwards
    compatibility.

    A side-effect of the implementation is that calling `parse_args` or
    `parse_known_args` permanently changes the defaults. A parser should thus
    only be used once and then thrown away. Also, because it changes the
    defaults rather than injecting actual arguments, argparse features like
    required arguments and mutually exclusive groups might not work as
    expected.

    Parameters
    ----------
    config_key : str, optional
        Name of the config dictionary within the telescope state (default: `config`)
    """

    _SPECIAL_NAMES = ['telstate', 'name']

    def __init__(self, *args, **kwargs):
        self.config_key = kwargs.pop('config_key', 'config')
        super(ArgumentParser, self).__init__(*args, **kwargs)
        # Create a separate parser that will extract only the special args
        self.config_parser = argparse.ArgumentParser(add_help=False)
        self.config_parser.add_argument('-h', '--help', action=_HelpAction, default=argparse.SUPPRESS, parser=self)
        for parser in [super(ArgumentParser, self), self.config_parser]:
            parser.add_argument('--telstate', help='Telescope state repository from which to retrieve config', metavar='HOST[:PORT]')
            parser.add_argument('--name', type=str, default='', help='Name of this process for telescope state configuration')
        self.config_keys = set()

    def add_argument(self, *args, **kwargs):
        action = super(ArgumentParser, self).add_argument(*args, **kwargs)
        # Check if we have finished initialising ourself yet
        if hasattr(self, 'config_keys'):
            if action.dest is not None and action.dest is not argparse.SUPPRESS:
                self.config_keys.add(action.dest)
        return action

    def _load_defaults(self, telstate, name):
        config_dict = telstate.get(self.config_key, {})
        parts = name.split('.')
        cur = config_dict
        dicts = [cur]
        split_name = self.config_key
        for part in parts:
            if cur is not None:
                cur = cur.get(part)
                if cur is not None:
                    dicts.append(cur)
            split_name += '.'
            split_name += part
            dicts.append(telstate.get(split_name, {}))

        # Go from most specific to most general, so that specific values
        # take precedence.
        seen = set(self._SPECIAL_NAMES)  # Prevents these being overridden
        for cur in reversed(dicts):
            for key in self.config_keys:
                if key in cur and key not in seen:
                    super(ArgumentParser, self).set_defaults(**{key: cur[key]})
                    seen.add(key)

    def set_defaults(self, **kwargs):
        for special in self._SPECIAL_NAMES:
            if special in kwargs:
                self.config_parser.set_defaults(**{special: kwargs.pop(special)})
        super(ArgumentParser, self).set_defaults(**kwargs)

    def parse_known_args(self, args=None, namespace=None):
        if namespace is None:
            namespace = argparse.Namespace()
        try:
            config_args, other = self.config_parser.parse_known_args(args)
        except argparse.ArgumentError:
            other = args
        else:
            if config_args.telstate is not None:
                try:
                    namespace.telstate = TelescopeState(config_args.telstate)
                except redis.ConnectionError as e:
                    self.error(str(e))
                namespace.name = config_args.name
                self._load_defaults(namespace.telstate, namespace.name)
        return super(ArgumentParser, self).parse_known_args(other, namespace)
