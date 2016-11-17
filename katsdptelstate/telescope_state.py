from __future__ import print_function, division, absolute_import
import redis
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


class InvalidKeyError(Exception):
    pass

class ImmutableKeyError(Exception):
    pass

class TimeoutError(Exception):
    pass


class TelescopeState(object):
    def __init__(self, endpoint='localhost', db=0):
        if not isinstance(endpoint, Endpoint):
            endpoint = endpoint_parser(default_port=None)(endpoint)
        if endpoint.port is not None:
            self._r = redis.StrictRedis(host=endpoint.host, port=endpoint.port,
                                        db=db, socket_timeout=5)
        else:
            self._r = redis.StrictRedis(host=endpoint.host, db=db)
        self._ps = self._r.pubsub(ignore_subscribe_messages=True)
        self._default_channel = 'tm_info'
        self._ps.subscribe(self._default_channel)
         # subscribe to the telescope model info channel

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
        if val is None: raise AttributeError
        return val

    def __getitem__(self, key):
        val = self._get(key)
        if val is None: raise KeyError
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
        return self._r.exists(key_name)

    def is_immutable(self, key):
        """Check to see if the specified key is an immutable."""
        return self._r.type(key) == b'string'

    def keys(self, filter='*', show_counts=False):
        """Return a list of keys currently in the model."""
        key_list = []
        if show_counts:
            keys = self._r.keys(filter)
            for k in keys:
                try:
                    kcount = self._r.zcard(k)
                except redis.ResponseError:
                    kcount = 1
                key_list.append((k, kcount))
        else:
            key_list = self._r.keys(filter)
        key_list.sort()
        return key_list

    def delete(self, key):
        """Remove a key, and all values, from the model."""
        return self._r.delete(key)

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
        if ts is None and not immutable: ts = time.time()
        existing_type = self._r.type(key)
        if existing_type == b'string':
            if immutable and pickle.dumps(value) == self._r.get(key):
                logger.info('Attribute {} updated with the same value'.format(key))
                return True
            raise ImmutableKeyError("Attempt to overwrite immutable key {}.".format(key))
        pickled = pickle.dumps(value)
        if immutable:
            ret = self._r.set(key, pickled)
        else:
            packed_ts = struct.pack('>d', float(ts))
            ret = self._r.zadd(key, 0, packed_ts + pickled)
        self._r.publish('update/' + key, pickled)
        return ret

    def wait_key(self, key, condition=None, timeout=None):
        """Wait for a key to exist, possibly with some condition.

        Parameters
        ----------
        key : str
            Key name to monitor
        condition : callable, optional
            If not specified, wait until the key exists. Otherwise, it must
            take a single argument, the value of the key, and return a
            boolean to indicate whether the condition is satisfied.
        timeout : float, optional
            If specified and the condition is not met within the time limit,
            an exception is thrown.

        Raises
        ------
        TimeoutError
            if a timeout was specified and was exceeded
        """
        def check():
            if self._r.exists(key):
                value = self._get(key)
                return condition(value)

        if condition is None:
            condition = lambda value: True
        # First check if condition is already satisfied, in which case we
        # don't need to create a pubsub connection.
        if check():
            return
        p = self._r.pubsub()
        p.subscribe('update/' + key)
        with contextlib.closing(p):
            start = time.time()
            while True:
                # redis-py automatically reconnects to the server if the connection
                # goes down, but we might miss messages in that case. So rather
                # than waiting an arbitrarily long time, we make sure to poll from
                # time to time
                get_timeout = 1.0
                if timeout is not None:
                    remain = (start + timeout) - time.time()
                    if remain <= 0:
                        raise TimeoutError()
                    get_timeout = min(get_timeout, remain)
                message = p.get_message(timeout=get_timeout)
                if message is None:
                    continue
                if message['type'] == 'subscribe':
                    # An update may have happened between our first check and our
                    # subscription taking effects, so check again
                    if check():
                        return
                elif message['type'] == 'message':
                    value = pickle.loads(message['data'])
                    if condition(value):
                        return

    def _get(self, key, return_pickle=False):
        if self._r.exists(key):
            try:
                str_val = self._r.get(key)
                 # assume simple string type for immutable
                if return_pickle: return str_val
                try:
                    return pickle.loads(str_val)
                except pickle.UnpicklingError:
                    return str_val
            except redis.ResponseError:
                return self._strip(self._r.zrange(key, -1, -1)[0], return_pickle)[0]
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
        include_previous : bool or float, optional
            Default False if st is specified, True if st is unspecified
            Numeric value returns [st, et) as well as the last value prior to the start time (if any),
               within a search window given by include_previous
            'False' returns [st, et)
            'True' returns [st, et) as well as the last value prior to the start time (if any),
               with unlimited search window. The current implementation has a significant performance
               impact (it queries all values from the start).
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

        Note - the above usage is inefficient, it is better to specify backward search window:

        get_range('key_name',et=t1,include_previous=dw)
            returns the most recent record prior to time t1, within the time window [t1-dw,t1)
            or [] if no key value exists in the time window

        get_range('key_name',st=t0,et=t1,include_previous=dw)
            returns list of all records in the range [t0,t1) plus the most recent record prior
            to time t0, if there is such a record in the time window [t0-dw,t0)
        """
        if not self._r.exists(key): raise KeyError

        if self._r.type(key) == b'string': return self._get(key)
         # immutables return value with no timestamp information

        # set up include_previous default values
        if include_previous is None:
            include_previous = True if st is None else False

        orig_st = st
        # if st and et None, and include_previous is True, return most recent value
        if st is None and et is None and isinstance(include_previous, bool):
            if include_previous is False:
                ret_list = []
            elif include_previous is True:
                ret_list = [self._strip(self._r.zrange(key, -1, -1)[0], return_pickle)]
        else:
            # if include_previous is True, search from the beginning
            # if include_previous is numeric, search prior window include_previous for key value
            if include_previous is True:
                st = 0
            elif include_previous is not False:
                if et is not None:
                    st = et - float(include_previous) if st is None else st - float(include_previous)
                else:
                    et = time.time()
                    st = et - float(include_previous) if st is None else st - float(include_previous)
                    orig_st = st

            # if st <= 0, get values from the beginning
            # if st is None or include_previous, get values from the beginning for trimming later
            if st <= 0.0 or st is None:
                packed_st = '-'
                # Negative values (including negative zero) don't sort properly when
                # treated as byte strings
            else:
                packed_st = b'[' + struct.pack('>d', float(st))
            # if et is None, get values to the most recent
            if et is None:
                packed_et = b'+'
            else:
                if et <= 0: et = 0.0
                packed_et = b'(' + struct.pack('>d', float(et))
            # get values from redis, extract into (key_value, time) pairs
            ret_vals = self._r.zrangebylex(key, packed_st, packed_et)
            ret_list = [self._strip(str_val, return_pickle) for str_val in ret_vals]

        if include_previous:
            if orig_st is None:
                try:
                    ret_list = [ret_list[-1]]
                except IndexError:
                    # there was no key value in the window
                    ret_list = []
            else:
                # Find the last value prior to the start time
                start_idx = 0
                for idx, value in enumerate(ret_list):
                    if value[1] < orig_st:
                        start_idx = idx
                ret_list = ret_list[start_idx:]

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
