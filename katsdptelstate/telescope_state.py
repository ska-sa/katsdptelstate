import redis
import struct
import time
import cPickle
import logging
import argparse
from .endpoint import Endpoint, endpoint_parser
import numpy as np

logger = logging.getLogger(__name__)

class InvalidKeyError(Exception):
    pass

class ImmutableKeyError(Exception):
    pass

class TelescopeState(object):
    def __init__(self, endpoint='localhost', db=0):
        if not isinstance(endpoint, Endpoint):
            endpoint = endpoint_parser(default_port=None)(endpoint)
        if endpoint.port is not None:
            self._r = redis.StrictRedis(host=endpoint.host, port=endpoint.port, db=db)
        else:
            self._r = redis.StrictRedis(host=endpoint.host, db=db)
        self._ps = self._r.pubsub(ignore_subscribe_messages=True)
        self._default_channel = 'tm_info'
        self._ps.subscribe(self._default_channel)
         # subscribe to the telescope model info channel

    def _strip(self, str_val):
        if len(str_val) < 8: return None
        ts = struct.unpack('>d',str_val[:8])[0]
        try:
            ret_val = cPickle.loads(str_val[8:])
        except cPickle.UnpicklingError:
            ret_val = str_val[8:]
        return (ret_val,ts)

    def __getattr__(self, key):
        val = self._get(key)
        if val is None: raise AttributeError
        return val

    def __getitem__(self,key):
        val = self._get(key)
        if val is None: raise KeyError
        return val

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
        self._r.exists(key_name)

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
                key_list.append((k,kcount))
        else:
            key_list = self._r.keys(filter)
        return key_list

    def delete(self, key):
        """Remove a key, and all values, from the model."""
        return self._r.delete(key)

    def add(self, key, value, ts=None, immutable=False):
        """Add a new key / value pair to the model."""
        if self.__class__.__dict__.has_key(key):
            raise InvalidKeyError("The specified key already exists as a class method and thus cannot be used.")
         # check that we are not going to munge a class method
        if ts is None and not immutable: ts = time.time()
        existing_type = self._r.type(key)
        if existing_type == 'string':
            raise ImmutableKeyError("Attempt to overwrite immutable key.")
        if immutable:
            return self._r.set(key, cPickle.dumps(value))
        else:
            packed_ts = struct.pack('>d',float(ts))
            return self._r.zadd(key, 0, "{}{}".format(packed_ts,cPickle.dumps(value)))

    def _get(self, key):
        if self._r.exists(key):
            try:
                str_val = self._r.get(key)
                 # assume simple string type for immutable
                try:
                    return cPickle.loads(str_val)
                except cPickle.UnpicklingError:
                    return str_val
            except redis.ResponseError:
                return self._strip(self._r.zrange(key,-1,-1)[0])[0]
        return None

    def get(self, key, default=None):
        val = self._get(key)
        if val is None: return default
        return val

    def get_range(self, key, st=None, et=None, dt=None, return_format=None, include_previous=False):
        """Get the value specified by the key from the model.

        Parameters
        ----------
        key : string
            database key to extract
        st : float, optional
            start time, default returns the most recent value prior to et
        et: float, optional
            end time, defaults to the end of time
        dt : float, optional
            limit the search for the key value to time interval [t-dt, t]
        return_format : string, optional
            'recarray' returns values and times as numpy recarray with keys 'value' and 'time'
            `None` returns values and times as 2D list of elements format (key_value, time)
        include_previous : bool, optional
            If `True`, the last value prior to the start time (if any) is
            included. The current implementation has a significant performance
            impact (it queries all values from the start)

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
            returns list of all records between times t0 and t1

        get_range('key_name',st=t0)
            returns array of all records after time t0

        get_range('key_name',st=t0,et=t1,dt=dw)
            returns ValueError as this usage is ambiguous

        get_range('key_name',et=t1)
            returns the most recent record prior to time t1

        Note - the above usage is inefficient, it is better to specify backward search window:

        get_range('key_name',et=t1,dt=tw)
            returns the most recent record prior to time t1, within
            the time window [t1-dw,t1] or [] if no key value exists in the time window
        """
        if not self._r.exists(key): raise KeyError

        # if dt is set, search range [et-dt,et] for key value
        if dt is not None:
            if (st is not None) or (et is None):
                raise ValueError('Ambiguous set of parameters. Window parameter dt only to be used with et.')                    
            else:
                st = et-dt

        # if st and et None, return most recent value
        if st is None and et is None and not include_previous:
            ret_list = [self._strip(self._r.zrange(key,-1,-1)[0])]
        else:
            # if st <= 0, get values from the beginning
            # if st is None or include_previous, get values from the beginning for trimming later
            if include_previous or st <= 0.0 or st is None:
                packed_st = '-'
                # Negative values (including negative zero) don't sort properly when
                # treated as byte strings
            else:
                packed_st = '[' + struct.pack('>d', float(st))
            # if et is None, get values to the most recent
            if et is None:
                packed_et = '+'
            else:
                packed_et = '(' + struct.pack('>d', float(et))
            # get values from redis, extract into (key_value, time) pairs
            ret_vals = self._r.zrangebylex(key, packed_st, packed_et)
            ret_list = [self._strip(str_val) for str_val in ret_vals]

        # extract most recent value from list
        if dt is not None or st is None:
        	try:
        		ret_list = [ret_list[-1]]
        	except IndexError:
        		# there was no key value in the window
        		ret_list = []

        if include_previous:
            # Find the last value prior to the start time
            start_idx = 0
            for idx, value in enumerate(ret_list):
                if value[1] <= st:
                    start_idx = idx
            ret_list = ret_list[start_idx:]

        if return_format is None:
            return ret_list
        elif return_format == 'recarray':
            val_shape = np.array(np.atleast_2d(ret_list)[0][0]).shape
            val_type = np.array(np.atleast_2d(ret_list)[0][0]).dtype
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
    `config`, `config["foo"]` and `config["foo"]["0"]`. If configuration is
    found in multiple places, the most specific location will be used first.
    It is not an error for one of these dictionaries not to exist, but it is
    an error if a name is found but is not a dictionary.

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
        for part in parts:
            cur = cur.get(part)
            if cur is None:
                break
            dicts.append(cur)

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
