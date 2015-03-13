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
    
    def get_range(self, key, st=None, et=None, return_format=None):
        """Get the value specified by the key from the model.
        
        Parameters
        ----------
        key : string 
            database key to extract
        st : float, default None
            start time
        et: float, default None
            end time
        return_format : string, default None
            'recarray' returns values and times as numpy recarray with keys 'value' and 'time'
            None returns values and times as 2D list of elements format (key_value, time)
        
        Returns
        -------
        array of key_value, time database elements between specified time range
        
        Notes
        -----
        For the cases of:
            * st and et arguments, both non-zero : returns (key_value, time) range between times st and et
            * No st argument, no et argument : returns most recent (key_value, time)
            * st or et arguments zero : returns all (key_value, time) in database
            * Only st or et, second argument non-zero : returns error
        """
        if not self._r.exists(key): raise KeyError
        if st is None and et is None:
            ret_list = self._strip(self._r.zrange(key,-1,-1)[0])
        elif st == 0 or et == 0:
            ret_list = [self._strip(str_val) for str_val in self._r.zrange(key,0,-1)]
        else:
            packed_st = struct.pack('>d',float(st))
            packed_et = struct.pack('>d',float(et))
            ret_vals = self._r.zrangebylex(key,"[{}".format(packed_st),"[{}".format(packed_et))
            ret_list = [self._strip(str_val) for str_val in ret_vals]

        if return_format is not 'recarray':
            return ret_list
        else:
            val_shape = np.array(np.atleast_2d(ret_list)[0][0]).shape
            val_type = np.array(np.atleast_2d(ret_list)[0][0]).dtype
            return np.array(ret_list, dtype=[('value', val_type, val_shape), ('time', np.float)])
            
    def get_previous(self, key, t, dt=2.0):
        """
        Get the most recent value of a Telescope Stake key prior to a given time,
        within a specified interval.

        Parameters
        ----------
        key : string 
            key to be extracted from the ts
        t : float
            time that key value is desired, default 2.0 seconds 
        dt : float 
            search time interval [t-dt, t] for key value

        Returns
        -------
        (key value, time) for time in the Telescope State closest to t, 
        or None if key value is not present in the time range

        """  
        key_list = self.get_range(key,st=t-dt,et=t)
        
        if key_list == []:
            # key_list empty because key value not present in the time range, return None
            return None
        else:
            time_diffs = [t - np.float(line[1]) for line in key_list]
            return key_list[np.argmin(time_diffs)]


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
