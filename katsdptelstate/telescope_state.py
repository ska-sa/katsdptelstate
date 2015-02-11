import redis
import struct
import time
import cPickle
import logging
import argparse

logger = logging.getLogger(__name__)

class InvalidKeyError(Exception):
    pass

class ImmutableKeyError(Exception):
    pass

class TelescopeState(object):
    def __init__(self, host='localhost', db=0):
        self._r = redis.StrictRedis(db=db)
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

    def override_local_defaults(self, parser, config_key='config'):
        """Override local defaults with remote config options.

        Look for a config dict in the Telescope State Repository and
        use any configuration values in this to override the default
        values for options before they are parsed.

        So precedence is defaults -> telescope state -> command line.
        This method must be called before parser.parse_args is called.

        Parameters
        ----------
        parser : :class: `OptionParser` object
            The parser for which to override defaults. 
        config_key : string
            Configuration key to use to retrieve config dict from Telescope State Repository.

        """
        if not self.has_key(config_key):
            logger.warning("Requested merge to non-existant config key {}".format(config_key))
        else:
            for (k,v) in parser.defaults.iteritems():
                if config_dict.has_key(k): 
                    parser.defaults[k] = config_dict[k]
                    logger.debug("Local default {}={} overriden by TelescopeState config to value {}".format(k,v,config_dict[k]))
        return parser

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
    
    def get(self, key):
        val = self._get(key)
        if val is None: raise KeyError
        return val
    
    def get_range(self, key, st=None, et=None):
        """Get the value specified by the key from the model."""
        if not self._r.exists(key): raise KeyError
        if st is None and et is None:
            return self._strip(self._r.zrange(key,-1,-1)[0])
        elif st == 0 or et == 0:
            return [self._strip(str_val) for str_val in self._r.zrange(key,0,-1)]
        else:
            packed_st = struct.pack('>d',float(st))
            packed_et = struct.pack('>d',float(et))
            ret_vals = self._r.zrangebylex(key,"[{}".format(packed_st),"[{}".format(packed_et))
            return [self._strip(str_val) for str_val in ret_vals]

class ArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        self.config_key = kwargs.pop('config_key', 'config')
        super(ArgumentParser, self).__init__(*args, **kwargs)
        self.add_argument('--telstate', type=TelescopeState, help='Telescope state repository from which to retrieve config', metavar='HOST')
        self.add_argument('--name', type=str, default='', help='Name of this process for telescope state configuration')
        self.config_keys = set()

    def add_argument(self, *args, **kwargs):
        action = super(ArgumentParser, self).add_argument(*args, **kwargs)
        # Check if we have finished initialising ourself yet
        if hasattr(self, 'config_keys'):
            if action.dest is not None and action.dest is not argparse.SUPPRESS:
                self.config_keys.add(action.dest)
        return action

    def _load_defaults(self, namespace, telstate, name):
        config_dict = telstate.get(self.config_key)
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
        for cur in reversed(dicts):
            for key in self.config_keys:
                if key in cur and not hasattr(namespace, key):
                    setattr(namespace, key, cur[key])

    def parse_known_args(self, args=None, namespace=None):
        if namespace is None:
            namespace = argparse.Namespace()
        config_parser = argparse.ArgumentParser(add_help=False)
        config_parser.add_argument('--telstate', type=TelescopeState)
        config_parser.add_argument('--name', type=str, default='')
        try:
            config_args, other = config_parser.parse_known_args(args)
        except argparse.ArgumentError:
            other = args
        else:
            if config_args.telstate is not None:
                namespace.telstate = config_args.telstate
                namespace.name = config_args.name
                self._load_defaults(namespace, config_args.telstate, config_args.name)
        return super(ArgumentParser, self).parse_known_args(other, namespace)
