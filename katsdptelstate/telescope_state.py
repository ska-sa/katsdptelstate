import redis
import struct
import time
import cPickle
import logging
import numpy as np

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
