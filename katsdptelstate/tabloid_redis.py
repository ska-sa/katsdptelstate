import os
import bisect
import logging
from redis import ResponseError

try:
    from rdbtools import RdbParser, RdbCallback
    from rdbtools.encodehelpers import bytes_to_unicode
except ImportError:
    pass

_WRONGTYPE_MSG = "WRONGTYPE Operation against a key holding the wrong kind of value"

logging.basicConfig()

class TabloidRedis(object):
    """A Redis like class that provides a very superficial
    simulcrum of a real Redis server. Designed specifically to 
    support the read cases in use by katsdptelstate.
    
    Simple key/val and zsets are supported.
    """
    def __init__(self, filename):
        self.filename = filename
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.data = {}
         # dict of redis keys with approximate data structures within
        self.update()

    def update(self):
        self.data = {}
        try:
            callback = TStateCallback(self)
            self._parser = RdbParser(callback)
            self.logger.info("Loading data from RDB dump of {} bytes".format(os.path.getsize(self.filename)))
            self._parser.parse(self.filename)
            self.logger.info("TabloidRedis updated with {} keys".format(len(self.data)))
        except NameError:
            self.logger.error("Unable to import rdbtools. Instance will be initialised with an empty data structure...")

    def exists(self, key):
        return self.data.has_key(key)

    def pubsub(self, ignore_subscribe_messages=False):
        return None

    def publish(self, channel, data):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError

    def set(self, key, val):
        raise NotImplementedError

    def zadd(self, key, score, val):
        raise NotImplementedError

    def get(self, key):
        if self.type(key) == 'string': return self.data[key]
        raise ResponseError(_WRONGTYPE_MSG)

    def type(self, key):
        if type(self.data[key]) == list: return 'zset'
        return 'string'

    def keys(self, filter=None):
        if filter: self.logger.warning("Filtering not supported. Returning all keys.")
        return self.data.keys()

    def zcard(self, key):
        if self.type(key) == 'zset':
            return len(self.data[key])
        return 1

    def zrange(self, key, start, end, desc=False, withscores=False):
        """Retrieve range of contiguous data specified directly by the supplied
        start and end indices. Fully inclusive range.
        desc and withscores can be supplied but are ignored.
        """
        if end == -1: end = None
        else: end += 1
        return self.data[key][start:end]

    def zrevrangebylex(self, key, packed_et="+", packed_st="-", offset=None, num=None):
        """Reversed version of standard zrangebylex. Principally used to retrieve the 
        last known value working backwards from the specified et."""
        _vals = self.zrangebylex(key, packed_st, packed_et)
        _vals.reverse()
        if (offset is not None and num is not None): return _vals[offset:offset+num]
        return _vals

    def zrangebylex(self, key, packed_st="-", packed_et="+"):
        """Allow retrieval of contiguous time ranges of data
        specified by a packed start and end time.

        Note: This is not indented to be a fully generic replacement for the Redis
        command and as such we explicitly mention time range retrieval.

        Parameters
        ----------
        key : string
            Key from which to extract range
        packed_st : Either '-' to start from the earliest possible time or
                    (|[ (open or closed start) followed by a packed string containing a float start time in epoch seconds
        packed_et : Either '+' to include the latest possible time or
                    (|[ (open or closed start) followed by a packed string containing a float end time in epoch seconds

        Returns
        -------
        list of records between specified time range
        """
        vals = self.data[key]
        ts_keys = [x[:8] for x in vals]
         # this seems to be the most efficient, rather than keeping another copy of the keys around
         # allows use of bisect for range retrieval
        st_index = 0
        et_index = -1
         # assume complete retrieval
        if not packed_st.startswith("-"):
            st_index = bisect.bisect_left(ts_keys, packed_st[1:])
            if packed_st.startswith("("): st_index += 1
        if not packed_et.startswith("+"):
            et_index = bisect.bisect_right(ts_keys, packed_et[1:], lo=st_index) - 1
            if packed_et.startswith("("): et_index -= 1
        return vals[st_index:et_index]

class TStateCallback(RdbCallback):
    def __init__(self, tr):
        self._set = {}
        self._zadd = {}
        self.tr = tr
        super(TStateCallback, self).__init__(string_escape=None)

    def end_rdb(self):
        for k,v in self._zadd.iteritems():
            self.tr.data[k] = sorted(v)
        self.tr.data.update(self._set)

    def encode_key(self, key):
        return bytes_to_unicode(key, self._escape, skip_printable=True)

    def encode_value(self, val):
        return bytes_to_unicode(val, self._escape)

    def set(self, key, value, expiry, info):
        #self._set[self.encode_key(key)] = self.encode_value(value)
        self._set[key] = value

    def zadd(self, key, score, member):
        if not self._zadd.has_key(key): self._zadd[key] = []
        self._zadd[key].append(member)

if __name__ == '__main__':
    tr = TabloidRedis('./dump.rdb')
