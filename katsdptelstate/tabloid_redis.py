import os
import bisect
import logging
import struct
from redis import ResponseError

from rdbtools import RdbParser, RdbCallback
from rdbtools.encodehelpers import bytes_to_unicode

_WRONGTYPE_MSG = "WRONGTYPE Operation against a key holding the wrong kind of value"

DUMP_POSTFIX = "\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00"

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
         # dict of redis keys with appropriate data structures within
        if os.path.exists(self.filename):
            self.update()
        else:
            self.logger.warning("Initialised as empty since specified file does not exist.")

    def update(self):
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
        self.data.pop(key)

    def set(self, key, value, expiry=None, info=None):
        if expiry: logger.warning("Key expiry not supported in TabloidRedis")
        if info: logger.warning("Key information not supported in TabloidRedis")
        self.data[key] = value

    def zadd(self, key, score, val):
        if score != 0:
            raise NotImplementedError('TabloidRedis does not support non zero scores for zset')
        if not self.data.has_key(key): self.data[key] = []
        self.data[key].append(val)

    def get(self, key):
        if self.type(key) == 'string': return self.data[key]
        raise ResponseError(_WRONGTYPE_MSG)

    def encode_len(self, length):
        """Encodes the specified length as 1,2 or 5 bytes of
           RDB specific length encoded byte.
           For values less than 64 (i.e two MSBs zero - encode directly in the byte)
           For values less than 16384 use two bytes, leading MSBs are 01 followed by 14 bits encoding the value
           For values less than (2^32 -1) use 5 bytes, leading MSBs are 10. Length encoded only in the lowest 32 bits.
        """
        if length > (2**32 -1): raise ValueError("Cannot encode item of length greater than 2^32 -1")
        if length < 64: return chr(length)
        if length < 16384: return struct.pack(">h",0x4000 + length)
        return struct.pack('>q',0x8000000000 + length)[3:]

    def encode_prev_length(self, length):
        """Special helper for zset previous entry lengths.
           If length < 253 then use 1 byte directly, otherwise
           set first byte to 254 and add 4 trailing bytes as an
           unsigned integer.
        """
        if length < 254: return chr(length)
        return b'\xfe' + struct.pack(">q",length)

    def dump(self, key):
        """Encode redis key value in an RDB compatible format.
           Note: This follows the DUMP command in Redis itself which produces output
           that is similarly encoded to an RDB, but not exactly the same.

           String types are encoded simply with a length specified (as documented in encode_len) followed directly by the
           value bytestring.

           Zset types are more complex and Redis uses both LZF and Ziplist format depending on various arcane heuristics.
           For maximum compatibility we use LZF and Ziplist as this supports really large values well, at the expense of
           additional overhead for small values.

           A zset dump thus takes the following form:

                <length encoding><ziplist string envelope>

           The ziplist string envelope itself is an LZF compressed string with the following form:
           (format descriptions are provided in the description for each component)

                <bytes in list '<i'><offset to tail '<i'><number of entries '<h'><entry 1><entry 2><entry 2N><terminator 0xFF>

           The entries themselves are encoded as follows:

                <previous entry length 1byte or 5bytes><entry length - up to 5 bytes as per length encoding><value>

           The entries alternate between zset values and scores, so there should always be 2N entries in the decode.
        """

        type_specifier = b'\x00'
        if self.type(key) == 'zset':
            type_specifier = b'\x0c'
            data = self.data[key]
            entry_count = 2 * len(data)

            # The entries counter for ziplists is encoded as 2 bytes, if we exceed this limit
            # we fall back to making a simple set. Redis of course makes the decision point using
            # only 7 out of the 16 bits available and switches at 127 entries...
            if entry_count > 127:
                _enc = b'\x03' + self.encode_len(len(data))
                 # for inscrutable reasons the length here is half the number of actual entries (i.e. scores are ignored)
                for entry in data:
                    _enc += self.encode_len(len(entry)) + entry + '\x010'
                     # interleave entries and scores directly
                return _enc + DUMP_POSTFIX

            raw_entries = []
            previous_length = b'\x00'
            # loop through each entry in the data interleaving encoded values and scores (all set to zero)
            for entry in data:
                _enc = previous_length + self.encode_len(len(entry)) + entry
                raw_entries.append(_enc)
                previous_length = self.encode_prev_length(len(_enc))
                raw_entries.append(previous_length + b'\xf1')
                 # scores are encoded using a special length schema which supports direct integer addressing
                 # 4 MSB set implies direct unsigned integer in 4 LSB (minus 1). Hence \xf1 is integer 0
                previous_length = b'\x02'
            encoded_entries = "".join(raw_entries) + b'\xff'
            zl_length = 10 + len(encoded_entries)
             # account for the known 10 bytes worth of length descriptors when calculating envelope length
            print "{}: {}/{}".format(key, entry_count, zl_length)
            zl_envelope = struct.pack('<i', zl_length) + struct.pack('<i', zl_length - 3) + struct.pack('<h', entry_count) + encoded_entries
            return b'\x0c' + self.encode_len(len(zl_envelope)) + zl_envelope + DUMP_POSTFIX
        else:
            type_specifier = b'\x00'
            val = self.get(key)
            encoded_length = self.encode_len(len(val))
            return type_specifier + encoded_length + val.encode('UTF-8') + DUMP_POSTFIX

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
