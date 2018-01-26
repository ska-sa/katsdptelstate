import os
import bisect
import logging
import struct

from redis import ResponseError
from fakenewsredis import FakeStrictRedis
from .rdb_utility import encode_len, encode_prev_length

try:
    from rdbtools import RdbParser, RdbCallback
    from rdbtools.encodehelpers import bytes_to_unicode
except ImportError:
    class RdbCallback(object):
        """A simple stub for this class so that we can use TabloidRedis
        as a backing store for Telstate without having rdbtools installed.
        This stub is needed, rather than a simple `RdbCallback = object` since it is
        subclassed by TStateCallback which has a named parameter."""
        def __init__(self, *args, **kwargs):
            pass

_WRONGTYPE_MSG = "WRONGTYPE Operation against a key holding the wrong kind of value"

DUMP_POSTFIX = b"\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00"

logging.basicConfig()

class TabloidRedis(FakeStrictRedis):
    """A Redis-like class that provides a very superficial
    simulcrum of a real Redis server. Designed specifically to 
    support the read cases in use by katsdptelstate.

    The Redis-like functionality is almost entirely derived from FakeStrictRedis,
    we only add a dump function.
    """
    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        super(TabloidRedis, self).__init__(**kwargs)

    def load_from_file(self, filename):
        """Load keys from the specified RDB compatible dump file.
        """
        try:
            callback = TStateCallback(self)
            self._parser = RdbParser(callback)
            self.logger.debug("Loading data from RDB dump of {} bytes".format(os.path.getsize(filename)))
            self._parser.parse(filename)
        except NameError:
            raise ImportError("Unable to load RDB parser. Please check that rdbtools is installed.")
        return len(self.keys())

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

                (length encoding)(ziplist string envelope)

           The ziplist string envelope itself is an LZF compressed string with the following form:
           (format descriptions are provided in the description for each component)

                (bytes in list '<i')(offset to tail '<i')(number of entries '<h')(entry 1)(entry 2)(entry 2N)(terminator 0xFF)

           The entries themselves are encoded as follows:

                (previous entry length 1byte or 5bytes)(entry length - up to 5 bytes as per length encoding)(value)

           The entries alternate between zset values and scores, so there should always be 2N entries in the decode.

           Returns None is key not found
        """

        type_specifier = b'\x00'
        key_type = self.type(key)
        if key_type == b'none': return None
        if key_type == b'zset':
            type_specifier = b'\x0c'
            data = self.zrange(key, 0, -1, withscores=True)
            entry_count = 2 * len(data)

            # The entries counter for ziplists is encoded as 2 bytes, if we exceed this limit
            # we fall back to making a simple set. Redis of course makes the decision point using
            # only 7 out of the 16 bits available and switches at 127 entries...
            if entry_count > 127:
                enc = [b'\x03' + encode_len(len(data))]
                 # for inscrutable reasons the length here is half the number of actual entries (i.e. scores are ignored)
                for entry in data:
                    enc.append(encode_len(len(entry[0])) + entry[0] + '\x010')
                     # interleave entries and scores directly
                     # for now, scores are kept fixed at integer zero since float 
                     # encoding is breaking for some reason and is not needed for telstate
                enc.append(DUMP_POSTFIX)
                return b"".join(enc)

            raw_entries = []
            previous_length = b'\x00'
            # loop through each entry in the data interleaving encoded values and scores (all set to zero)
            for entry in data:
                enc = previous_length + encode_len(len(entry[0])) + entry[0]
                raw_entries.append(enc)
                previous_length = encode_prev_length(len(enc))
                raw_entries.append(previous_length + b'\xf1')
                 # scores are encoded using a special length schema which supports direct integer addressing
                 # 4 MSB set implies direct unsigned integer in 4 LSB (minus 1). Hence \xf1 is integer 0
                previous_length = b'\x02'
            encoded_entries = b"".join(raw_entries) + b'\xff'
            zl_length = 10 + len(encoded_entries)
             # account for the known 10 bytes worth of length descriptors when calculating envelope length
            zl_envelope = struct.pack('<i', zl_length) + struct.pack('<i', zl_length - 3) + struct.pack('<h', entry_count) + encoded_entries
            return b'\x0c' + encode_len(len(zl_envelope)) + zl_envelope + DUMP_POSTFIX
        else:
            type_specifier = b'\x00'
            val = self.get(key)
            encoded_length = encode_len(len(val))
            return type_specifier + encoded_length + val + DUMP_POSTFIX
        raise NotImplementedError("Unsupported key type {}. Must be either string or zset".format(key_type))

class TStateCallback(RdbCallback):
    def __init__(self, tr):
        self.tr = tr
        self._zset = {}
        super(TStateCallback, self).__init__(string_escape=None)

    def set(self, key, value, expiry, info):
        self.tr.set(key, value, expiry)

    def start_sorted_set(self, key, length, expiry, info):
        self._zset = []

    def zadd(self, key, score, member):
        self._zset.append(score)
        self._zset.append(member)

    def end_sorted_set(self, key):
        self.tr.zadd(key, *self._zset)
        self._zset = []
