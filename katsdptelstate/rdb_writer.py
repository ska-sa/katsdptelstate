import redis
import struct
import logging
import os

from katsdptelstate.endpoint import endpoint_parser

RDB_HEADER = b'REDIS0006\xfe\x00'
 # Basic RDB header. First 5 bytes are the standard REDIS magic
 # Next 4 bytes store the RDB format version number (6 in this case)
 # 0xFE flags the start of the DB selector (which is set to 0)

RDB_TERMINATOR = b'\xFF'

RDB_CHECKSUM = b'\x00\x00\x00\x00\x00\x00\x00\x00'
 # Fast CRC-64 implementations on Python seem rare
 # RDB is happy with zero checksum - we will protect the RDB
 # dump in other ways

class RDBWriter(object):
    """Very limited RDB dump utility used to dump a specified subset
    of keys from an active Redis DB to a valid RDB format file.

    Either an endpoint or a compatible client must be provided.

    Parameters
    ----------
    endpoint : str or :class:`~katsdptelstate.endpoint.Endpoint`
        The address of the redis server (if a string, it is passed to the
        :class:`~katsdptelstate.endpoint.Endpoint` constructor). 
    client : :class:`~katsdptelstate.tabloid_redis.TabloidRedis` or
             :class:`~redis.StrictRedis`
        A Redis compatible client instance. Must support keys() and dump()
    """
    def __init__(self, endpoint=None, client=None):
        if not endpoint and not client:
            raise NameError("You must specify either an endpoint or a valid client.")
        if client:
            self._r = client
        else:
            if type(endpoint) == str:
                endpoint = endpoint_parser(6379)(endpoint)
            self._r = redis.StrictRedis(host=endpoint.host, port=endpoint.port)
        self.logger = logging.getLogger(__name__)

    def save(self, filename, keys=[]):
        """Encodes specified keys from the RDB file into binary
        string representation and writes these to a file.

        Parameters
        ----------
        filename : str
            Destination filename. Will be opened in 'wb'.
        keys : list
            A list of the keys to extract from Redis and include in the dump.
            Keys that don't exist will not raise an Exception, only a log message.
            Empty list default includes all keys.

        Returns
        -------
        keys_written : integer
            Number of keys written to the file
        """
        if not keys:
            self.logger.warning("No keys specified - dumping entire database")
            keys = self._r.keys()

        with open(filename, 'wb') as f:
            f.write(RDB_HEADER)
            keys_written = 0
            for key in keys:
                try:
                    enc_str = self.encode_item(key)
                except (ValueError, KeyError) as e:
                    self.logger.error("Failed to save key: {}".format(e))
                    continue
                f.write(enc_str)
                keys_written += 1
            f.write(RDB_TERMINATOR + RDB_CHECKSUM)
        if not keys_written:
            self.logger.error("No valid keys found - removing empty file")
            os.remove(filename)
        return keys_written

    def encode_len(self, length):
        """Encodes the specified length as 1,2 or 5 bytes of
           RDB specific length encoded byte.
           For values less than 64 (i.e two MSBs zero - encode directly in the byte)
           For values less than 16384 use two bytes, leading MSBs are 01 followed by 14 bits encoding the value
           For values less than (2^32 -1) use 5 bytes, leading MSBs are 10. Length encoded only in the lowest 32 bits.
        """
        if length > (2**32 -1): raise ValueError("Cannot encode item of length greater than 2^32 -1")
        if length < 64: return struct.pack('B', length)
        if length < 16384: return struct.pack(">h",0x4000 + length)
        return struct.pack('>q',0x8000000000 + length)[3:]


    def encode_item(self, key):
        """Returns a binary string containing an RDB encoded key and value.

        First byte is used to indicate the encoding used for the value of this key.
        This is essentially just the Redis type.

        Subsequent bytes represent the name of the key, which consists of a length
        encoding followed by the actual byte representation of the string. For this simple
        writer we only provide two length encoding schemas:
             String length up to 63 - single byte, 6 LSB encode number directly
             String length from 64 to 16383 - two bytes, 6 LSB of byte 1 + 8 from byte 2 encode number

        Thereafter the value, encoding according to its appropriate schema is appended. As a shortcut,
        the Redis DUMP command is used to generate the encoded value string.

        Note: Redis provides a mechanism for optional key expiry, which we ignore here.
        """
        #if not isinstance(key, bytes): key = key.encode('utf-8')
        try:
            key_len = self.encode_len(len(key))
        except ValueError as e:
            raise ValueError('Failed to encode key length: {}'.format(e))
        key_dump = self._r.dump(key)
        if not key_dump: raise KeyError('Key {} not found in Redis'.format(key))
        key_type = key_dump[:1]
        encoded_val = key_dump[1:-10]
         # The DUMPed value includes a leading type descriptor, the encoded value itself (including length specifier),
         # a trailing version specifier (2 bytes) and finally an 8 byte checksum.
         # The version specified and checksum are discarded.
        return key_type + key_len + key.encode('utf-8') + encoded_val

if __name__ == '__main__':
    import argparse    

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--outfile', default='simple_out.rdb', metavar='FILENAME',
                        help='Output RDB filename [default=%(default)s]')
    parser.add_argument('--keys', default=None, metavar='FILENAME',
                        help='Comma seperated list of Redis keys to write to RDB. [default=all]')
    parser.add_argument('--redis', type=endpoint_parser(6379), default=endpoint_parser(6379)('localhost'),
                        help='host[:port] of the Redis instance to connect to. [default=localhost:6379]')
    args = parser.parse_args()
   
    logger.info("Connecting to Redis instance at {}".format(args.redis)) 
    rbd_writer = RDBWriter(args.redis)
    logger.info("Saving keys to RDB file {}".format(args.outfile))
    if args.keys: keys = args.keys.split(",")
    else: keys = []
    keys_written = rbd_writer.save(args.outfile, keys)
    if keys and keys_written < len(keys):
        logger.warning("Done - Warning only {} of {} requested keys writtent".format(keys_written, len(keys)))
    else:
        logger.info("Done - {} keys written".format(keys_written))

