################################################################################
# Copyright (c) 2018-2019, National Research Foundation (Square Kilometre Array)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import logging
import os

import redis

from .endpoint import endpoint_parser
from .rdb_utility import encode_len


RDB_HEADER = b'REDIS0006\xfe\x00'
 # Basic RDB header. First 5 bytes are the standard REDIS magic
 # Next 4 bytes store the RDB format version number (6 in this case)
 # 0xFE flags the start of the DB selector (which is set to 0)

RDB_TERMINATOR = b'\xFF'

RDB_CHECKSUM = b'\x00\x00\x00\x00\x00\x00\x00\x00'
 # Fast CRC-64 implementations on Python seem rare
 # RDB is happy with zero checksum - we will protect the RDB
 # dump in other ways

DEFAULT_PORT = 6379
 # Default Redis port for use in endpoint constructors


class RDBWriter(object):
    """Very limited RDB dump utility used to dump a specified subset
    of keys from an active Redis DB to a valid RDB format file.

    Either an endpoint or a compatible client must be provided.

    Parameters
    ----------
    endpoint : str or :class:`~katsdptelstate.endpoint.Endpoint`
        The address of the Redis server (if a string, it is passed to the
        :class:`~katsdptelstate.endpoint.Endpoint` constructor).
    client : :class:`~katsdptelstate.tabloid_redis.TabloidRedis` or :class:`~redis.StrictRedis`
        A Redis compatible client instance. Must support keys() and dump()
    """
    def __init__(self, endpoint=None, client=None):
        if not endpoint and not client:
            raise ValueError("You must specify either an endpoint or a valid client.")
        if client:
            self._r = client
        else:
            if isinstance(endpoint, str):
                endpoint = endpoint_parser(DEFAULT_PORT)(endpoint)
            self._r = redis.StrictRedis(host=endpoint.host, port=endpoint.port)
        self.logger = logging.getLogger(__name__)

    def save(self, filename, keys=None, supplemental_dumps=[]):
        """Encodes specified keys from the RDB file into binary
        string representation and writes these to a file.

        Parameters
        ----------
        filename : str
            Destination filename. Will be opened in 'wb'.
        keys : list
            A list of the keys to extract from Redis and include in the dump.
            Keys that don't exist will not raise an Exception, only a log message.
            None (default) includes all keys.
        supplemental_dumps : list
            A list of encoded supplemental key/values in string form to be included
            in the RDB dump.

        Returns
        -------
        keys_written : int
            Number of keys written to the file.
        keys_failed : int
            Number of keys that failed to be written.
        """
        if keys is None:
            self.logger.warning("No keys specified - dumping entire database")
            keys = self._r.keys()

        with open(filename, 'wb') as f:
            f.write(RDB_HEADER)
            keys_written = 0
            keys_failed = 0
            for key in keys:
                try:
                    enc_str = self.encode_item(key)
                except (ValueError, KeyError) as e:
                    keys_failed += 1
                    self.logger.error("Failed to save key: {}".format(e))
                    continue
                f.write(enc_str)
                keys_written += 1
            for dump in supplemental_dumps:
                f.write(dump)
            f.write(RDB_TERMINATOR + RDB_CHECKSUM)
        if not keys_written:
            self.logger.error("No valid keys found - removing empty file")
            os.remove(filename)
        return (keys_written, keys_failed)

    def encode_supplemental_keys(self, client, keys):
        """Helper class to encode a specific set of keys on a non-prime Redis like client.
        """
        return [self._encode_item(key, client.dump(key)) for key in keys]

    def encode_item(self, key):
        return self._encode_item(key, self._r.dump(key))

    def _encode_item(self, key, key_dump):
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
        if not key_dump:
            raise KeyError('Key {} not found in Redis'.format(key))
        if not isinstance(key, bytes):
            key = key.encode('utf-8')
        try:
            key_len = encode_len(len(key))
        except ValueError as e:
            raise ValueError('Failed to encode key length: {}'.format(e))
        key_type = key_dump[:1]
        encoded_val = key_dump[1:-10]
         # The DUMPed value includes a leading type descriptor, the encoded value itself (including length specifier),
         # a trailing version specifier (2 bytes) and finally an 8 byte checksum.
         # The version specified and checksum are discarded.
        return key_type + key_len + key + encoded_val


if __name__ == '__main__':
    import argparse

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--outfile', default='simple_out.rdb', metavar='FILENAME',
                        help='Output RDB filename [default=%(default)s]')
    parser.add_argument('--keys', default=None, metavar='KEYS',
                        help='Comma-separated list of Redis keys to write to RDB. [default=all]')
    parser.add_argument('--redis', type=endpoint_parser(DEFAULT_PORT),
                        default=endpoint_parser(DEFAULT_PORT)('localhost'),
                        help='host[:port] of the Redis instance to connect to. [default=localhost:{}]'
                             .format(DEFAULT_PORT))
    args = parser.parse_args()

    logger.info("Connecting to Redis instance at {}".format(args.redis))
    rbd_writer = RDBWriter(args.redis)
    logger.info("Saving keys to RDB file {}".format(args.outfile))
    keys = args.keys
    if keys is not None:
        keys = args.keys.split(",")
    keys_written, keys_failed = rbd_writer.save(args.outfile, keys)
    if keys_failed > 0:
        logger.warning("Done - Warning {} keys failed to be written ({} succeeded)"
                       .format(keys_failed, keys_written))
    else:
        logger.info("Done - {} keys written".format(keys_written))
