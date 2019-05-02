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

from __future__ import print_function, division, absolute_import
from six import raise_from

import logging
import os
from contextlib import contextmanager

from .rdb_utility import encode_len
from .telescope_state import _ensure_binary, _display_str

# Basic RDB header. First 5 bytes are the standard REDIS magic
# Next 4 bytes store the RDB format version number (6 in this case)
# 0xFE flags the start of the DB selector (which is set to 0)
RDB_HEADER = b'REDIS0006\xfe\x00'

RDB_TERMINATOR = b'\xFF'

# Fast CRC-64 implementations on Python seem rare
# RDB is happy with zero checksum - we will protect the RDB dump in other ways
RDB_CHECKSUM = b'\x00\x00\x00\x00\x00\x00\x00\x00'

logger = logging.getLogger(__name__)


def encode_item(key, dumped_value):
    """Encode key and corresponding DUMPed value to RDB format.

    The first byte indicates the encoding used for the value of this key. This
    is essentially just the Redis type. Subsequent bytes represent the key name,
    which consists of a length encoding followed by the actual byte
    representation of the string. For this simple writer we only provide two
    length encoding schemas:

    - String length up to 63: a single byte - the 6 LSBs encode number directly
    - String length 64 - 16383: two bytes, using 6 LSBs of byte 1, all of byte 2

    Thereafter follows the value, encoded according to its appropriate schema.
    As a shortcut, the Redis DUMP command is used to generate the encoded value.

    Note: Redis provides a mechanism for optional key expiry, which we ignore here.
    """
    try:
        key_len = encode_len(len(key))
    except ValueError as exc:
        err = ValueError('Failed to encode key length: {}'.format(exc))
        raise_from(err, None)
    # The DUMPed value includes a leading type descriptor,
    # the encoded value itself (including length specifier),
    # a trailing version specifier (2 bytes) and finally an 8 byte checksum.
    # The version specified and checksum are discarded.
    key_type = dumped_value[:1]
    encoded_value = dumped_value[1:-10]
    return key_type + key_len + key + encoded_value


@contextmanager
def _rdb_file(filename):
    """Create new RDB file and ensure that header and trailer are in place."""
    with open(filename, 'wb') as f:
        f.write(RDB_HEADER)
        try:
            yield f
        finally:
            f.write(RDB_TERMINATOR + RDB_CHECKSUM)


class RDBWriter(object):
    """RDB file resource that stores keys from one (or more) Redis DBs.

    Parameters
    ----------
    filename : str
        Destination filename. Will be opened in 'wb'.

    Attributes
    ----------
    keys_written : int
        Number of keys written to the file.
    keys_failed : int
        Number of keys that failed to be written.
    """
    def __init__(self, filename):
        self.filename = filename
        self.keys_written = 0
        self.keys_failed = 0
        self._rdb_context = _rdb_file(self.filename)
        self._fileobj = None

    def __enter__(self):
        """Enter RDB writer context, opening RDB file and writing header."""
        self._fileobj = self._rdb_context.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit RDB writer context, closing off file and deleting it if empty."""
        try:
            self._rdb_context.__exit__(exc_type, exc_value, traceback)
        finally:
            if not self.keys_written:
                logger.error("No valid keys found - removing empty file")
                os.remove(self.filename)
        return False

    def save(self, client, keys=None):
        """Save a specified subset of keys from a Redis DB to the RDB dump file.

        This is a very limited RDB dump utility. Either a compatible redis client
        or a :class:`katsdptelstate.Backend` must be provided. Missing or broken
        keys are skipped and logged without raising an exception.

        Parameters
        ----------
        client : :class:`~redis.StrictRedis` or :class:`~katsdptelstate.telescope_state.Backend`
            A Redis-compatible client instance. Must support keys() and dump().
        keys : sequence of str or bytes, optional
            The keys to extract from Redis and include in the dump.
            Keys that don't exist will not raise an Exception, only a log message.
            None (default) includes all keys.
        """
        if keys is None:
            logger.info("No keys specified - dumping entire database")
            keys = client.keys(b'*')
        for key in keys:
            key = _ensure_binary(key)
            dumped_value = client.dump(key)
            try:
                if not dumped_value:
                    raise KeyError('Key not found in Redis')
                encoded_str = encode_item(key, dumped_value)
            except (ValueError, KeyError) as e:
                self.keys_failed += 1
                logger.error("Failed to save key %s: %s", _display_str(key), e)
                continue
            self._fileobj.write(encoded_str)
            self.keys_written += 1


if __name__ == '__main__':
    import argparse
    import redis

    from .endpoint import endpoint_parser

    # Default Redis port for use in endpoint constructors
    DEFAULT_PORT = 6379

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
                        help='host[:port] of the Redis instance to connect to. '
                             '[default=%(default)s]'.format(DEFAULT_PORT))
    args = parser.parse_args()

    endpoint = args.redis
    logger.info("Connecting to Redis instance at %s", endpoint)
    client = redis.StrictRedis(host=endpoint.host, port=endpoint.port)
    logger.info("Saving keys to RDB file %s", args.outfile)
    keys = args.keys
    if keys is not None:
        keys = keys.split(",")
    with RDBWriter(args.outfile) as rdb_writer:
        rdb_writer.save(client, keys)
    if rdb_writer.keys_failed > 0:
        logger.warning("Done - Warning %d keys failed to be written (%d succeeded)",
                       rdb_writer.keys_failed, rdb_writer.keys_written)
    else:
        logger.info("Done - %d keys written", rdb_writer.keys_written)
