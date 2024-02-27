################################################################################
# Copyright (c) 2018-2020, National Research Foundation (Square Kilometre Array)
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
from typing import Iterable, Union, Optional

import redis

from .rdb_writer_base import RDBWriterBase
from .utils import ensure_binary
from .telescope_state import TelescopeState
from .backend import Backend


logger = logging.getLogger(__name__)


class RDBWriter(RDBWriterBase):
    __doc__ = RDBWriterBase.__doc__

    def save(self, client: Union[TelescopeState, Backend, redis.Redis],
             keys: Optional[Iterable[Union[str, bytes]]] = None) -> None:
        """Save a specified subset of keys from a Redis DB to the RDB dump file.

        This is a very limited RDB dump utility. It takes a Redis database
        either from a high-level :class:`~katsdptelstate.TelescopeState` object,
        its low-level backend or a compatible redis client. Missing or broken
        keys are skipped and logged without raising an exception.

        Parameters
        ----------
        client : :class:`~katsdptelstate.telescope_state.TelescopeState` or :class:`~katsdptelstate.telescope_state.Backend` or :class:`~redis.Redis`-like
            A telstate, backend, or Redis-compatible client instance supporting keys() and dump()
        keys : iterable of str or bytes, optional
            The keys to extract from Redis and include in the dump.
            Keys that don't exist will not raise an Exception, only a log message.
            None (default) includes all keys.
        """  # noqa: E501
        if isinstance(client, TelescopeState):
            client = client.backend
        if keys is None:
            logger.info("No keys specified - dumping entire database")
            keys = client.keys(b'*')
        for key in keys:
            key = ensure_binary(key)
            dumped_value = client.dump(key)
            self.write_key(key, dumped_value)


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
                             '[default=%(default)s]')
    args = parser.parse_args()

    endpoint = args.redis
    logger.info("Connecting to Redis instance at %s", endpoint)
    client = redis.Redis(host=endpoint.host, port=endpoint.port)
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
