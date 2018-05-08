from __future__ import print_function, division, absolute_import

import logging
import os.path

from rdbtools import RdbParser, RdbCallback


logger = logging.getLogger(__name__)


class _Callback(RdbCallback):
    def __init__(self, client):
        super(_Callback, self).__init__(string_escape=None)
        self.client = client
        self._zset = []
        self.n_keys = 0

    def set(self, key, value, expiry, info):
        self.client.set(key, value, expiry)
        self.n_keys += 1

    def start_sorted_set(self, key, length, expiry, info):
        self._zset = []
        self.n_keys += 1

    def zadd(self, key, score, member):
        self._zset.append(score)
        self._zset.append(member)

    def end_sorted_set(self, key):
        self.client.zadd(key, *self._zset)
        self._zset = []


def load_from_file(client, filename):
    """Load keys from the specified RDB-compatible dump file.

    Parameters
    ---------
    client : :class:`redis.StrictRedis`-like
        Redis client wrapper
    filename : str
        Filename of .rdb file to import

    Returns
    -------
    int
        Number of keys loaded
    """
    callback = _Callback(client)
    parser = RdbParser(callback)
    logger.debug("Loading data from RDB dump of %d bytes", os.path.getsize(filename))
    parser.parse(filename)
    return callback.n_keys
