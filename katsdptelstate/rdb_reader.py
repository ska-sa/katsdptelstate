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

import logging
import os.path

from rdbtools import RdbParser, RdbCallback
from . import compat


logger = logging.getLogger(__name__)


class Callback(RdbCallback):
    """Callback that stores keys in :class:`redis.StrictRedis`-like client."""
    def __init__(self, client):
        super(Callback, self).__init__(string_escape=None)
        self.client = client
        self._zset = {}
        self.n_keys = 0

    def set(self, key, value, expiry, info):
        self.client.set(key, value, expiry)
        self.n_keys += 1

    def start_sorted_set(self, key, length, expiry, info):
        self._zset = {}
        self.n_keys += 1

    def zadd(self, key, score, member):
        self._zset[member] = score

    def end_sorted_set(self, key):
        compat.zadd(self.client, key, self._zset)
        self._zset = {}


def load_from_file(callback, file):
    """Load keys from the specified RDB-compatible dump file.

    Parameters
    ----------
    callback : :class:`rdbtools.RdbCallback`-like
        Backend-specific callback that stores keys as RDB file is parsed
    file : str or file-like object
        Filename of .rdb file to import, or object representing contents of RDB

    Returns
    -------
    int
        Number of keys loaded
    """
    try:
        size = os.path.getsize(file)
    except TypeError:
        start_pos = file.tell()
        file.seek(0, 2)
        size = file.tell() - start_pos
        file.seek(start_pos)
    except AttributeError:
        size = -1   # File object does not support seeking
    logger.debug("Loading data from RDB dump of %s bytes",
                 str(size) if size >= 0 else 'unknown')
    parser = RdbParser(callback)
    try:
        parser.parse(file)
    except TypeError:
        parser.parse_fd(file)
    return callback.n_keys
