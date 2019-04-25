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
import os.path

from rdbtools import RdbParser, RdbCallback

from . import compat
from .telescope_state import RdbParseError


logger = logging.getLogger(__name__)


class Callback(RdbCallback):
    """Callback that stores keys in :class:`redis.StrictRedis`-like client."""
    def __init__(self, client):
        super(Callback, self).__init__(string_escape=None)
        self.client = client
        self._zset = {}
        self.n_keys = 0
        # Flag that helps to disambiguate callback errors from parser errors
        self.client_busy = False

    def set(self, key, value, expiry, info):
        self.client_busy = True
        self.client.set(key, value, expiry)
        self.client_busy = False
        self.n_keys += 1

    def start_sorted_set(self, key, length, expiry, info):
        self._zset = {}
        self.n_keys += 1

    def zadd(self, key, score, member):
        self._zset[member] = score

    def end_sorted_set(self, key):
        self.client_busy = True
        compat.zadd(self.client, key, self._zset)
        self.client_busy = False
        self._zset = {}


def _parse_rdb_file(parser, fd, filename=None):
    """Apply RDB parser to file descriptor, raising RdbParseError on error."""
    try:
        parser.parse_fd(fd)
    except Exception as exc:
        # Don't remap exception to RdbParseError if it originates from callback
        if parser._callback.client_busy:
            raise
        raise_from(RdbParseError(filename), exc)


def load_from_file(callback, file):
    """Load keys from the specified RDB-compatible dump file.

    Parameters
    ----------
    callback : :class:`rdbtools.RdbCallback`-like with `n_keys` attribute
        Backend-specific callback that stores keys as RDB file is parsed. In
        addition to the interface of :class:`rdbtools.RdbCallback` it should
        have an `n_keys` attribute that reflects the number of keys loaded from
        the RDB file.
    file : str or file-like object
        Filename of .rdb file to import, or object representing contents of RDB

    Returns
    -------
    int
        Number of keys loaded (obtained from :attr:`callback.n_keys`)

    Raises
    ------
    RdbParseError
        If `file` does not represent a valid RDB file
    """
    try:
        size = os.path.getsize(file)
    except TypeError:
        # One could maybe seek() and tell() on file object but is it worth it?
        size = 'unknown'
    logger.debug("Loading data from RDB dump of %s bytes", size)
    parser = RdbParser(callback)
    try:
        fd = open(file, 'rb')
    except TypeError:
        _parse_rdb_file(parser, file)
    else:
        with fd:
            _parse_rdb_file(parser, fd, file)
    return callback.n_keys
