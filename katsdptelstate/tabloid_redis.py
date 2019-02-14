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
import struct

from fakeredis import FakeStrictRedis

from .rdb_utility import dump_string, dump_zset


class TabloidRedis(FakeStrictRedis):
    """A Redis-like class that provides a very superficial
    simulacrum of a real Redis server. Designed specifically to
    support the read cases in use by katsdptelstate.

    The Redis-like functionality is almost entirely derived from FakeStrictRedis,
    we only add a dump function.
    """
    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        super(TabloidRedis, self).__init__(**kwargs)

    def dump(self, key):
        """Encode Redis key value in an RDB compatible format.
           Note: This follows the DUMP command in Redis itself which produces output
           that is similarly encoded to an RDB, but not exactly the same.

           ZSet scores are ignored and encoded as zero.

           Returns None if `key` not found.
        """

        key_type = self.type(key)
        if key_type == b'none':
            return None
        if key_type == b'zset':
            data = self.zrange(key, 0, -1)
            return dump_zset(data)
        if key_type == b'string':
            return dump_string(self.get(key))
        raise NotImplementedError("Unsupported key type {}. Must be either string or zset".format(key_type))
