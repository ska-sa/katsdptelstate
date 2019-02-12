################################################################################
# Copyright (c) 2019, National Research Foundation (Square Kilometre Array)
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

import itertools

import redis
import distutils.version


# This won't correctly handle a fakeredis 0.x client when redis-py 3.x is
# installed. However, the last fakeredis 0.x version has a dependency on
# redis<3 to prevent this.
if distutils.version.StrictVersion(redis.__version__) >= '3.0':
    def zadd(client, key, values):
        """Provide redis-py 3 interface to either v2 or v3 client"""
        return client.zadd(key, values)
else:
    def zadd(client, key, values):
        """Provide redis-py 3 interface to either v2 or v3 client"""
        # We can't just use **values, because the names might not be native
        # strings. So we have to produce a list of
        # score1, name1, score2, name2, ...
        it = itertools.chain(*((score, name) for (name, score) in values.items()))
        return client.zadd(key, *it)
