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

import contextlib

import pkg_resources
import redis

from . import utils
from .telescope_state import ConnectionError, ImmutableKeyError, Backend
from . import compat
try:
    from . import rdb_reader
    from .rdb_reader import BackendCallback
except ImportError as _rdb_reader_import_error:   # noqa: F841
    rdb_reader = None
    BackendCallback = object     # So that RedisCallback can still be defined


class RedisCallback(BackendCallback):
    """RDB callback that stores keys in :class:`redis.StrictRedis`-like client."""
    def __init__(self, client):
        super().__init__()
        self.client = client
        self._zset = {}

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


@contextlib.contextmanager
def _handle_wrongtype():
    """Map redis WRONGTYPE error to ImmutableKeyError.

    It also handles WRONGTYPE errors from inside scripts.
    """
    try:
        yield
    except redis.ResponseError as error:
        if (error.args[0].startswith('WRONGTYPE ')
                or (error.args[0].startswith('Error running script')
                    and 'WRONGTYPE ' in error.args[0])):
            raise ImmutableKeyError
        raise


class RedisBackend(Backend):
    """Backend for :class:`TelescopeState` using redis for storage."""

    def __init__(self, client):
        self.client = client
        self._ps = self.client.pubsub(ignore_subscribe_messages=True)
        try:
            # This is the first command to the server and therefore
            # the first test of its availability
            self.client.ping()
        except (redis.TimeoutError, redis.ConnectionError) as e:
            # redis.TimeoutError: bad host
            # redis.ConnectionError: good host, bad port
            raise ConnectionError(f"could not connect to redis server: {e}")
        self._get_script = self._register_script('get.lua')
        self._set_immutable_script = self._register_script('set_immutable.lua')
        self._add_mutable_script = self._register_script('add_mutable.lua')
        self._get_range_script = self._register_script('get_range.lua')

    def _register_script(self, basename):
        script = pkg_resources.resource_string('katsdptelstate', 'lua_scripts/' + basename)
        return self.client.register_script(script)

    def load_from_file(self, file):
        if rdb_reader is None:
            raise _rdb_reader_import_error   # noqa: F821
        return rdb_reader.load_from_file(RedisCallback(self.client), file)

    def __contains__(self, key):
        return self.client.exists(key)

    def keys(self, filter):
        return self.client.keys(filter)

    def delete(self, key):
        self.client.delete(key)

    def clear(self):
        self.client.flushdb()

    def is_immutable(self, key):
        type_ = self.client.type(key)
        if type_ == b'none':
            raise KeyError
        else:
            return type_ == b'string'

    def set_immutable(self, key, value):
        with _handle_wrongtype():
            result = self._set_immutable_script([key], [value])
            if isinstance(result, bytes):
                return result

    def get_immutable(self, key):
        with _handle_wrongtype():
            return self.client.get(key)

    def get(self, key):
        return self._get_script([key])

    def add_mutable(self, key, value, timestamp):
        str_val = utils.pack_timestamp(timestamp) + value
        with _handle_wrongtype():
            self._add_mutable_script([key], [str_val])

    def get_range(self, key, start_time, end_time, include_previous, include_end):
        packed_st = utils.pack_query_timestamp(start_time, False)
        packed_et = utils.pack_query_timestamp(end_time, True, include_end)
        with _handle_wrongtype():
            ret_vals = self._get_range_script([key], [packed_st, packed_et, int(include_previous)])
        if ret_vals is None:
            return None     # Key does not exist
        return [utils.split_timestamp(val) for val in ret_vals]

    def monitor_keys(self, keys):
        p = self.client.pubsub()
        with contextlib.closing(p):
            for key in keys:
                p.subscribe(b'update/' + key)
            timeout = yield None
            while True:
                message = p.get_message(timeout=timeout)
                if message is None:
                    timeout = yield None
                else:
                    assert message['channel'].startswith(b'update/')
                    key = message['channel'][7:]
                    if message['type'] == 'subscribe':
                        # An update may have occurred before our subscription
                        # happened. Also, if we lost connection and
                        # reconnected, we will get the subscribe message
                        # again.
                        timeout = yield (key,)
                    elif message['type'] == 'message':
                        # TODO: eventually change the protocol to indicate
                        # mutability in the message.
                        type_ = self.client.type(key)
                        if type_ == b'string':
                            timeout = yield (key, message['data'])
                        else:
                            value, timestamp = utils.split_timestamp(message['data'])
                            timeout = yield (key, value, timestamp)

    def dump(self, key):
        return self.client.dump(key)
