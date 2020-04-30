################################################################################
# Copyright (c) 2019-2020, National Research Foundation (Square Kilometre Array)
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
from .backend import Backend
from .errors import ConnectionError, ImmutableKeyError
from .utils import KeyType, ensure_str, display_str
try:
    from . import rdb_reader
    from .rdb_reader import BackendCallback
except ImportError as _rdb_reader_import_error:   # noqa: F841
    rdb_reader = None
    BackendCallback = object     # So that RedisCallback can still be defined


class RedisCallback(BackendCallback):
    """RDB callback that stores keys in :class:`redis.Redis`-like client."""

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
        self.client.zadd(key, self._zset)
        self.client_busy = False
        self._zset = {}

    def start_hash(self, key, length, expiry, info):
        self._hash = []
        self.n_keys += 1

    def hset(self, key, field, value):
        self._hash.extend([field, value])

    def end_hash(self, key):
        self.client_busy = True
        # redis-py's hset doesn't support multiple key/value pairs, and its
        # hmset takes a mapping rather than interleaved keys and values. To
        # avoid the cost of building a dict and then flattening it again,
        # we execute the raw Redis command directly.
        self.client.execute_command('HSET', key, *self._hash)
        self.client_busy = False
        self._hash = []


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

    _KEY_TYPES = {
        b'none': None,
        b'string': KeyType.IMMUTABLE,
        b'hash': KeyType.INDEXED,
        b'zset': KeyType.MUTABLE
    }

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
            raise ConnectionError("could not connect to redis server: {}".format(e))
        self._scripts = {}
        for script_name in ['get', 'set_immutable', 'get_indexed', 'set_indexed',
                            'add_mutable', 'get_range']:
            script = pkg_resources.resource_string(
                'katsdptelstate', 'lua_scripts/{}.lua'.format(script_name))
            self._scripts[script_name] = self.client.register_script(script)

    def _call(self, script_name, *args, **kwargs):
        return self._scripts[script_name](*args, **kwargs)

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

    def key_type(self, key):
        type_ = self.client.type(key)
        return self._KEY_TYPES[type_]

    def set_immutable(self, key, value):
        with _handle_wrongtype():
            return self._call('set_immutable', [key], [value])

    def get(self, key):
        result = self._call('get', [key])
        if result[1] == b'none':
            return None, None
        elif result[1] == b'string':
            return result[0], None
        elif result[1] == b'zset':
            return utils.split_timestamp(result[0])
        elif result[1] == b'hash':
            it = iter(result[0])
            d = dict(zip(it, it))
            return d, None
        else:
            raise RuntimeError('Unknown key type {} for key {}'
                               .format(ensure_str(result[1]), display_str(key)))

    def add_mutable(self, key, value, timestamp):
        str_val = utils.pack_timestamp(timestamp) + value
        with _handle_wrongtype():
            self._call('add_mutable', [key], [str_val])

    def set_indexed(self, key, sub_key, value):
        with _handle_wrongtype():
            return self._call('set_indexed', [key], [sub_key, value])

    def get_indexed(self, key, sub_key):
        with _handle_wrongtype():
            result = self._call('get_indexed', [key], [sub_key])
            if result == 1:
                # Key does not exist
                raise KeyError
            else:
                return result

    def get_range(self, key, start_time, end_time, include_previous, include_end):
        packed_st = utils.pack_query_timestamp(start_time, False)
        packed_et = utils.pack_query_timestamp(end_time, True, include_end)
        with _handle_wrongtype():
            ret_vals = self._call('get_range', [key], [packed_st, packed_et, int(include_previous)])
        if ret_vals is None:
            return None     # Key does not exist
        return [utils.split_timestamp(val) for val in ret_vals]

    def monitor_keys(self, keys):
        # Updates trigger pubsub messages to update/<key>. The format depends
        # on the key type:
        # immutable: [\x01] <value>
        # mutable:   [\x02] <timestamp> <value>
        # indexed:   \x03 <subkey-len> \n <subkey> <value>
        #            (subkey-len is ASCII text)
        # The first byte corresponds to the values in the KeyType
        # enumeration. For immutable and mutable, the leading byte is
        # currently omitted, and the key type needs to be looked up. This can
        # only be fixed once the code to handle these bytes is present in all
        # deployments.
        #
        # This can in theory cause ambiguities, because the first byte could
        # be either the key type or the start of a value or timestamp.
        # However, values are encoded and the encoding currently in use is
        # signalled with a \xFF at the start; and timestamps starting with
        # one of these bytes is infinitesimally close to zero and so will
        # not arise in practice.
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
                        timeout = yield ()
                    elif message['type'] == 'message':
                        # First check if there is a key type byte in the message.
                        data = message['data']
                        try:
                            key_type = KeyType(data[0])
                        except (ValueError, IndexError):
                            # It's the older format lacking a key type byte
                            key_type = self.key_type(key)
                            if key_type is None:
                                continue     # Key was deleted before we could retrieve the type
                        else:
                            data = data[1:]  # Strip off the key type
                        if key_type == KeyType.IMMUTABLE:
                            timeout = yield (key_type, key, data)
                        elif key_type == KeyType.INDEXED:
                            # Encoding is <sub-key-length>\n<sub-key><value>
                            newline = data.find(b'\n')
                            if newline == -1:
                                raise RuntimeError('Pubsub message missing newline')
                            sub_key_len = int(data[:newline])
                            sub_key = data[newline + 1 : newline + 1 + sub_key_len]
                            value = data[newline + 1 + sub_key_len :]
                            timeout = yield (key_type, key, value, sub_key)
                        elif key_type == KeyType.MUTABLE:
                            value, timestamp = utils.split_timestamp(data)
                            timeout = yield (key_type, key, value, timestamp)
                        else:
                            raise RuntimeError('Unhandled key type {}'.format(key_type))

    def dump(self, key):
        return self.client.dump(key)
