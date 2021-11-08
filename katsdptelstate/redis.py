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
from datetime import datetime
import itertools
from typing import List, Tuple, Dict, BinaryIO, Generator, Iterable, Optional, Union, Any

import pkg_resources
import redis

from . import utils
from .backend import Backend, KeyUpdateBase, MutableKeyUpdate, ImmutableKeyUpdate, IndexedKeyUpdate
from .errors import ConnectionError, ImmutableKeyError
from .utils import KeyType, ensure_str, display_str, _PathType
try:
    from . import rdb_reader
    from .rdb_reader import BackendCallback
except ImportError as _rdb_reader_import_error:   # noqa: F841
    rdb_reader = None            # type: ignore
    BackendCallback = object     # type: ignore   # So that RedisCallback can still be defined


class RedisCallback(BackendCallback):
    """RDB callback that stores keys in :class:`redis.Redis`-like client."""

    def __init__(self, client: redis.Redis) -> None:
        super().__init__()
        self.client = client
        self._zset = {}         # type: Dict[bytes, float]
        self._hash = []         # type: List[bytes]

    def set(self, key: bytes, value: bytes, expiry: Optional[datetime], info: dict) -> None:
        self.client_busy = True
        # mypy 0.720's typeshed doesn't allow bytes keys
        self.client.set(key, value)        # type: ignore
        if expiry is not None:
            self.client.expireat(key, expiry)
        self.client_busy = False
        self.n_keys += 1

    def start_sorted_set(self, key: bytes, length: int,
                         expiry: Optional[datetime], info: dict) -> None:
        self._zset = {}
        self.n_keys += 1

    def zadd(self, key: bytes, score: float, member: bytes) -> None:
        self._zset[member] = score

    def end_sorted_set(self, key):
        self.client_busy = True
        self.client.zadd(key, self._zset)
        self.client_busy = False
        self._zset = {}

    def start_hash(self, key: bytes, length: int, expiry: Optional[datetime], info: dict) -> None:
        self._hash = []
        self.n_keys += 1

    def hset(self, key: bytes, field: bytes, value: bytes) -> None:
        self._hash.extend([field, value])

    def end_hash(self, key: bytes) -> None:
        self.client_busy = True
        # redis-py's hset takes a mapping rather than interleaved keys and
        # values. To avoid the cost of building a dict and then flattening it
        # again, we execute the raw Redis command directly.
        self.client.execute_command('HSET', key, *self._hash)
        self.client_busy = False
        self._hash = []


@contextlib.contextmanager
def _handle_wrongtype() -> Generator[None, None, None]:
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

    def __init__(self, client: redis.Redis) -> None:
        self.client = client
        try:
            # This is the first command to the server and therefore
            # the first test of its availability
            self.client.ping()
        except (redis.TimeoutError, redis.ConnectionError) as e:
            # redis.TimeoutError: bad host
            # redis.ConnectionError: good host, bad port
            raise ConnectionError("could not connect to redis server: {}".format(e))
        self._scripts = {}     # type: Dict[str, redis.client.Script]
        for script_name in ['get', 'set_immutable', 'get_indexed', 'set_indexed',
                            'add_mutable']:
            script = pkg_resources.resource_string(
                'katsdptelstate', 'lua_scripts/{}.lua'.format(script_name))
            self._scripts[script_name] = self.client.register_script(script)

    @classmethod
    def from_url(cls, url: str, *, db: Optional[int] = None) -> 'RedisBackend':
        """Create a backend from a redis URL.

        This is the recommended approach as it ensures that the server is
        reachable, and sets some timeouts to reasonable values.
        """
        client = redis.Redis.from_url(
            url,
            db=db,
            socket_timeout=5,
            health_check_interval=30
        )
        return cls(client)

    def _call(self, script_name: str, *args, **kwargs) -> Any:
        return self._scripts[script_name](*args, **kwargs)

    def load_from_file(self, file: Union[_PathType, BinaryIO]) -> int:
        if rdb_reader is None:
            raise _rdb_reader_import_error   # noqa: F821
        return rdb_reader.load_from_file(RedisCallback(self.client), file)

    def __contains__(self, key: bytes) -> bool:
        # ignore due to typeshed bug: https://github.com/python/typeshed/pull/3969
        return bool(self.client.exists(key))     # type: ignore

    def keys(self, filter: bytes) -> List[bytes]:
        return self.client.keys(filter)

    def delete(self, key: bytes) -> None:
        # ignore due to typeshed bug: https://github.com/python/typeshed/pull/3969
        self.client.unlink(key)                  # type: ignore

    def clear(self) -> None:
        # typeshed doesn't know about the asynchronous argument
        self.client.flushdb(asynchronous=True)   # type: ignore

    def key_type(self, key: bytes) -> Optional[KeyType]:
        type_ = self.client.type(key)
        return self._KEY_TYPES[type_]

    def set_immutable(self, key: bytes, value: bytes) -> Optional[bytes]:
        with _handle_wrongtype():
            return self._call('set_immutable', [key], [value])

    def get(self, key: bytes) -> Union[
            Tuple[None, None],
            Tuple[bytes, None],
            Tuple[bytes, float],
            Tuple[Dict[bytes, bytes], None]]:
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

    def add_mutable(self, key: bytes, value: bytes, timestamp: float) -> None:
        str_val = utils.pack_timestamp(timestamp) + value
        with _handle_wrongtype():
            self._call('add_mutable', [key], [str_val])

    def set_indexed(self, key: bytes, sub_key: bytes, value: bytes) -> Optional[bytes]:
        with _handle_wrongtype():
            return self._call('set_indexed', [key], [sub_key, value])

    def get_indexed(self, key: bytes, sub_key: bytes) -> Optional[bytes]:
        with _handle_wrongtype():
            result = self._call('get_indexed', [key], [sub_key])
            if result == 1:
                # Key does not exist
                raise KeyError
            else:
                return result

    def get_range(self, key: bytes, start_time: float, end_time: float,
                  include_previous: bool, include_end: bool) -> Optional[List[Tuple[bytes, float]]]:
        packed_st = utils.pack_query_timestamp(start_time, False)
        packed_et = utils.pack_query_timestamp(end_time, True, include_end)
        with _handle_wrongtype():
            with self.client.pipeline() as pipe:
                pipe.exists(key)
                if include_previous and packed_st != b'-':
                    pipe.zrevrangebylex(key, packed_st, b'-', start=0, num=1)
                if packed_st != b'+' and packed_et != b'-':
                    pipe.zrangebylex(key, packed_st, packed_et)
                # raise_on_error annotates the exception message, which breaks
                # the _handle_wrongtype detection.
                ret_vals = pipe.execute(raise_on_error=False)
            for item in ret_vals:
                if isinstance(item, redis.ResponseError):
                    raise item
        if not ret_vals[0]:
            return None      # Key does not exist
        return [utils.split_timestamp(val) for val in itertools.chain(*ret_vals[1:])]

    def monitor_keys(self, keys: Iterable[bytes]) \
            -> Generator[Optional[KeyUpdateBase], Optional[float], None]:
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
                # ignore due to typeshed bug: https://github.com/python/typeshed/pull/3969
                p.subscribe(b'update/' + key)     # type: ignore
            timeout = yield None
            while True:
                assert timeout is not None
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
                        timeout = yield KeyUpdateBase()
                    elif message['type'] == 'message':
                        # First check if there is a key type byte in the message.
                        data = message['data']
                        try:
                            key_type = KeyType(data[0])
                        except (ValueError, IndexError):
                            # It's the older format lacking a key type byte
                            key_type2 = self.key_type(key)
                            if key_type2 is None:
                                continue     # Key was deleted before we could retrieve the type
                            key_type = key_type2
                        else:
                            data = data[1:]  # Strip off the key type
                        if key_type == KeyType.IMMUTABLE:
                            timeout = yield ImmutableKeyUpdate(key, data)
                        elif key_type == KeyType.INDEXED:
                            # Encoding is <sub-key-length>\n<sub-key><value>
                            newline = data.find(b'\n')
                            if newline == -1:
                                raise RuntimeError('Pubsub message missing newline')
                            sub_key_len = int(data[:newline])
                            sub_key = data[newline + 1 : newline + 1 + sub_key_len]
                            value = data[newline + 1 + sub_key_len :]
                            timeout = yield IndexedKeyUpdate(key, sub_key, value)
                        elif key_type == KeyType.MUTABLE:
                            value, timestamp = utils.split_timestamp(data)
                            timeout = yield MutableKeyUpdate(key, value, timestamp)
                        else:
                            raise RuntimeError('Unhandled key type {}'.format(key_type))

    def dump(self, key: bytes) -> Optional[bytes]:
        return self.client.dump(key)
