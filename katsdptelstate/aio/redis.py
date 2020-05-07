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

import asyncio
import contextlib
import hashlib
import logging
from typing import (List, Tuple, Dict, Generator, AsyncGenerator, Sequence,
                    Iterable, Optional, Union, Any)

import pkg_resources
import aioredis

from .. import utils
from .backend import Backend
from ..backend import KeyUpdateBase, MutableKeyUpdate, ImmutableKeyUpdate, IndexedKeyUpdate
from ..errors import ImmutableKeyError
from ..utils import KeyType, ensure_str, display_str


logger = logging.getLogger(__name__)


@contextlib.contextmanager
def _handle_wrongtype() -> Generator[None, None, None]:
    """Map redis WRONGTYPE error to ImmutableKeyError.

    It also handles WRONGTYPE errors from inside scripts.
    """
    try:
        yield
    except aioredis.ReplyError as error:
        if (error.args[0].startswith('WRONGTYPE ')
                or (error.args[0].startswith('ERR Error running script')
                    and 'WRONGTYPE ' in error.args[0])):
            raise ImmutableKeyError
        raise


class _Script:
    """Handle caching of scripts on the redis server."""

    def __init__(self, script: bytes) -> None:
        self.script = script
        self.digest = hashlib.sha1(self.script).hexdigest()

    async def __call__(self, client: aioredis.Redis,
                       keys: Sequence = [], args: Sequence = []) -> Any:
        try:
            return await client.evalsha(self.digest, keys, args)
        except aioredis.ReplyError as exc:
            if not exc.args[0].startswith('NOSCRIPT '):
                raise
        # The script wasn't cached, so try again with original source
        return await client.eval(self.script, keys, args)


class RedisBackend(Backend):
    """Backend for :class:`TelescopeState` using redis for storage."""

    _KEY_TYPES = {
        b'none': None,
        b'string': KeyType.IMMUTABLE,
        b'hash': KeyType.INDEXED,
        b'zset': KeyType.MUTABLE
    }

    def __init__(self, client: aioredis.Redis) -> None:
        self.client = client
        self._scripts = {}     # type: Dict[str, _Script]
        for script_name in ['get', 'set_immutable', 'get_indexed', 'set_indexed',
                            'add_mutable', 'get_range']:
            script = pkg_resources.resource_string(
                'katsdptelstate', 'lua_scripts/{}.lua'.format(script_name))
            self._scripts[script_name] = _Script(script)

    async def _call(self, script_name: str, *args, **kwargs) -> Any:
        return await self._scripts[script_name](self.client, *args, **kwargs)

    async def exists(self, key: bytes) -> bool:
        return bool(await self.client.exists(key))

    async def keys(self, filter: bytes) -> List[bytes]:
        return await self.client.keys(filter)

    async def delete(self, key: bytes) -> None:
        await self.client.delete(key)

    async def clear(self) -> None:
        await self.client.flushdb()

    async def key_type(self, key: bytes) -> Optional[KeyType]:
        type_ = await self.client.type(key)
        return self._KEY_TYPES[type_]

    async def set_immutable(self, key: bytes, value: bytes) -> Optional[bytes]:
        with _handle_wrongtype():
            return await self._call('set_immutable', [key], [value])

    async def get(self, key: bytes) -> Union[
            Tuple[None, None],
            Tuple[bytes, None],
            Tuple[bytes, float],
            Tuple[Dict[bytes, bytes], None]]:
        result = await self._call('get', [key])
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

    async def add_mutable(self, key: bytes, value: bytes, timestamp: float) -> None:
        str_val = utils.pack_timestamp(timestamp) + value
        with _handle_wrongtype():
            await self._call('add_mutable', [key], [str_val])

    async def set_indexed(self, key: bytes, sub_key: bytes, value: bytes) -> Optional[bytes]:
        with _handle_wrongtype():
            return await self._call('set_indexed', [key], [sub_key, value])

    async def get_indexed(self, key: bytes, sub_key: bytes) -> Optional[bytes]:
        with _handle_wrongtype():
            result = await self._call('get_indexed', [key], [sub_key])
            if result == 1:
                # Key does not exist
                raise KeyError
            else:
                return result

    async def get_range(self, key: bytes, start_time: float, end_time: float,
                        include_previous: bool,
                        include_end: bool) -> Optional[List[Tuple[bytes, float]]]:
        packed_st = utils.pack_query_timestamp(start_time, False)
        packed_et = utils.pack_query_timestamp(end_time, True, include_end)
        with _handle_wrongtype():
            ret_vals = await self._call('get_range', [key],
                                        [packed_st, packed_et, int(include_previous)])
        if ret_vals is None:
            return None     # Key does not exist
        return [utils.split_timestamp(val) for val in ret_vals]

    async def monitor_keys(self, keys: Iterable[bytes]) -> AsyncGenerator[KeyUpdateBase, None]:
        # Refer to katsdptelstate.redis.RedisBackend for details of the protocol
        while True:
            receiver = aioredis.pubsub.Receiver()
            await self.client.subscribe(*(receiver.channel(b'update/' + key) for key in keys))
            try:
                # Condition may have been satisfied while we were busy subscribing
                logger.debug('Subscribed to channels, notifying caller to check')
                yield KeyUpdateBase()
                async for channel, data in receiver.iter():
                    assert channel.name.startswith(b'update/')
                    key = channel.name[7:]
                    logger.debug('Received update on channel %r', key)
                    # First check if there is a key type byte in the message.
                    try:
                        key_type = KeyType(data[0])
                    except (ValueError, IndexError):
                        # It's the older format lacking a key type byte
                        key_type2 = await self.key_type(key)
                        if key_type2 is None:
                            continue     # Key was deleted before we could retrieve the type
                        key_type = key_type2
                    else:
                        data = data[1:]  # Strip off the key type
                    if key_type == KeyType.IMMUTABLE:
                        yield ImmutableKeyUpdate(key, data)
                    elif key_type == KeyType.INDEXED:
                        # Encoding is <sub-key-length>\n<sub-key><value>
                        newline = data.find(b'\n')
                        if newline == -1:
                            raise RuntimeError('Pubsub message missing newline')
                        sub_key_len = int(data[:newline])
                        sub_key = data[newline + 1 : newline + 1 + sub_key_len]
                        value = data[newline + 1 + sub_key_len :]
                        yield IndexedKeyUpdate(key, sub_key, value)
                    elif key_type == KeyType.MUTABLE:
                        value, timestamp = utils.split_timestamp(data)
                        yield MutableKeyUpdate(key, value, timestamp)
                    else:
                        raise RuntimeError('Unhandled key type {}'.format(key_type))
            finally:
                await asyncio.shield(self.client.unsubscribe(*(b'update/' + key for key in keys)))

    async def dump(self, key: bytes) -> Optional[bytes]:
        return await self.client.dump(key)
