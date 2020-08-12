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
import itertools
import logging
from typing import (List, Tuple, Dict, Generator, AsyncGenerator, Sequence,
                    Iterable, Callable, Awaitable, Optional, Union, Any)

import pkg_resources
import aioredis

from .. import utils
from .backend import Backend
from ..backend import KeyUpdateBase, MutableKeyUpdate, ImmutableKeyUpdate, IndexedKeyUpdate
from ..errors import ImmutableKeyError
from ..utils import KeyType, ensure_str, display_str


logger = logging.getLogger(__name__)


def _is_wrongtype(error: aioredis.ReplyError) -> bool:
    return (
        error.args[0].startswith('WRONGTYPE ')
        or (error.args[0].startswith('ERR Error running script')
            and 'WRONGTYPE ' in error.args[0])
    )


@contextlib.contextmanager
def _handle_wrongtype() -> Generator[None, None, None]:
    """Map redis WRONGTYPE error to ImmutableKeyError.

    It also handles WRONGTYPE errors from inside scripts.
    """
    try:
        yield
    except aioredis.PipelineError as exc:
        for error in exc.args[1]:
            if _is_wrongtype(error):
                raise ImmutableKeyError
        raise
    except aioredis.ReplyError as error:
        if _is_wrongtype(error):
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


def _unpack_query_timestamp(packed_timestamp: bytes) -> Tuple[bytes, bool]:
    """Split the result of :func:`katsdptelstate.utils.pack_query_timestamp`.

    Returns
    -------
    value
        Boundary value
    include
        Whether the boundary value is inclusive (false for ``-`` and ``+``).
    """
    if packed_timestamp in {b'-', b'+'}:
        return packed_timestamp, False
    elif packed_timestamp[:1] == b'[':
        return packed_timestamp[1:], True
    elif packed_timestamp[:1] == b'(':
        return packed_timestamp[1:], False
    else:
        raise ValueError('packed_timestamp must be -, +, or start with [ or (')


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
                            'add_mutable']:
            script = pkg_resources.resource_string(
                'katsdptelstate', 'lua_scripts/{}.lua'.format(script_name))
            self._scripts[script_name] = _Script(script)

    async def _execute(self, call: Callable[..., Awaitable], *args, **kwargs) -> Any:
        """Execute a redis command, retrying if the connection died.

        This handles cases like an idle connection being silently closed,
        the server restarting etc. If there is a connection error, a fresh
        connection is made and one more attempt is made.

        Since there is no way to tell if the original attempt actually
        succeeded, the command must be idempotent.
        """
        try:
            return await call(*args, **kwargs)
        except (ConnectionError, aioredis.ConnectionClosedError) as exc:
            # Closes all connections in the pool to ensure that we get a
            # fresh connection.
            logger.warning('redis connection error (%s), trying again', exc)
            await self.client.connection.clear()
            return await call(*args, **kwargs)

    async def _call(self, script_name: str, *args, **kwargs) -> Any:
        """Call a Lua script by name.

        This uses _execute, so the script must be idempotent.
        """
        return await self._execute(self._scripts[script_name], self.client, *args, **kwargs)

    async def exists(self, key: bytes) -> bool:
        return bool(await self._execute(self.client.exists, key))

    async def keys(self, filter: bytes) -> List[bytes]:
        return await self._execute(self.client.keys, filter)

    async def delete(self, key: bytes) -> None:
        await self._execute(self.client.unlink, key)

    async def clear(self) -> None:
        await self._execute(self.client.flushdb, async_op=True)

    async def key_type(self, key: bytes) -> Optional[KeyType]:
        type_ = await self._execute(self.client.type, key)
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

    async def _get_range(self, key: bytes,
                         st: bytes, include_st: bool,
                         et: bytes, include_et: bool,
                         include_previous: bool) -> list:
        """Internal part of :meth:`get_range` that is retried if the connection fails."""
        tr = self.client.multi_exec()
        tr.exists(key)
        if include_previous and st != b'-':
            tr.zrevrangebylex(key, max=st, include_max=False, min=b'-', offset=0, count=1)
        # aioredis doesn't correctly handle min of + or max of -
        if st != b'+' and et != b'-':
            tr.zrangebylex(key, min=st, include_min=include_st, max=et, include_max=include_et)
        return await tr.execute()

    async def get_range(self, key: bytes, start_time: float, end_time: float,
                        include_previous: bool,
                        include_end: bool) -> Optional[List[Tuple[bytes, float]]]:
        packed_st = utils.pack_query_timestamp(start_time, False)
        packed_et = utils.pack_query_timestamp(end_time, True, include_end)
        # aioredis wants these split out and combined them itself
        st, include_st = _unpack_query_timestamp(packed_st)
        et, include_et = _unpack_query_timestamp(packed_et)
        with _handle_wrongtype():
            ret_vals = await self._execute(
                self._get_range, key, st, include_st, et, include_et, include_previous)
        if not ret_vals[0]:
            return None     # Key does not exist
        return [utils.split_timestamp(val) for val in itertools.chain(*ret_vals[1:])]

    async def monitor_keys(self, keys: Iterable[bytes]) -> AsyncGenerator[KeyUpdateBase, None]:
        # Refer to katsdptelstate.redis.RedisBackend for details of the protocol.
        while True:
            receiver = aioredis.pubsub.Receiver()
            # We use a dedicated connection for the lifetime of this generator.
            # While the connection pool also has a member to hold a pubsub
            # connection, it's more robust to just close a connection than to
            # try to manage unsubscriptions.
            client: Optional[aioredis.Redis] = None
            try:
                conn = await self.client.connection.acquire()
                client = aioredis.Redis(conn)
                await client.subscribe(*(receiver.channel(b'update/' + key) for key in keys))
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
            except (ConnectionError, aioredis.ConnectionClosedError) as exc:
                logger.warning('pubsub connection error (%s), retrying in 1s', exc)
            finally:
                if client is not None:
                    client.close()
                    await client.wait_closed()
                receiver.stop()
            await asyncio.sleep(1)

    async def dump(self, key: bytes) -> Optional[bytes]:
        return await self._execute(self.client.dump, key)

    def close(self) -> None:
        self.client.close()

    async def wait_closed(self) -> None:
        await self.client.wait_closed()
