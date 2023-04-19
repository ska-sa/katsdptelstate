################################################################################
# Copyright (c) 2019-2021, National Research Foundation (Square Kilometre Array)
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
import enum
import itertools
import logging
from typing import (List, Tuple, Dict, Set, Generator, AsyncGenerator,
                    Iterable, Callable, Awaitable, Optional, Union, Any)

import pkg_resources
from redis import asyncio as aioredis

from .. import utils
from .backend import Backend
from ..backend import KeyUpdateBase, MutableKeyUpdate, ImmutableKeyUpdate, IndexedKeyUpdate
from ..errors import ImmutableKeyError
from ..utils import KeyType, ensure_str, display_str


logger = logging.getLogger(__name__)
_QueueItem = Tuple[bytes, Optional[bytes]]
# Note: this must be valid UTF-8, because aioredis decodes it if it needs to
# reconnect to the server.
_DUMMY_CHANNEL = b'\0katsdptelstate-internal0\001'


@contextlib.contextmanager
def _handle_wrongtype() -> Generator[None, None, None]:
    """Map redis WRONGTYPE error to ImmutableKeyError.

    It also handles WRONGTYPE errors from inside scripts.
    """
    try:
        yield
    except aioredis.ResponseError as error:
        if (error.args[0].startswith('WRONGTYPE ')
                or (error.args[0].startswith('Error running script')
                    and 'WRONGTYPE ' in error.args[0])):
            raise ImmutableKeyError
        raise


class _CommandType(enum.Enum):
    SUBSCRIBE = 1
    UNSUBSCRIBE = 2


class _Command:
    """Request to the pub/sub loop to subscribe or unsubscribe a queue."""

    def __init__(self, keys: Iterable[bytes], queue: 'asyncio.Queue[_QueueItem]',
                 type: _CommandType) -> None:
        self.keys = list(keys)
        self.queue = queue
        self.type = type

    def __repr__(self) -> str:
        return f'{self.__class__}({self.keys!r}, {self.queue!r}, {self.type!r}'


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
        self._pubsub = client.pubsub()
        self._pubsub_task = asyncio.get_event_loop().create_task(self._run_pubsub())
        self._commands = asyncio.Queue()  # type: asyncio.Queue[_Command]
        # Channels are indexed by key, not pub/sub channel name
        self._channels = {}       # type: Dict[bytes, Set[asyncio.Queue[_QueueItem]]]
        self._scripts = {}
        self._close_task = None   # type: Optional[asyncio.Task]
        for script_name in ['get', 'set_immutable', 'get_indexed', 'set_indexed',
                            'add_mutable']:
            script = pkg_resources.resource_string(
                'katsdptelstate', 'lua_scripts/{}.lua'.format(script_name))
            self._scripts[script_name] = self.client.register_script(script)

    @classmethod
    async def from_url(cls, url: str, *, db: Optional[int] = None) -> 'RedisBackend':
        """Create a backend from a redis URL.

        This is the recommended approach as it ensures that the server is
        reachable, and sets some timeouts to reasonable values.
        """
        # These are the same timeouts as the synchronous client
        client = aioredis.Redis.from_url(
            url, db=db, socket_timeout=5, health_check_interval=30)
        # The above doesn't actually try to talk to the server. Do that before
        # claiming success.
        await client.ping()
        return cls(client)

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
        except (ConnectionError, aioredis.ConnectionError) as exc:
            # Closes all connections in the pool to ensure that we get a
            # fresh connection.
            logger.warning('redis connection error (%s), trying again', exc)
            return await call(*args, **kwargs)

    async def _call(self, script_name: str, *args, **kwargs) -> Any:
        """Call a Lua script by name.

        This uses _execute, so the script must be idempotent.
        """
        return await self._execute(self._scripts[script_name], *args, **kwargs)

    async def exists(self, key: bytes) -> bool:
        return bool(await self._execute(self.client.exists, key))

    async def keys(self, filter: bytes) -> List[bytes]:
        return await self._execute(self.client.keys, filter)

    async def delete(self, key: bytes) -> None:
        await self._execute(self.client.unlink, key)

    async def clear(self) -> None:
        await self._execute(self.client.flushdb, asynchronous=True)

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
                         packed_st: bytes, packed_et: bytes,
                         include_previous: bool) -> Optional[List[Tuple[bytes, float]]]:
        """Internal part of :meth:`get_range` that is retried if the connection fails."""
        with _handle_wrongtype():
            async with self.client.pipeline() as pipe:
                pipe.exists(key)
                if include_previous and packed_st != b'-':
                    pipe.zrevrangebylex(key, packed_st, b'-', start=0, num=1)
                if packed_st != b'+' and packed_et != b'-':
                    pipe.zrangebylex(key, packed_st, packed_et)
                # raise_on_error annotates the exception message, which breaks
                # the _handle_wrongtype detection.
                ret_vals = await pipe.execute(raise_on_error=False)
            for item in ret_vals:
                if isinstance(item, aioredis.ResponseError):
                    raise item
        if not ret_vals[0]:
            return None      # Key does not exist
        return [utils.split_timestamp(val) for val in itertools.chain(*ret_vals[1:])]

    async def get_range(self, key: bytes, start_time: float, end_time: float,
                        include_previous: bool,
                        include_end: bool) -> Optional[List[Tuple[bytes, float]]]:
        packed_st = utils.pack_query_timestamp(start_time, False)
        packed_et = utils.pack_query_timestamp(end_time, True, include_end)
        return await self._execute(self._get_range, key, packed_st, packed_et, include_previous)

    def _handle_message(self, message: Dict[str, Any]) -> None:
        """Process a message received via pub/sub."""
        logger.debug('Received pub/sub message %s', message)
        channel_name = message['channel']
        if channel_name == _DUMMY_CHANNEL:
            return
        if isinstance(channel_name, int):
            # Extra workaround for
            # https://github.com/aio-libs/aioredis-py/issues/1206,
            # although subscribing to _DUMMY_CHANNEL should be sufficient.
            return
        assert channel_name.startswith(b'update/')
        key = channel_name[7:]
        channel = self._channels.get(key)
        if not channel:
            return
        if message['type'] == 'subscribe':
            update = (key, None)
        elif message['type'] == 'message':
            update = (key, message['data'])
        else:
            return
        for queue in channel:
            queue.put_nowait(update)

    async def _handle_command(self, command: _Command) -> None:
        """Process a :class:`_Command` received on the command queue."""
        logger.debug('Received command %s', command)
        if command.type == _CommandType.SUBSCRIBE:
            channel_updates = {}
            to_subscribe = []
            for key in command.keys:
                channel = self._channels.get(key)
                if channel is None:
                    channel = set()
                    channel_updates[key] = channel
                    to_subscribe.append(b'update/' + key)
                channel.add(command.queue)
            if to_subscribe:
                await self._pubsub.subscribe(*to_subscribe)
            # Only update the dict after subscription is successful; if it
            # fails the command will be retried.
            self._channels.update(channel_updates)
        elif command.type == _CommandType.UNSUBSCRIBE:
            to_unsubscribe = []
            channel_removals = []
            for key in command.keys:
                channel = self._channels.get(key)
                if channel is None:
                    # Should never happen, but exception handling gets tricky
                    # so rather be robust.
                    continue  # pragma: nocover
                channel.discard(command.queue)
                if not channel:
                    to_unsubscribe.append(b'update/' + key)
                    channel_removals.append(key)
            if to_unsubscribe:
                await self._pubsub.unsubscribe(*to_unsubscribe)
            # Only update the dict after unsubscription is successful; if it
            # fails the command will be retried.
            for key in channel_removals:
                self._channels.pop(key, None)
        else:
            raise RuntimeError(f'Unknown command type {command.type}')

    async def _run_pubsub(self) -> None:
        """Background task for handling pub/sub messages.

        It has two sources of input: messages on the pub/sub connection, and
        commands to add or remove subscriptions. These are multiplexed
        together rather than handled independently, because it's not clear
        that a single :class:`aioredis.client.PubSub` is async-safe.
        """
        get_message_task: Optional[asyncio.Task] = None
        command_queue_task: Optional[asyncio.Task] = None
        try:
            loop = asyncio.get_event_loop()
            # Ensure we are always subscribed to something, as a workaround for
            # https://github.com/aio-libs/aioredis-py/issues/1206.
            await self._pubsub.subscribe(_DUMMY_CHANNEL)
            command_queue_task = loop.create_task(self._commands.get())
            tasks: Set[asyncio.Future] = {command_queue_task}
            while True:
                # get_message raises an error if we try this when not connected.
                if get_message_task is None and self._pubsub.connection:
                    # Using a small timeout ensures that health checks get run
                    get_message_task = loop.create_task(self._pubsub.get_message(timeout=1))
                    tasks.add(get_message_task)
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

                if get_message_task is not None and get_message_task in done:
                    try:
                        message = await get_message_task
                    except (ConnectionError, aioredis.ConnectionError) as exc:
                        message = None
                        logger.warning('redis connection error (%s), trying to reconnect', exc)
                        # aioredis doesn't automatically reconnect
                        # (see https://github.com/andymccurdy/redis-py/issues/1464).
                        try:
                            if self._pubsub.connection is not None:
                                await self._pubsub.connection.disconnect()
                                await self._pubsub.connection.connect()
                        except (ConnectionError, aioredis.ConnectionError) as exc:
                            # Avoid spamming the server with connection attempts
                            logger.warning('redis reconnect attempt failed (%s), trying in 1s', exc)
                        await asyncio.sleep(1)
                    finally:
                        # Causes new task to created on next iteration
                        get_message_task = None
                    if message is not None:
                        self._handle_message(message)

                if command_queue_task in done:
                    try:
                        command = await command_queue_task
                        await self._handle_command(command)
                    except aioredis.ConnectionError as exc:
                        # Put the task back on the pending list, so that next
                        # time around the loop we'll process it again.
                        tasks.add(command_queue_task)
                        logger.warning('subscribe/unsubscribe failed (%s), trying in 1s', exc)
                        await asyncio.sleep(1)
                    else:
                        # Get ready to retrieve the next task
                        command_queue_task = loop.create_task(self._commands.get())
                        tasks.add(command_queue_task)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('Unexpected error in pubsub task')
            raise
        finally:
            if get_message_task is not None:
                get_message_task.cancel()
            if command_queue_task is not None:
                command_queue_task.cancel()

    async def monitor_keys(self, keys: Iterable[bytes]) -> AsyncGenerator[KeyUpdateBase, None]:
        # Refer to katsdptelstate.redis.RedisBackend for details of the protocol.
        keys = list(keys)  # Protect against generators that can only be iterated once
        queue = asyncio.Queue()  # type: asyncio.Queue[_QueueItem]
        try:
            self._commands.put_nowait(_Command(keys, queue, _CommandType.SUBSCRIBE))

            # Condition may have been satisfied while we were busy subscribing
            logger.debug('Requested subscription to channels, notifying caller to check')
            yield KeyUpdateBase()
            while True:
                update = await queue.get()
                key, data = update
                logger.debug('Received update on channel %r', key)
                if data is None:
                    # A subscription has been completed; just recheck
                    yield KeyUpdateBase()
                else:
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
            self._commands.put_nowait(_Command(keys, queue, _CommandType.UNSUBSCRIBE))

    async def dump(self, key: bytes) -> Optional[bytes]:
        return await self._execute(self.client.dump, key)

    async def _close(self) -> None:
        self._pubsub_task.cancel()
        try:
            await self._pubsub_task
        except asyncio.CancelledError:
            pass
        if self._pubsub.connection and self._pubsub.connection.is_connected:
            await self._pubsub.connection.disconnect()
        await self.client.close()
        await self.client.connection_pool.disconnect()

    def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.get_event_loop().create_task(self._close())

    async def wait_closed(self) -> None:
        if self._close_task is None:
            self.close()
        assert self._close_task is not None
        await self._close_task
