################################################################################
# Copyright (c) 2015-2019, National Research Foundation (Square Kilometre Array)
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

"""Tests for the asynchronous Telescope State client."""

import asyncio
from unittest import mock
from typing import AsyncGenerator, AnyStr

import async_timeout
import numpy as np
import pytest
import pytest_asyncio
import fakeredis.aioredis

import katsdptelstate
from katsdptelstate import ImmutableKeyError, encode_value, KeyType, ENCODING_MSGPACK
from katsdptelstate.aio import TelescopeState
from katsdptelstate.aio.memory import MemoryBackend
from katsdptelstate.aio.redis import RedisBackend, _DUMMY_CHANNEL


class TestTelescopeState:
    @pytest_asyncio.fixture
    async def ts(self) -> AsyncGenerator[TelescopeState, None]:
        ts = await self.make_telescope_state()
        yield ts
        ts.backend.close()
        await ts.backend.wait_closed()

    @pytest_asyncio.fixture
    async def ns(self, ts: TelescopeState) -> TelescopeState:
        return ts.view('ns')

    async def make_telescope_state(self) -> TelescopeState:
        return TelescopeState()

    def test_bad_construct(self, ts: TelescopeState) -> None:
        with pytest.raises(ValueError):
            TelescopeState(ts.backend, base=ts)

    def test_namespace(self, ts: TelescopeState, ns: TelescopeState) -> None:
        assert ts.prefixes == ('',)
        assert ns.prefixes == ('ns_', '')
        ns2 = ns.view(b'ns_child_grandchild')
        assert ns2.prefixes == ('ns_child_grandchild_', 'ns_', '')
        assert ns2.root().prefixes == ('',)
        ns_excl = ns.view('exclusive', exclusive=True)
        assert ns_excl.prefixes == ('exclusive_',)

    async def test_basic_add(self, ts: TelescopeState) -> None:
        await ts.add('test_key', 1234.5)
        assert await ts['test_key'] == 1234.5

        await ts.delete('test_key')
        with pytest.raises(KeyError):
            await ts['test_key']

    async def test_namespace_add(self, ts: TelescopeState, ns: TelescopeState) -> None:
        await ns.add('test_key', 1234.5)
        assert await ns['test_key'] == 1234.5
        assert await ts[ts.join('ns', 'test_key')] == 1234.5
        with pytest.raises(KeyError):
            await ts['test_key']

    async def test_delete(self, ts: TelescopeState) -> None:
        await ts.add('test_key', 1234.5)
        assert await ts.exists('test_key')
        await ts.delete('test_key')
        await ts.delete('test_key')
        assert not await ts.exists('test_key')

    async def test_namespace_delete(self, ts: TelescopeState, ns: TelescopeState) -> None:
        await ts.add('parent_key', 1234.5)
        await ns.add('child_key', 2345.6)
        await ns.delete('child_key')
        await ns.delete('parent_key')
        assert await ts.keys() == []

    async def test_clear(self, ts: TelescopeState) -> None:
        await ts.add('test_key', 1234.5)
        await ts.add('test_key_rt', 2345.6)
        await ts.clear()
        assert await ts.keys() == []

    async def test_get_default(self, ts: TelescopeState) -> None:
        assert await ts.get('foo') is None
        assert await ts.get('foo', 'bar') == 'bar'

    async def test_get_return_encoded(self, ts: TelescopeState) -> None:
        for immutable in [True, False]:
            x = np.array([(1.0, 2), (3.0, 4)], dtype=[('x', float), ('y', int)])
            await ts.add('test_key_rt', x, immutable=True)
            x_decoded = await ts.get('test_key_rt')
            assert (x_decoded == x).all()
            x_encoded = await ts.get('test_key_rt', return_encoded=True)
            assert x_encoded == encode_value(x)
            await ts.delete('test_key_rt')

    async def test_get_range_return_encoded(self, ts: TelescopeState) -> None:
        test_values = ['Test Value: {}'.format(x) for x in range(5)]
        for i, test_value in enumerate(test_values):
            await ts.add('test_key', test_value, i)
        stored_values = await ts.get_range('test_key', st=0)
        assert stored_values[2][0] == test_values[2]
        stored_values_pickled = await ts.get_range('test_key', st=0, return_encoded=True)
        assert stored_values_pickled[2][0] == encode_value(test_values[2])
        # check timestamp
        assert stored_values_pickled[2][1] == 2

    async def test_immutable(self, ts: TelescopeState) -> None:
        await ts.add('test_immutable', 1234.5, immutable=True)
        with pytest.raises(ImmutableKeyError):
            await ts.add('test_immutable', 1234.5)
        with pytest.raises(ImmutableKeyError):
            await ts.add('test_immutable', 5432.1, immutable=True)
        with pytest.raises(ImmutableKeyError):
            await ts.set_indexed('test_immutable', 1234.5, 1234.5)

    async def test_immutable_same_value(self, ts: TelescopeState) -> None:
        await ts.add('test_immutable', 1234.5, immutable=True)
        await ts.add('test_mutable', 1234.5)
        with pytest.raises(ImmutableKeyError):
            await ts.add('test_mutable', 2345.6, immutable=True)

    async def test_immutable_same_value_str(self, ts: TelescopeState) -> None:
        with mock.patch('katsdptelstate.encoding._allow_pickle', True), \
                mock.patch('katsdptelstate.encoding._warn_on_pickle', False):
            await ts.add('test_bytes', b'caf\xc3\xa9', immutable=True)
            await ts.add('test_bytes', b'caf\xc3\xa9', immutable=True)
            await ts.add('test_bytes', 'café', immutable=True)
            with pytest.raises(ImmutableKeyError):
                await ts.add('test_bytes', b'cafe', immutable=True)
            with pytest.raises(ImmutableKeyError):
                await ts.add('test_bytes', 'cafe', immutable=True)
            await ts.add('test_unicode', 'ümlaut', immutable=True)
            await ts.add('test_unicode', 'ümlaut', immutable=True)
            await ts.add('test_unicode', b'\xc3\xbcmlaut', immutable=True)
            with pytest.raises(ImmutableKeyError):
                await ts.add('test_unicode', b'umlaut', immutable=True)
            with pytest.raises(ImmutableKeyError):
                await ts.add('test_unicode', 'umlaut', immutable=True)
            # Test with a binary string that isn't valid UTF-8
            await ts.add('test_binary', b'\x00\xff', immutable=True)
            await ts.add('test_binary', b'\x00\xff', immutable=True)
            # Test Python 2/3 interop by directly injecting the pickled values
            await ts.backend.set_immutable(b'test_2', b"S'hello'\np1\n.")
            await ts.backend.set_immutable(b'test_3', b'Vhello\np0\n.')
            await ts.add('test_2', 'hello', immutable=True)
            await ts.add('test_3', 'hello', immutable=True)
            # Test handling of the case where the old value cannot be decoded
            # Empty string is never valid encoding
            await ts.backend.set_immutable(b'test_failed_decode', b'')
            with pytest.raises(ImmutableKeyError, match='failed to decode the previous value'):
                await ts.add('test_failed_decode', '', immutable=True)

    async def test_immutable_none(self, ts: TelescopeState) -> None:
        await ts.add('test_none', None, immutable=True)
        assert await ts.get('test_none') is None
        assert await ts.get('test_none', 'not_none') is None
        assert await ts['test_none'] is None

    async def test_immutable_wrong_type(self, ts: TelescopeState) -> None:
        await ts.add('test_mutable', 5)
        await ts.add('test_indexed', 5, 5)
        with pytest.raises(ImmutableKeyError):
            await ts.add('test_mutable', 5, immutable=True)
        with pytest.raises(ImmutableKeyError):
            await ts.add('test_indexed', 5, immutable=True)

    async def test_namespace_immutable(self, ts: TelescopeState, ns: TelescopeState) -> None:
        await ts.add('parent_immutable', 1234.5, immutable=True)
        await ns.add('child_immutable', 2345.5, immutable=True)
        with pytest.raises(KeyError):
            await ts['child_immutable']
        assert await ns.get('child_immutable') == 2345.5
        assert await ns.get('parent_immutable') == 1234.5

    async def test_key_type(self, ts: TelescopeState, ns: TelescopeState) -> None:
        await ts.add('parent_immutable', 1, immutable=True)
        await ns.add('child_immutable', 2, immutable=True)
        await ts.add('parent', 3)
        await ns.add('child', 4)
        await ts.set_indexed('parent_indexed', 'a', 1)
        await ns.set_indexed('child_indexed', 'b', 2)

        assert await ts.key_type('parent_immutable') == KeyType.IMMUTABLE
        assert await ts.key_type('parent') == KeyType.MUTABLE
        assert await ts.key_type('parent_indexed') == KeyType.INDEXED
        assert await ts.key_type('child_immutable') is None
        assert await ts.key_type('child') is None
        assert await ts.key_type('not_a_key') is None

        assert await ns.key_type('parent_immutable') == KeyType.IMMUTABLE
        assert await ns.key_type('parent') == KeyType.MUTABLE
        assert await ns.key_type('parent_indexed') == KeyType.INDEXED
        assert await ns.key_type('child_immutable') == KeyType.IMMUTABLE
        assert await ns.key_type('child') == KeyType.MUTABLE
        assert await ns.key_type('child_indexed') == KeyType.INDEXED
        assert await ns.key_type('not_a_key') is None

    async def test_keys(self, ts: TelescopeState, ns: TelescopeState) -> None:
        await ts.add('key1', 'a')
        await ns.add('key2', 'b')
        await ns.add(b'key2', 'c')
        await ts.add(b'immutable', 'd', immutable=True)
        assert await ts.keys() == ['immutable', 'key1', 'ns_key2']
        assert await ts.keys('ns_*') == ['ns_key2']

    async def test_complex_store(self, ts: TelescopeState) -> None:
        x = np.array([(1.0, 2), (3.0, 4)], dtype=[('x', float), ('y', int)])
        await ts.add('test_key', x)
        assert (await ts['test_key'] == x).all()

    async def test_exists(self, ts: TelescopeState) -> None:
        await ts.add('test_key', 1234.5)
        assert await ts.exists('test_key')
        assert not await ts.exists('nonexistent_test_key')

    async def test_time_range(self, ts: TelescopeState) -> None:
        await ts.delete('test_key')
        await ts.add('test_key', 8192, 1)
        await ts.add('test_key', 16384, 2)
        await ts.add('test_key', 4096, 3)
        await ts.add('test_key', 2048, 4)
        await ts.add('test_immutable', 12345, immutable=True)
        assert [(2048, 4)] == await ts.get_range('test_key')
        assert [(16384, 2)] == await ts.get_range('test_key', et=3)
        assert [(8192, 1), (16384, 2), (4096, 3)] == \
            await ts.get_range('test_key', st=2, et=4, include_previous=True)
        assert [(8192, 1), (16384, 2), (4096, 3), (2048, 4)] == await ts.get_range('test_key', st=0)
        assert [(8192, 1), (16384, 2), (4096, 3)] == await ts.get_range('test_key', st=0, et=3.5)
        assert [(8192, 1)] == await ts.get_range('test_key', st=-1, et=1.5)
        assert [(16384, 2), (4096, 3), (2048, 4)] == await ts.get_range('test_key', st=2)
        assert [(8192, 1)] == await ts.get_range('test_key', et=1.5)
        assert [] == await ts.get_range('test_key', 3.5, 1.5)
        assert [] == await ts.get_range('test_key', et=-0.)
        assert [(8192, 1), (16384, 2), (4096, 3), (2048, 4)] == \
            await ts.get_range('test_key', st=1.5, include_previous=True)
        assert [(2048, 4)] == await ts.get_range('test_key', st=5, et=6, include_previous=True)
        assert [(8192, 1), (16384, 2), (4096, 3)] == \
            await ts.get_range('test_key', st=2, et=4, include_previous=True)
        with pytest.raises(KeyError):
            await ts.get_range('not_a_key')
        with pytest.raises(ImmutableKeyError):
            await ts.get_range('test_immutable')

    async def test_time_range_include_end(self, ts: TelescopeState) -> None:
        await ts.add('test_key', 1234, 0)
        await ts.add('test_key', 8192, 1)
        await ts.add('test_key', 16384, 2)
        await ts.add('test_key', 4096, 3)
        await ts.add('test_key', 2048, 4)
        assert [] == await ts.get_range('test_key', st=1.5, et=1.5, include_end=True)
        assert [(1234, 0)] == await ts.get_range('test_key', st=0.0, et=0.0, include_end=True)
        assert [(1234, 0)] == await ts.get_range('test_key', st=0.0, et=-0.0, include_end=True)
        assert [(4096, 3), (2048, 4)] == \
            await ts.get_range('test_key', st=3, et=4, include_end=True)
        # include_previous tests
        assert [(8192, 1), (16384, 2)] == await ts.get_range('test_key', et=2, include_end=True)
        assert [(8192, 1), (16384, 2), (4096, 3)] == \
            await ts.get_range('test_key', st=2, et=3, include_previous=True, include_end=True)

    async def test_add_duplicate(self, ts: TelescopeState) -> None:
        await ts.add('test_key', 'value', 1234.5)
        await ts.add('test_key', 'value', 1234.5)
        assert [('value', 1234.5)] == await ts.get_range('test_key', st=0)

    async def test_wait_key_already_done_mutable(self, ts: TelescopeState) -> None:
        """Calling wait_key with a condition that is met must return (mutable version)."""
        await ts.add('test_key', 123)
        value, timestamp = (await ts.get_range('test_key'))[0]
        await ts.wait_key('test_key', lambda v, t: v == value and t == timestamp)

    async def test_wait_key_already_done_immutable(self, ts: TelescopeState) -> None:
        """Calling wait_key with a condition that is met must return (immutable version)."""
        await ts.add('test_key', 123, immutable=True)
        await ts.wait_key('test_key', lambda v, t: v == 123 and t is None)

    async def test_wait_key_already_done_indexed(self, ts: TelescopeState) -> None:
        """Calling wait_key with a condition that is met must return (indexed version)."""
        await ts.set_indexed('test_key', 'idx', 5)
        await ts.wait_key('test_key', lambda v, t: v == {'idx': 5} and t is None)

    async def test_wait_key_timeout(self, ts: TelescopeState) -> None:
        """wait_key must time out in the given time if the condition is not met"""
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.1):
                await ts.wait_key('test_key')
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.1):
                # Takes a different code path, even though equivalent
                await ts.wait_key('test_key', lambda value, ts: True)

    async def _set_key_immutable(self, ts: TelescopeState) -> None:
        await asyncio.sleep(0.1)
        await ts.set('test_key', 123)

    async def _set_key_mutable(self, ts: TelescopeState) -> None:
        await asyncio.sleep(0.1)
        await ts.add('test_key', 123, ts=1234567890)

    async def _set_key_indexed(self, ts: TelescopeState) -> None:
        await asyncio.sleep(0.1)
        await ts.set_indexed('test_key', 'idx', 123)

    async def test_wait_key_delayed(self, ts: TelescopeState) -> None:
        """wait_key must succeed with a timeout that does not expire before the condition is met."""
        for (set_key, value, timestamp) in [
                (self._set_key_mutable, 123, 1234567890),
                (self._set_key_immutable, 123, None),
                (self._set_key_indexed, {'idx': 123}, None)]:
            task = asyncio.ensure_future(set_key(ts))
            await ts.wait_key('test_key', lambda v, t: v == value and t == timestamp)
            assert value == await ts.get('test_key')
            await task
            await ts.delete('test_key')

    async def test_wait_key_delayed_unconditional(self, ts: TelescopeState) -> None:
        """wait_key must succeed when given a timeout that does not expire before key appears."""
        for set_key, value in [
                (self._set_key_mutable, 123),
                (self._set_key_immutable, 123),
                (self._set_key_indexed, {'idx': 123})]:
            task = asyncio.ensure_future(set_key(ts))
            await ts.wait_key('test_key')
            assert value == await ts['test_key']
            await task
            await ts.delete('test_key')

    async def test_wait_key_cancel(self, ts: TelescopeState) -> None:
        """wait_key must deal gracefully with cancellation."""
        task = asyncio.ensure_future(ts.wait_key('test_key'))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_wait_key_shadow(self, ts: TelescopeState, ns: TelescopeState) -> None:
        """updates to a shadowed qualified key must be ignored"""
        async def set_key(telstate):
            await asyncio.sleep(0.1)
            await telstate.add('test_key', True, immutable=True)

        ns2 = ns.view('ns2')
        # Put a non-matching key into a mid-level namespace
        await ns.add('test_key', False)
        # Put a matching key into a shadowed namespace after a delay
        task = asyncio.ensure_future(set_key(ts))
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.5):
                await ns2.wait_key('test_key', lambda value, ts: value is True)
        await task

        # Put a matching key into a non-shadowed namespace after a delay
        task = asyncio.ensure_future(set_key(ns2))
        async with async_timeout.timeout(5):
            await ns2.wait_key('test_key', lambda value, ts: value is True)
        await task

    async def test_wait_key_concurrent(self, ts: TelescopeState) -> None:
        task1 = asyncio.ensure_future(ts.wait_key('key1'))
        task2 = asyncio.ensure_future(ts.wait_key('key2'))
        await asyncio.sleep(0.1)
        assert not task1.done()
        assert not task2.done()
        await ts.add('key1', 1, immutable=True)
        await ts.add('key2', 2)
        await task1
        await task2
        # Make sure we unsubscribed. Unsubscriptions are done asynchronously,
        # so we need to sleep a bit to let them take place.
        if isinstance(ts.backend, RedisBackend):
            await asyncio.sleep(0.1)
            assert ts.backend._pubsub.channels == {_DUMMY_CHANNEL: None}

    async def test_wait_key_concurrent_same(self, ts: TelescopeState) -> None:
        task1 = asyncio.ensure_future(ts.wait_key('key'))
        task2 = asyncio.ensure_future(ts.wait_key('key'))
        await asyncio.sleep(0.1)
        assert not task1.done()
        assert not task2.done()
        await ts.add('key', 1)
        await task1
        await task2
        # Make sure we unsubscribed. Unsubscriptions are done asynchronously,
        # so we need to sleep a bit to let them take place.
        if isinstance(ts.backend, RedisBackend):
            await asyncio.sleep(0.1)
            assert ts.backend._pubsub.channels == {_DUMMY_CHANNEL: None}

    async def test_wait_indexed_already_done(self, ts: TelescopeState) -> None:
        await ts.set_indexed('test_key', 'sub_key', 5)
        await ts.wait_indexed('test_key', 'sub_key')
        await ts.wait_indexed('test_key', 'sub_key', lambda v: v == 5)

    async def test_wait_indexed_timeout(self, ts: TelescopeState) -> None:
        await ts.set_indexed('test_key', 'sub_key', 5)
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.05):
                await ts.wait_indexed('test_key', 'sub_key', lambda v: v == 4)
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.05):
                await ts.wait_indexed('test_key', 'another_key')
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.05):
                await ts.wait_indexed('not_present', 'sub_key')

    async def test_wait_indexed_delayed(self, ts: TelescopeState) -> None:
        async def set_key():
            await asyncio.sleep(0.05)
            await ts.set_indexed('test_key', 'foo', 1)
            await asyncio.sleep(0.05)
            await ts.set_indexed('test_key', 'bar', 2)
        for condition in [None, lambda value: value == 2]:
            await ts.delete('test_key')
            task = asyncio.ensure_future(set_key())
            async with async_timeout.timeout(2):
                await ts.wait_indexed('test_key', 'bar', condition)
            assert 2 == await ts.get_indexed('test_key', 'bar')
            await task

    async def test_wait_indexed_cancel(self, ts: TelescopeState) -> None:
        task = asyncio.ensure_future(ts.wait_indexed('test_key', 'sub_key'))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_wait_indexed_shadow(self, ts: TelescopeState, ns: TelescopeState) -> None:
        async def set_key(telstate):
            await asyncio.sleep(0.1)
            await telstate.set_indexed('test_key', 'sub_key', 1)

        # Shadow the key that will be set by set_key
        await ns.set_indexed('test_key', 'blah', 1)
        task = asyncio.ensure_future(set_key(ts))
        with pytest.raises(asyncio.TimeoutError):
            async with async_timeout.timeout(0.2):
                await ns.wait_indexed('test_key', 'sub_key')
        await task

    async def test_wait_indexed_wrong_type(self, ts: TelescopeState) -> None:
        async def set_key():
            await asyncio.sleep(0.1)
            await ts.add('test_key', 'value')

        task = asyncio.ensure_future(set_key())
        with pytest.raises(ImmutableKeyError):
            await ts.wait_indexed('test_key', 'value')
        await task
        # Different code-path when key was already set at the start
        with pytest.raises(ImmutableKeyError):
            await ts.wait_indexed('test_key', 'value')

    async def _test_mixed_unicode_bytes(
            self, ts: TelescopeState, ns: TelescopeState, key: AnyStr) -> None:
        await ts.clear()
        await ns.add(key, 'value', immutable=True)
        assert await ns.get(key) == 'value'
        assert await ns.exists(key)
        assert await ns.key_type(key) == KeyType.IMMUTABLE
        await ns.delete(key)
        await ns.add(key, 'value1', ts=1)
        assert await ns.get_range(key) == [('value1', 1.0)]
        await ns.wait_key(key)

    async def test_mixed_unicode_bytes(self, ts: TelescopeState) -> None:
        await self._test_mixed_unicode_bytes(ts, ts.view(b'ns'), 'test_key')
        await self._test_mixed_unicode_bytes(ts, ts.view('ns'), b'test_key')

    async def test_undecodable_bytes_in_key(self, ts: TelescopeState) -> None:
        """Gracefully handle non-UTF-8 bytes in keys."""
        key_b = b'undecodable\xff'
        await ts.backend.set_immutable(key_b, encode_value('hello'))
        key = [k for k in await ts.keys() if k.startswith('undecodable')][0]
        assert await ts.get(key) == 'hello'
        assert await ts.get(key_b) == 'hello'
        assert await ts.get(key_b[1:]) is None

    async def test_get_indexed(self, ts: TelescopeState) -> None:
        await ts.set_indexed('test_indexed', 'a', 1)
        await ts.set_indexed('test_indexed', (2, 3j), 2)
        assert await ts.get_indexed('test_indexed', 'a') == 1
        assert await ts.get_indexed('test_indexed', (2, 3j)) == 2
        assert await ts.get_indexed('test_indexed', 'missing') is None
        assert await ts.get_indexed('not_a_key', 'missing') is None
        assert await ts.get_indexed('test_indexed', 'missing', 'default') == 'default'
        assert await ts.get_indexed('not_a_key', 'missing', 'default') == 'default'
        assert await ts.get('test_indexed') == {'a': 1, (2, 3j): 2}

    async def test_indexed_return_encoded(self, ts: TelescopeState) -> None:
        await ts.set_indexed('test_indexed', 'a', 1)
        values = await ts.get('test_indexed', return_encoded=True)
        assert values == {encode_value('a', encoding=ENCODING_MSGPACK): encode_value(1)}
        assert await ts.get_indexed('test_indexed', 'a', return_encoded=True) == encode_value(1)

    async def test_set_indexed_immutable(self, ts: TelescopeState) -> None:
        await ts.set_indexed('test_indexed', 'a', 1)
        await ts.set_indexed('test_indexed', 'a', 1)  # Same value is okay
        with pytest.raises(ImmutableKeyError):
            await ts.set_indexed('test_indexed', 'a', 2)
        assert await ts.get_indexed('test_indexed', 'a') == 1

    async def test_set_indexed_unhashable(self, ts: TelescopeState) -> None:
        with pytest.raises(TypeError):
            await ts.set_indexed('test_indexed', ['list', 'is', 'not', 'hashable'], 0)

    async def test_indexed_wrong_type(self, ts: TelescopeState) -> None:
        await ts.set('test_immutable', 1)
        await ts.add('test_mutable', 2)
        with pytest.raises(ImmutableKeyError):
            await ts.set_indexed('test_immutable', 'a', 1)
        with pytest.raises(ImmutableKeyError):
            await ts.set_indexed('test_mutable', 'a', 1)
        with pytest.raises(ImmutableKeyError):
            await ts.get_indexed('test_immutable', 'a')
        with pytest.raises(ImmutableKeyError):
            await ts.get_indexed('test_mutable', 'a')

    async def test_namespace_indexed(self, ts: TelescopeState, ns: TelescopeState) -> None:
        await ts.set_indexed('test_indexed', 'a', 1)
        assert await ns.get('test_indexed') == {'a': 1}
        assert await ns.get_indexed('test_indexed', 'a') == 1
        await ns.set_indexed('test_indexed', 'b', 2)
        assert await ns.get('test_indexed') == {'b': 2}
        assert await ns.get_indexed('test_indexed', 'b') == 2
        # Namespace key must completely shadow root
        assert await ns.get_indexed('test_indexed', 'a') is None


class TestTelescopeStateRedis(TestTelescopeState):
    async def make_telescope_state(self) -> TelescopeState:
        client = fakeredis.aioredis.FakeRedis()
        return TelescopeState(RedisBackend(client))


class TestTelescopeStateRedisFromUrl(TestTelescopeState):
    async def make_telescope_state(self) -> TelescopeState:
        def make_fakeredis(cls, **kwargs):
            return fakeredis.aioredis.FakeRedis()

        with mock.patch(
                'aioredis.Redis.from_url',
                side_effect=make_fakeredis,
                autospec=True) as mock_redis:
            backend = await RedisBackend.from_url('redis://example.invalid/')
            mock_redis.assert_called_once_with(
                'redis://example.invalid/',
                db=None,
                socket_timeout=mock.ANY,
                health_check_interval=mock.ANY
            )
        return TelescopeState(backend)


class TestSharedMemoryBackend:
    def setup(self) -> None:
        self.async_backend = MemoryBackend()
        self.sync_backend = self.async_backend.to_sync()
        self.async_ts = TelescopeState(self.async_backend)
        self.sync_ts = katsdptelstate.TelescopeState(self.sync_backend)

    async def test_shared(self) -> None:
        self.sync_ts['foo'] = 'bar'
        assert await self.async_ts.get('foo') == 'bar'

    async def test_from_sync(self) -> None:
        ts = TelescopeState(MemoryBackend.from_sync(self.sync_backend))
        self.sync_ts['foo'] = 'bar'
        assert await ts.get('foo') == 'bar'
