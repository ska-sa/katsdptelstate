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

import threading
import time
from unittest import mock

import asynctest
import numpy as np
import fakeredis

from katsdptelstate import ImmutableKeyError, encode_value, KeyType, ENCODING_MSGPACK
from katsdptelstate.aio import TelescopeState
from katsdptelstate.aio.memory import MemoryBackend


class TestTelescopeState(asynctest.TestCase):
    async def setUp(self) -> None:
        self.ts = await self.make_telescope_state()
        self.ns = self.ts.view('ns')

    async def make_telescope_state(self) -> TelescopeState:
        return TelescopeState()

    def test_bad_construct(self) -> None:
        with self.assertRaises(ValueError):
            TelescopeState(self.ts.backend, base=self.ts)

    def test_namespace(self) -> None:
        self.assertEqual(self.ts.prefixes, ('',))
        self.assertEqual(self.ns.prefixes, ('ns_', ''))
        ns2 = self.ns.view(b'ns_child_grandchild')
        self.assertEqual(ns2.prefixes, ('ns_child_grandchild_', 'ns_', ''))
        self.assertEqual(ns2.root().prefixes, ('',))
        ns_excl = self.ns.view('exclusive', exclusive=True)
        self.assertEqual(ns_excl.prefixes, ('exclusive_',))

    async def test_basic_add(self) -> None:
        await self.ts.add('test_key', 1234.5)
        self.assertEqual(await self.ts['test_key'], 1234.5)

        await self.ts.delete('test_key')
        with self.assertRaises(KeyError):
            await self.ts['test_key']

    async def test_namespace_add(self) -> None:
        await self.ns.add('test_key', 1234.5)
        self.assertEqual(await self.ns['test_key'], 1234.5)
        self.assertEqual(await self.ts[self.ts.join('ns', 'test_key')], 1234.5)
        with self.assertRaises(KeyError):
            await self.ts['test_key']

    async def test_delete(self) -> None:
        await self.ts.add('test_key', 1234.5)
        self.assertTrue(await self.ts.exists('test_key'))
        await self.ts.delete('test_key')
        await self.ts.delete('test_key')
        self.assertFalse(await self.ts.exists('test_key'))

    async def test_namespace_delete(self) -> None:
        await self.ts.add('parent_key', 1234.5)
        await self.ns.add('child_key', 2345.6)
        await self.ns.delete('child_key')
        await self.ns.delete('parent_key')
        self.assertEqual(await self.ts.keys(), [])

    async def test_clear(self) -> None:
        await self.ts.add('test_key', 1234.5)
        await self.ts.add('test_key_rt', 2345.6)
        await self.ts.clear()
        self.assertEqual(await self.ts.keys(), [])

    async def test_get_default(self) -> None:
        self.assertIsNone(await self.ts.get('foo'))
        self.assertEqual(await self.ts.get('foo', 'bar'), 'bar')

    async def test_get_return_encoded(self) -> None:
        for immutable in [True, False]:
            x = np.array([(1.0, 2), (3.0, 4)], dtype=[('x', float), ('y', int)])
            await self.ts.add('test_key_rt', x, immutable=True)
            x_decoded = await self.ts.get('test_key_rt')
            self.assertTrue((x_decoded == x).all())
            x_encoded = await self.ts.get('test_key_rt', return_encoded=True)
            self.assertEqual(x_encoded, encode_value(x))
            await self.ts.delete('test_key_rt')

    async def test_get_range_return_encoded(self) -> None:
        test_values = ['Test Value: {}'.format(x) for x in range(5)]
        for i, test_value in enumerate(test_values):
            await self.ts.add('test_key', test_value, i)
        stored_values = await self.ts.get_range('test_key', st=0)
        self.assertEqual(stored_values[2][0], test_values[2])
        stored_values_pickled = await self.ts.get_range('test_key', st=0, return_encoded=True)
        self.assertEqual(stored_values_pickled[2][0], encode_value(test_values[2]))
        # check timestamp
        self.assertEqual(stored_values_pickled[2][1], 2)

    async def test_immutable(self) -> None:
        await self.ts.add('test_immutable', 1234.5, immutable=True)
        with self.assertRaises(ImmutableKeyError):
            await self.ts.add('test_immutable', 1234.5)
        with self.assertRaises(ImmutableKeyError):
            await self.ts.add('test_immutable', 5432.1, immutable=True)
        with self.assertRaises(ImmutableKeyError):
            await self.ts.set_indexed('test_immutable', 1234.5, 1234.5)

    async def test_immutable_same_value(self) -> None:
        await self.ts.add('test_immutable', 1234.5, immutable=True)
        await self.ts.add('test_mutable', 1234.5)
        with self.assertRaises(ImmutableKeyError):
            await self.ts.add('test_mutable', 2345.6, immutable=True)

    async def test_immutable_same_value_str(self) -> None:
        with mock.patch('katsdptelstate.encoding._allow_pickle', True), \
             mock.patch('katsdptelstate.encoding._warn_on_pickle', False):
            await self.ts.add('test_bytes', b'caf\xc3\xa9', immutable=True)
            await self.ts.add('test_bytes', b'caf\xc3\xa9', immutable=True)
            await self.ts.add('test_bytes', 'café', immutable=True)
            with self.assertRaises(ImmutableKeyError):
                await self.ts.add('test_bytes', b'cafe', immutable=True)
            with self.assertRaises(ImmutableKeyError):
                await self.ts.add('test_bytes', 'cafe', immutable=True)
            await self.ts.add('test_unicode', 'ümlaut', immutable=True)
            await self.ts.add('test_unicode', 'ümlaut', immutable=True)
            await self.ts.add('test_unicode', b'\xc3\xbcmlaut', immutable=True)
            with self.assertRaises(ImmutableKeyError):
                await self.ts.add('test_unicode', b'umlaut', immutable=True)
            with self.assertRaises(ImmutableKeyError):
                await self.ts.add('test_unicode', 'umlaut', immutable=True)
            # Test with a binary string that isn't valid UTF-8
            await self.ts.add('test_binary', b'\x00\xff', immutable=True)
            await self.ts.add('test_binary', b'\x00\xff', immutable=True)
            # Test Python 2/3 interop by directly injecting the pickled values
            await self.ts.backend.set_immutable(b'test_2', b"S'hello'\np1\n.")
            await self.ts.backend.set_immutable(b'test_3', b'Vhello\np0\n.')
            await self.ts.add('test_2', 'hello', immutable=True)
            await self.ts.add('test_3', 'hello', immutable=True)
            # Test handling of the case where the old value cannot be decoded
            # Empty string is never valid encoding
            await self.ts.backend.set_immutable(b'test_failed_decode', b'')
            with self.assertRaisesRegex(ImmutableKeyError, 'failed to decode the previous value'):
                await self.ts.add('test_failed_decode', '', immutable=True)

    async def test_immutable_none(self) -> None:
        await self.ts.add('test_none', None, immutable=True)
        self.assertIsNone(await self.ts.get('test_none'))
        self.assertIsNone(await self.ts.get('test_none', 'not_none'))
        self.assertIsNone(await self.ts['test_none'])

    async def test_immutable_wrong_type(self) -> None:
        await self.ts.add('test_mutable', 5)
        await self.ts.add('test_indexed', 5, 5)
        with self.assertRaises(ImmutableKeyError):
            await self.ts.add('test_mutable', 5, immutable=True)
        with self.assertRaises(ImmutableKeyError):
            await self.ts.add('test_indexed', 5, immutable=True)

    async def test_namespace_immutable(self) -> None:
        await self.ts.add('parent_immutable', 1234.5, immutable=True)
        await self.ns.add('child_immutable', 2345.5, immutable=True)
        with self.assertRaises(KeyError):
            await self.ts['child_immutable']
        self.assertEqual(await self.ns.get('child_immutable'), 2345.5)
        self.assertEqual(await self.ns.get('parent_immutable'), 1234.5)

    async def test_key_type(self) -> None:
        await self.ts.add('parent_immutable', 1, immutable=True)
        await self.ns.add('child_immutable', 2, immutable=True)
        await self.ts.add('parent', 3)
        await self.ns.add('child', 4)
        await self.ts.set_indexed('parent_indexed', 'a', 1)
        await self.ns.set_indexed('child_indexed', 'b', 2)

        self.assertEqual(await self.ts.key_type('parent_immutable'), KeyType.IMMUTABLE)
        self.assertEqual(await self.ts.key_type('parent'), KeyType.MUTABLE)
        self.assertEqual(await self.ts.key_type('parent_indexed'), KeyType.INDEXED)
        self.assertEqual(await self.ts.key_type('child_immutable'), None)
        self.assertEqual(await self.ts.key_type('child'), None)
        self.assertEqual(await self.ts.key_type('not_a_key'), None)

        self.assertEqual(await self.ns.key_type('parent_immutable'), KeyType.IMMUTABLE)
        self.assertEqual(await self.ns.key_type('parent'), KeyType.MUTABLE)
        self.assertEqual(await self.ns.key_type('parent_indexed'), KeyType.INDEXED)
        self.assertEqual(await self.ns.key_type('child_immutable'), KeyType.IMMUTABLE)
        self.assertEqual(await self.ns.key_type('child'), KeyType.MUTABLE)
        self.assertEqual(await self.ns.key_type('child_indexed'), KeyType.INDEXED)
        self.assertEqual(await self.ns.key_type('not_a_key'), None)

    async def test_keys(self) -> None:
        await self.ts.add('key1', 'a')
        await self.ns.add('key2', 'b')
        await self.ns.add(b'key2', 'c')
        await self.ts.add(b'immutable', 'd', immutable=True)
        self.assertEqual(await self.ts.keys(), ['immutable', 'key1', 'ns_key2'])
        self.assertEqual(await self.ts.keys('ns_*'), ['ns_key2'])

    async def test_complex_store(self) -> None:
        x = np.array([(1.0, 2), (3.0, 4)], dtype=[('x', float), ('y', int)])
        await self.ts.add('test_key', x)
        self.assertTrue((await self.ts['test_key'] == x).all())

    async def test_exists(self) -> None:
        await self.ts.add('test_key', 1234.5)
        self.assertTrue(await self.ts.exists('test_key'))
        self.assertFalse(await self.ts.exists('nonexistent_test_key'))

    async def test_time_range(self) -> None:
        await self.ts.delete('test_key')
        await self.ts.add('test_key', 8192, 1)
        await self.ts.add('test_key', 16384, 2)
        await self.ts.add('test_key', 4096, 3)
        await self.ts.add('test_key', 2048, 4)
        await self.ts.add('test_immutable', 12345, immutable=True)
        self.assertEqual([(2048, 4)], await self.ts.get_range('test_key'))
        self.assertEqual([(16384, 2)], await self.ts.get_range('test_key', et=3))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3)],
                         await self.ts.get_range('test_key', st=2, et=4, include_previous=True))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3), (2048, 4)],
                         await self.ts.get_range('test_key', st=0))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3)],
                         await self.ts.get_range('test_key', st=0, et=3.5))
        self.assertEqual([(8192, 1)], await self.ts.get_range('test_key', st=-1, et=1.5))
        self.assertEqual([(16384, 2), (4096, 3), (2048, 4)],
                         await self.ts.get_range('test_key', st=2))
        self.assertEqual([(8192, 1)], await self.ts.get_range('test_key', et=1.5))
        self.assertEqual([], await self.ts.get_range('test_key', 3.5, 1.5))
        self.assertEqual([], await self.ts.get_range('test_key', et=-0.))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3), (2048, 4)],
                         await self.ts.get_range('test_key', st=1.5, include_previous=True))
        self.assertEqual([(2048, 4)],
                         await self.ts.get_range('test_key', st=5, et=6, include_previous=True))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3)],
                         await self.ts.get_range('test_key', st=2, et=4, include_previous=True))
        with self.assertRaises(KeyError):
            await self.ts.get_range('not_a_key')
        with self.assertRaises(ImmutableKeyError):
            await self.ts.get_range('test_immutable')

    async def test_time_range_include_end(self) -> None:
        await self.ts.add('test_key', 1234, 0)
        await self.ts.add('test_key', 8192, 1)
        await self.ts.add('test_key', 16384, 2)
        await self.ts.add('test_key', 4096, 3)
        await self.ts.add('test_key', 2048, 4)
        self.assertEqual([], await self.ts.get_range('test_key', st=1.5, et=1.5, include_end=True))
        self.assertEqual([(1234, 0)],
                         await self.ts.get_range('test_key', st=0.0, et=0.0, include_end=True))
        self.assertEqual([(1234, 0)],
                         await self.ts.get_range('test_key', st=0.0, et=-0.0, include_end=True))
        self.assertEqual([(4096, 3), (2048, 4)],
                         await self.ts.get_range('test_key', st=3, et=4, include_end=True))
        # include_previous tests
        self.assertEqual([(8192, 1), (16384, 2)],
                         await self.ts.get_range('test_key', et=2, include_end=True))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3)],
                         await self.ts.get_range('test_key', st=2, et=3,
                                                 include_previous=True, include_end=True))

    async def test_add_duplicate(self) -> None:
        await self.ts.add('test_key', 'value', 1234.5)
        await self.ts.add('test_key', 'value', 1234.5)
        self.assertEqual([('value', 1234.5)], await self.ts.get_range('test_key', st=0))

    def test_wait_key_already_done_mutable(self) -> None:
        """Calling wait_key with a condition that is met must return (mutable version)."""
        self.ts.add('test_key', 123)
        value, timestamp = self.ts.get_range('test_key')[0]
        self.ts.wait_key('test_key', lambda v, t: v == value and t == timestamp)

    def test_wait_key_already_done_immutable(self) -> None:
        """Calling wait_key with a condition that is met must return (immutable version)."""
        self.ts.add('test_key', 123, immutable=True)
        self.ts.wait_key('test_key', lambda v, t: v == self.ts['test_key'] and t is None)

    def test_wait_key_already_done_indexed(self) -> None:
        """Calling wait_key with a condition that is met must return (indexed version)."""
        self.ts.set_indexed('test_key', 'idx', 5)
        self.ts.wait_key('test_key', lambda v, t: v == {'idx': 5} and t is None)

    def test_wait_key_timeout(self) -> None:
        """wait_key must time out in the given time if the condition is not met"""
        with self.assertRaises(TimeoutError):
            self.ts.wait_key('test_key', timeout=0.1)
        with self.assertRaises(TimeoutError):
            # Takes a different code path, even though equivalent
            self.ts.wait_key('test_key', lambda value, ts: True, timeout=0.1)

    async def _set_key_immutable(self) -> None:
        await asyncio.sleep(0.1)
        await self.ts.set('test_key', 123)

    async def _set_key_mutable(self) -> None:
        await asyncio.sleep(0.1)
        await self.ts.add('test_key', 123, ts=1234567890)

    async def _set_key_indexed(self) -> None:
        await asyncio.sleep(0.1)
        await self.ts.set_indexed('test_key', 'idx', 123)

    def test_wait_key_delayed(self) -> None:
        """wait_key must succeed with a timeout that does not expire before the condition is met."""
        for (set_key, value, timestamp) in [
                (self._set_key_mutable, 123, 1234567890),
                (self._set_key_immutable, 123, None),
                (self._set_key_indexed, {'idx': 123}, None)]:
            thread = threading.Thread(target=set_key)
            thread.start()
            self.ts.wait_key('test_key', lambda v, t: v == value and t == timestamp, timeout=2)
            self.assertEqual(value, self.ts.get('test_key'))
            thread.join()
            self.ts.delete('test_key')

    def test_wait_key_delayed_unconditional(self) -> None:
        """wait_key must succeed when given a timeout that does not expire before key appears."""
        for set_key, value in [
                (self._set_key_mutable, 123),
                (self._set_key_immutable, 123),
                (self._set_key_indexed, {'idx': 123})]:
            thread = threading.Thread(target=set_key)
            thread.start()
            self.ts.wait_key('test_key', timeout=2)
            self.assertEqual(value, self.ts['test_key'])
            thread.join()
            self.ts.delete('test_key')

    def test_wait_key_already_cancelled(self) -> None:
        """wait_key must raise :exc:`CancelledError` if the `cancel_future` is already done."""
        future = mock.MagicMock()
        future.done.return_value = True
        with self.assertRaises(CancelledError):
            self.ts.wait_key('test_key', cancel_future=future)

    def test_wait_key_already_done_and_cancelled(self) -> None:
        """wait_key is successful if both the condition and the cancellation are done on entry."""
        future = mock.MagicMock()
        future.done.return_value = True
        self.ts.add('test_key', 123)
        self.ts.wait_key('test_key', lambda value, ts: value == 123, cancel_future=future)

    def test_wait_key_cancel(self) -> None:
        """wait_key must return when cancelled."""
        def cancel():
            time.sleep(0.1)
            future.done.return_value = True
        future = mock.MagicMock()
        future.done.return_value = False
        thread = threading.Thread(target=cancel)
        thread.start()
        with self.assertRaises(CancelledError):
            self.ts.wait_key('test_key', cancel_future=future)

    def test_wait_key_shadow(self) -> None:
        """updates to a shadowed qualified key must be ignored"""
        def set_key(telstate):
            time.sleep(0.1)
            telstate.add('test_key', True, immutable=True)

        ns2 = self.ns.view('ns2')
        # Put a non-matching key into a mid-level namespace
        self.ns.add('test_key', False)
        # Put a matching key into a shadowed namespace after a delay
        thread = threading.Thread(target=set_key, args=(self.ts,))
        thread.start()
        with self.assertRaises(TimeoutError):
            ns2.wait_key('test_key', lambda value, ts: value is True, timeout=0.5)
        thread.join()

        # Put a matching key into a non-shadowed namespace after a delay
        thread = threading.Thread(target=set_key, args=(ns2,))
        thread.start()
        ns2.wait_key('test_key', lambda value, ts: value is True, timeout=0.5)
        thread.join()

    def test_wait_indexed_already_done(self) -> None:
        self.ts.set_indexed('test_key', 'sub_key', 5)
        self.ts.wait_indexed('test_key', 'sub_key')
        self.ts.wait_indexed('test_key', 'sub_key', lambda v: v == 5)

    def test_wait_indexed_timeout(self) -> None:
        self.ts.set_indexed('test_key', 'sub_key', 5)
        with self.assertRaises(TimeoutError):
            self.ts.wait_indexed('test_key', 'sub_key', lambda v: v == 4, timeout=0.05)
        with self.assertRaises(TimeoutError):
            self.ts.wait_indexed('test_key', 'another_key', timeout=0.05)
        with self.assertRaises(TimeoutError):
            self.ts.wait_indexed('not_present', 'sub_key', timeout=0.05)

    def test_wait_indexed_delayed(self) -> None:
        def set_key():
            self.ts.set_indexed('test_key', 'foo', 1)
            time.sleep(0.05)
            self.ts.set_indexed('test_key', 'bar', 2)
        for condition in [None, lambda value: value == 2]:
            self.ts.delete('test_key')
            thread = threading.Thread(target=set_key)
            thread.start()
            self.ts.wait_indexed('test_key', 'bar', condition, timeout=2)
            self.assertEqual(2, self.ts.get_indexed('test_key', 'bar'))
            thread.join()

    def test_wait_indexed_already_cancelled(self) -> None:
        future = mock.MagicMock()
        future.done.return_value = True
        with self.assertRaises(CancelledError):
            self.ts.wait_indexed('test_key', 'foo', cancel_future=future)

    def test_wait_indexed_already_done_and_cancelled(self) -> None:
        future = mock.MagicMock()
        future.done.return_value = True
        self.ts.set_indexed('test_key', 'sub_key', 1)
        self.ts.wait_indexed('test_key', 'sub_key', lambda value: value == 1, cancel_future=future)

    def test_wait_indexed_cancel(self) -> None:
        def cancel():
            time.sleep(0.1)
            future.done.return_value = True
        future = mock.MagicMock()
        future.done.return_value = False
        thread = threading.Thread(target=cancel)
        thread.start()
        with self.assertRaises(CancelledError):
            self.ts.wait_indexed('test_key', 'sub_key', cancel_future=future)

    def test_wait_indexed_shadow(self) -> None:
        def set_key(telstate):
            time.sleep(0.1)
            telstate.set_indexed('test_key', 'sub_key', 1)

        # Shadow the key that will be set by set_key
        self.ns.set_indexed('test_key', 'blah', 1)
        thread = threading.Thread(target=set_key, args=(self.ts,))
        thread.start()
        with self.assertRaises(TimeoutError):
            self.ns.wait_indexed('test_key', 'sub_key', timeout=0.2)

    def test_wait_indexed_wrong_type(self) -> None:
        def set_key():
            time.sleep(0.1)
            self.ts.add('test_key', 'value')

        thread = threading.Thread(target=set_key)
        thread.start()
        with self.assertRaises(ImmutableKeyError):
            self.ts.wait_indexed('test_key', 'value')
        thread.join()
        # Different code-path when key was already set at the start
        with self.assertRaises(ImmutableKeyError):
            self.ts.wait_indexed('test_key', 'value')

    async def _test_mixed_unicode_bytes(self, ns, key) -> None:
        await self.ts.clear()
        await ns.add(key, 'value', immutable=True)
        self.assertEqual(await ns.get(key), 'value')
        self.assertTrue(await ns.exists(key))
        self.assertEqual(await ns.key_type(key), KeyType.IMMUTABLE)
        await ns.delete(key)
        await ns.add(key, 'value1', ts=1)
        self.assertEqual(await ns.get_range(key), [('value1', 1.0)])
        await ns.wait_key(key)

    async def test_mixed_unicode_bytes(self) -> None:
        await self._test_mixed_unicode_bytes(self.ts.view(b'ns'), 'test_key')
        await self._test_mixed_unicode_bytes(self.ts.view('ns'), b'test_key')

    async def test_undecodable_bytes_in_key(self) -> None:
        """Gracefully handle non-UTF-8 bytes in keys."""
        key_b = b'undecodable\xff'
        await self.ts.backend.set_immutable(key_b, encode_value('hello'))
        key = [k for k in await self.ts.keys() if k.startswith('undecodable')][0]
        self.assertEqual(await self.ts.get(key), 'hello')
        self.assertEqual(await self.ts.get(key_b), 'hello')
        self.assertEqual(await self.ts.get(key_b[1:]), None)

    async def test_get_indexed(self) -> None:
        await self.ts.set_indexed('test_indexed', 'a', 1)
        await self.ts.set_indexed('test_indexed', (2, 3j), 2)
        self.assertEqual(await self.ts.get_indexed('test_indexed', 'a'), 1)
        self.assertEqual(await self.ts.get_indexed('test_indexed', (2, 3j)), 2)
        self.assertIsNone(await self.ts.get_indexed('test_indexed', 'missing'))
        self.assertIsNone(await self.ts.get_indexed('not_a_key', 'missing'))
        self.assertEqual(await self.ts.get_indexed('test_indexed', 'missing', 'default'), 'default')
        self.assertEqual(await self.ts.get_indexed('not_a_key', 'missing', 'default'), 'default')
        self.assertEqual(await self.ts.get('test_indexed'), {'a': 1, (2, 3j): 2})

    async def test_indexed_return_encoded(self) -> None:
        await self.ts.set_indexed('test_indexed', 'a', 1)
        values = await self.ts.get('test_indexed', return_encoded=True)
        self.assertEqual(values, {
            encode_value('a', encoding=ENCODING_MSGPACK): encode_value(1)
        })
        self.assertEqual(await self.ts.get_indexed('test_indexed', 'a', return_encoded=True),
                         encode_value(1))

    async def test_set_indexed_immutable(self) -> None:
        await self.ts.set_indexed('test_indexed', 'a', 1)
        await self.ts.set_indexed('test_indexed', 'a', 1)  # Same value is okay
        with self.assertRaises(ImmutableKeyError):
            await self.ts.set_indexed('test_indexed', 'a', 2)
        self.assertEqual(await self.ts.get_indexed('test_indexed', 'a'), 1)

    async def test_set_indexed_unhashable(self) -> None:
        with self.assertRaises(TypeError):
            await self.ts.set_indexed('test_indexed', ['list', 'is', 'not', 'hashable'], 0)

    async def test_indexed_wrong_type(self) -> None:
        await self.ts.set('test_immutable', 1)
        await self.ts.add('test_mutable', 2)
        with self.assertRaises(ImmutableKeyError):
            await self.ts.set_indexed('test_immutable', 'a', 1)
        with self.assertRaises(ImmutableKeyError):
            await self.ts.set_indexed('test_mutable', 'a', 1)
        with self.assertRaises(ImmutableKeyError):
            await self.ts.get_indexed('test_immutable', 'a')
        with self.assertRaises(ImmutableKeyError):
            await self.ts.get_indexed('test_mutable', 'a')

    async def test_namespace_indexed(self) -> None:
        await self.ts.set_indexed('test_indexed', 'a', 1)
        self.assertEqual(await self.ns.get('test_indexed'), {'a': 1})
        self.assertEqual(await self.ns.get_indexed('test_indexed', 'a'), 1)
        await self.ns.set_indexed('test_indexed', 'b', 2)
        self.assertEqual(await self.ns.get('test_indexed'), {'b': 2})
        self.assertEqual(await self.ns.get_indexed('test_indexed', 'b'), 2)
        # Namespace key must completely shadow root
        self.assertIsNone(await self.ns.get_indexed('test_indexed', 'a'))


class TestTelescopeStateRedis(TestTelescopeState):
    async def make_telescope_state(self) -> TelescopeState:
        client = await fakeredis.aioredis.create_redis_pool()
        return TelescopeState(RedisBackend(client))
