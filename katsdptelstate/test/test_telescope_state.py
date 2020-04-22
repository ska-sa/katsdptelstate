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

"""Tests for the sdp telescope state client."""

import threading
import time
import unittest
from unittest import mock

import numpy as np
import fakeredis

from katsdptelstate import (TelescopeState, InvalidKeyError, ImmutableKeyError,
                            TimeoutError, CancelledError, encode_value, KeyType)
from katsdptelstate.memory import MemoryBackend


class TestTelescopeState(unittest.TestCase):
    def setUp(self):
        self.ts = self.make_telescope_state()
        self.ns = self.ts.view('ns')

    def make_telescope_state(self):
        return TelescopeState()

    def test_bad_construct(self):
        with self.assertRaises(ValueError):
            TelescopeState('redis.example.com:7148', base=self.ts)
        with self.assertRaises(ValueError):
            TelescopeState('', 1, base=self.ts)
        with self.assertRaises(ValueError):
            TelescopeState(MemoryBackend(), 1)
        with self.assertRaises(ValueError):
            TelescopeState(db=1)

    def test_namespace(self):
        self.assertEqual(self.ts.prefixes, ('',))
        self.assertEqual(self.ns.prefixes, ('ns_', ''))
        ns2 = self.ns.view(b'ns_child_grandchild')
        self.assertEqual(ns2.prefixes, ('ns_child_grandchild_', 'ns_', ''))
        self.assertEqual(ns2.root().prefixes, ('',))
        ns_excl = self.ns.view('exclusive', exclusive=True)
        self.assertEqual(ns_excl.prefixes, ('exclusive_',))

    def test_basic_add(self):
        self.ts.add('test_key', 1234.5)
        self.assertEqual(self.ts.test_key, 1234.5)
        self.assertEqual(self.ts['test_key'], 1234.5)

        self.ts.delete('test_key')
        with self.assertRaises(AttributeError):
            self.ts.test_key

    def test_namespace_add(self):
        self.ns.add('test_key', 1234.5)
        self.assertEqual(self.ns.test_key, 1234.5)
        self.assertEqual(self.ns['test_key'], 1234.5)
        self.assertEqual(self.ts[self.ts.join('ns', 'test_key')], 1234.5)
        with self.assertRaises(KeyError):
            self.ts['test_key']

    def test_method_protection(self):
        with self.assertRaises(InvalidKeyError):
            self.ts.add('get', 1234.5)
        with self.assertRaises(InvalidKeyError):
            self.ts['get'] = 1234.5
        with self.assertRaises(InvalidKeyError):
            self.ts.set_indexed('get', 'sub', 1)

    def test_delete(self):
        self.ts.add('test_key', 1234.5)
        self.assertIn('test_key', self.ts)
        self.ts.delete('test_key')
        self.ts.delete('test_key')
        self.assertNotIn('test_key', self.ts)

    def test_namespace_delete(self):
        self.ts.add('parent_key', 1234.5)
        self.ns.add('child_key', 2345.6)
        self.ns.delete('child_key')
        self.ns.delete('parent_key')
        self.assertEqual(self.ts.keys(), [])

    def test_clear(self):
        self.ts.add('test_key', 1234.5)
        self.ts.add('test_key_rt', 2345.6)
        self.ts.clear()
        self.assertEqual(self.ts.keys(), [])

    def test_get_default(self):
        self.assertIsNone(self.ts.get('foo'))
        self.assertEqual(self.ts.get('foo', 'bar'), 'bar')

    def test_get_return_encoded(self):
        for immutable in [True, False]:
            x = np.array([(1.0, 2), (3.0, 4)], dtype=[('x', float), ('y', int)])
            self.ts.add('test_key_rt', x, immutable=True)
            x_decoded = self.ts.get('test_key_rt')
            self.assertTrue((x_decoded == x).all())
            x_encoded = self.ts.get('test_key_rt', return_encoded=True)
            self.assertEqual(x_encoded, encode_value(x))
            self.ts.delete('test_key_rt')

    def test_get_range_return_encoded(self):
        test_values = ['Test Value: {}'.format(x) for x in range(5)]
        for i, test_value in enumerate(test_values):
            self.ts.add('test_key', test_value, i)
        stored_values = self.ts.get_range('test_key', st=0)
        self.assertEqual(stored_values[2][0], test_values[2])
        stored_values_pickled = self.ts.get_range('test_key', st=0, return_encoded=True)
        self.assertEqual(stored_values_pickled[2][0], encode_value(test_values[2]))
        # check timestamp
        self.assertEqual(stored_values_pickled[2][1], 2)

    def test_immutable(self):
        self.ts.add('test_immutable', 1234.5, immutable=True)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_immutable', 1234.5)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_immutable', 5432.1, immutable=True)
        with self.assertRaises(ImmutableKeyError):
            self.ts.set_indexed('test_immutable', 1234.5, 1234.5)

    def test_immutable_same_value(self):
        self.ts.add('test_immutable', 1234.5, immutable=True)
        self.ts.add('test_mutable', 1234.5)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_mutable', 2345.6, immutable=True)

    @mock.patch('katsdptelstate.encoding._allow_pickle', True)
    @mock.patch('katsdptelstate.encoding._warn_on_pickle', False)
    def test_immutable_same_value_str(self):
        self.ts.add('test_bytes', b'caf\xc3\xa9', immutable=True)
        self.ts.add('test_bytes', b'caf\xc3\xa9', immutable=True)
        self.ts.add('test_bytes', 'café', immutable=True)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_bytes', b'cafe', immutable=True)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_bytes', 'cafe', immutable=True)
        self.ts.add('test_unicode', 'ümlaut', immutable=True)
        self.ts.add('test_unicode', 'ümlaut', immutable=True)
        self.ts.add('test_unicode', b'\xc3\xbcmlaut', immutable=True)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_unicode', b'umlaut', immutable=True)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_unicode', 'umlaut', immutable=True)
        # Test with a binary string that isn't valid UTF-8
        self.ts.add('test_binary', b'\x00\xff', immutable=True)
        self.ts.add('test_binary', b'\x00\xff', immutable=True)
        # Test Python 2/3 interop by directly injecting the pickled values
        self.ts.backend.set_immutable(b'test_2', b"S'hello'\np1\n.")
        self.ts.backend.set_immutable(b'test_3', b'Vhello\np0\n.')
        self.ts.add('test_2', 'hello', immutable=True)
        self.ts.add('test_3', 'hello', immutable=True)
        # Test handling of the case where the old value cannot be decoded
        # Empty string is never valid encoding
        self.ts.backend.set_immutable(b'test_failed_decode', b'')
        with self.assertRaisesRegex(ImmutableKeyError, 'failed to decode the previous value'):
            self.ts.add('test_failed_decode', '', immutable=True)

    def test_immutable_none(self):
        self.ts.add('test_none', None, immutable=True)
        self.assertIsNone(self.ts.get('test_none'))
        self.assertIsNone(self.ts.get('test_none', 'not_none'))
        self.assertIsNone(self.ts.test_none)
        self.assertIsNone(self.ts['test_none'])

    def test_immutable_wrong_type(self):
        self.ts.add('test_mutable', 5)
        self.ts.add('test_indexed', 5, 5)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_mutable', 5, immutable=True)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_indexed', 5, immutable=True)

    def test_namespace_immutable(self):
        self.ts.add('parent_immutable', 1234.5, immutable=True)
        self.ns.add('child_immutable', 2345.5, immutable=True)
        with self.assertRaises(KeyError):
            self.ts['child_immutable']
        self.assertEqual(self.ns.get('child_immutable'), 2345.5)
        self.assertEqual(self.ns.get('parent_immutable'), 1234.5)

    def test_key_type(self):
        self.ts.add('parent_immutable', 1, immutable=True)
        self.ns.add('child_immutable', 2, immutable=True)
        self.ts.add('parent', 3)
        self.ns.add('child', 4)
        self.ts.set_indexed('parent_indexed', 'a', 1)
        self.ns.set_indexed('child_indexed', 'b', 2)

        self.assertEqual(self.ts.key_type('parent_immutable'), KeyType.IMMUTABLE)
        self.assertEqual(self.ts.key_type('parent'), KeyType.MUTABLE)
        self.assertEqual(self.ts.key_type('parent_indexed'), KeyType.INDEXED)
        self.assertEqual(self.ts.key_type('child_immutable'), None)
        self.assertEqual(self.ts.key_type('child'), None)
        self.assertEqual(self.ts.key_type('not_a_key'), None)

        self.assertEqual(self.ns.key_type('parent_immutable'), KeyType.IMMUTABLE)
        self.assertEqual(self.ns.key_type('parent'), KeyType.MUTABLE)
        self.assertEqual(self.ns.key_type('parent_indexed'), KeyType.INDEXED)
        self.assertEqual(self.ns.key_type('child_immutable'), KeyType.IMMUTABLE)
        self.assertEqual(self.ns.key_type('child'), KeyType.MUTABLE)
        self.assertEqual(self.ns.key_type('child_indexed'), KeyType.INDEXED)
        self.assertEqual(self.ns.key_type('not_a_key'), None)

    def test_keys(self):
        self.ts.add('key1', 'a')
        self.ns.add('key2', 'b')
        self.ns.add(b'key2', 'c')
        self.ts.add(b'immutable', 'd', immutable=True)
        self.assertEqual(self.ts.keys(), ['immutable', 'key1', 'ns_key2'])
        self.assertEqual(self.ts.keys('ns_*'), ['ns_key2'])

    def test_complex_store(self):
        x = np.array([(1.0, 2), (3.0, 4)], dtype=[('x', float), ('y', int)])
        self.ts.add('test_key', x)
        self.assertTrue((self.ts.test_key == x).all())

    def test_contains(self):
        self.ts.add('test_key', 1234.5)
        self.assertTrue('test_key' in self.ts)
        self.assertFalse('nonexistent_test_key' in self.ts)

    def test_setattr(self):
        self.ts.test_key = 'foo'
        self.assertEqual(self.ts['test_key'], 'foo')
        self.assertEqual(self.ts.key_type('test_key'), KeyType.IMMUTABLE)
        with self.assertRaises(AttributeError):
            self.ts.root = 'root is a method'
        self.ts._internal = 'bar'
        self.assertFalse('_internal' in self.ts)

    def test_setitem(self):
        self.ts['test_key'] = 'foo'
        self.assertEqual(self.ts['test_key'], 'foo')
        self.assertEqual(self.ts.key_type('test_key'), KeyType.IMMUTABLE)

    def test_time_range(self):
        self.ts.delete('test_key')
        self.ts.add('test_key', 8192, 1)
        self.ts.add('test_key', 16384, 2)
        self.ts.add('test_key', 4096, 3)
        self.ts.add('test_key', 2048, 4)
        self.ts.add('test_immutable', 12345, immutable=True)
        self.assertEqual([(2048, 4)], self.ts.get_range('test_key'))
        self.assertEqual([(16384, 2)], self.ts.get_range('test_key', et=3))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3)],
                         self.ts.get_range('test_key', st=2, et=4, include_previous=True))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3), (2048, 4)],
                         self.ts.get_range('test_key', st=0))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3)],
                         self.ts.get_range('test_key', st=0, et=3.5))
        self.assertEqual([(8192, 1)], self.ts.get_range('test_key', st=-1, et=1.5))
        self.assertEqual([(16384, 2), (4096, 3), (2048, 4)],
                         self.ts.get_range('test_key', st=2))
        self.assertEqual([(8192, 1)], self.ts.get_range('test_key', et=1.5))
        self.assertEqual([], self.ts.get_range('test_key', 3.5, 1.5))
        self.assertEqual([], self.ts.get_range('test_key', et=-0.))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3), (2048, 4)],
                         self.ts.get_range('test_key', st=1.5, include_previous=True))
        self.assertEqual([(2048, 4)],
                         self.ts.get_range('test_key', st=5, et=6, include_previous=True))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3)],
                         self.ts.get_range('test_key', st=2, et=4, include_previous=True))
        self.assertRaises(KeyError, self.ts.get_range, 'not_a_key')
        self.assertRaises(ImmutableKeyError, self.ts.get_range, 'test_immutable')

    def test_time_range_include_end(self):
        self.ts.add('test_key', 1234, 0)
        self.ts.add('test_key', 8192, 1)
        self.ts.add('test_key', 16384, 2)
        self.ts.add('test_key', 4096, 3)
        self.ts.add('test_key', 2048, 4)
        self.assertEqual([], self.ts.get_range('test_key', st=1.5, et=1.5, include_end=True))
        self.assertEqual([(1234, 0)],
                         self.ts.get_range('test_key', st=0.0, et=0.0, include_end=True))
        self.assertEqual([(1234, 0)],
                         self.ts.get_range('test_key', st=0.0, et=-0.0, include_end=True))
        self.assertEqual([(4096, 3), (2048, 4)],
                         self.ts.get_range('test_key', st=3, et=4, include_end=True))
        # include_previous tests
        self.assertEqual([(8192, 1), (16384, 2)],
                         self.ts.get_range('test_key', et=2, include_end=True))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3)],
                         self.ts.get_range('test_key', st=2, et=3,
                                           include_previous=True, include_end=True))

    def test_add_duplicate(self):
        self.ts.add('test_key', 'value', 1234.5)
        self.ts.add('test_key', 'value', 1234.5)
        self.assertEqual([('value', 1234.5)], self.ts.get_range('test_key', st=0))

    def test_wait_key_already_done_mutable(self):
        """Calling wait_key with a condition that is met must return (mutable version)."""
        self.ts.add('test_key', 123)
        value, timestamp = self.ts.get_range('test_key')[0]
        self.ts.wait_key('test_key', lambda v, t: v == value and t == timestamp)

    def test_wait_key_already_done_immutable(self):
        """Calling wait_key with a condition that is met must return (immutable version)."""
        self.ts.add('test_key', 123, immutable=True)
        self.ts.wait_key('test_key', lambda v, t: v == self.ts['test_key'] and t is None)

    def test_wait_key_already_done_indexed(self):
        """Calling wait_key with a condition that is met must return (indexed version)."""
        self.ts.set_indexed('test_key', 'idx', 5)
        self.ts.wait_key('test_key', lambda v, t: v == {'idx': 5} and t is None)

    def test_wait_key_timeout(self):
        """wait_key must time out in the given time if the condition is not met"""
        with self.assertRaises(TimeoutError):
            self.ts.wait_key('test_key', timeout=0.1)
        with self.assertRaises(TimeoutError):
            # Takes a different code path, even though equivalent
            self.ts.wait_key('test_key', lambda value, ts: True, timeout=0.1)

    def test_wait_key_delayed(self):
        """wait_key must succeed with a timeout that does not expire before the condition is met."""
        def set_key():
            self.ts.add('test_key', 123)
            time.sleep(0.1)
            self.ts.add('test_key', 234)
        thread = threading.Thread(target=set_key)
        thread.start()
        self.ts.wait_key('test_key', lambda value, ts: value == 234, timeout=2)
        self.assertEqual(234, self.ts.get('test_key'))
        thread.join()

    def test_wait_key_delayed_unconditional(self):
        """wait_key must succeed when given a timeout that does not expire before key appears."""
        def set_key_immutable():
            time.sleep(0.1)
            self.ts['test_key'] = 123

        def set_key_mutable():
            time.sleep(0.1)
            self.ts.add('test_key', 123)

        def set_key_indexed():
            time.sleep(0.1)
            self.ts.set_indexed('test_key', 'idx', 123)

        for set_key, value in [
                (set_key_mutable, 123),
                (set_key_immutable, 123),
                (set_key_indexed, {'idx': 123})]:
            thread = threading.Thread(target=set_key)
            thread.start()
            self.ts.wait_key('test_key', timeout=2)
            self.assertEqual(value, self.ts['test_key'])
            thread.join()
            self.ts.delete('test_key')

    def test_wait_key_already_cancelled(self):
        """wait_key must raise :exc:`CancelledError` if the `cancel_future` is already done."""
        future = mock.MagicMock()
        future.done.return_value = True
        with self.assertRaises(CancelledError):
            self.ts.wait_key('test_key', cancel_future=future)

    def test_wait_key_already_done_and_cancelled(self):
        """wait_key is successful if both the condition and the cancellation are done on entry."""
        future = mock.MagicMock()
        future.done.return_value = True
        self.ts.add('test_key', 123)
        self.ts.wait_key('test_key', lambda value, ts: value == 123, cancel_future=future)

    def test_wait_key_cancel(self):
        """wait_key must return when cancelled."""
        def cancel():
            time.sleep(0.1)
            future.done.return_value = True
        future = mock.MagicMock()
        future.done.return_value = True
        thread = threading.Thread(target=cancel)
        thread.start()
        with self.assertRaises(CancelledError):
            self.ts.wait_key('test_key', cancel_future=future)

    def test_wait_key_shadow(self):
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

    def _test_mixed_unicode_bytes(self, ns, key):
        self.ts.clear()
        ns.add(key, 'value', immutable=True)
        self.assertEqual(ns.get(key), 'value')
        self.assertIn(key, ns)
        self.assertEqual(ns.key_type(key), KeyType.IMMUTABLE)
        ns.delete(key)
        ns.add(key, 'value1', ts=1)
        self.assertEqual(
            ns.get_range(key), [('value1', 1.0)])
        ns.wait_key(key)

    def test_mixed_unicode_bytes(self):
        self._test_mixed_unicode_bytes(self.ts.view(b'ns'), 'test_key')
        self._test_mixed_unicode_bytes(self.ts.view('ns'), b'test_key')

    def test_undecodable_bytes_in_key(self):
        """Gracefully handle non-UTF-8 bytes in keys."""
        key_b = b'undecodable\xff'
        self.ts.backend.set_immutable(key_b, encode_value('hello'))
        key = [k for k in self.ts.keys() if k.startswith('undecodable')][0]
        self.assertEqual(self.ts.get(key), 'hello')
        self.assertEqual(self.ts.get(key_b), 'hello')
        self.assertEqual(self.ts.get(key_b[1:]), None)

    def test_tab_completion(self):
        self.ts['z'] = 'value'
        self.ns['b'] = 'value'
        self.ns['a'] = 'value'
        keys = self.ns._ipython_key_completions_()
        self.assertEqual(sorted(keys), ['a', 'b', 'ns_a', 'ns_b', 'z'])
        keys = self.ts._ipython_key_completions_()
        self.assertEqual(sorted(keys), ['ns_a', 'ns_b', 'z'])

    def test_get_indexed(self):
        self.ts.set_indexed('test_indexed', 'a', 1)
        self.ts.set_indexed('test_indexed', (2, 3j), 2)
        self.assertEqual(self.ts.get_indexed('test_indexed', 'a'), 1)
        self.assertEqual(self.ts.get_indexed('test_indexed', (2, 3j)), 2)
        self.assertIsNone(self.ts.get_indexed('test_indexed', 'missing'))
        self.assertIsNone(self.ts.get_indexed('not_a_key', 'missing'))
        self.assertEqual(self.ts.get('test_indexed'), {'a': 1, (2, 3j): 2})

    def test_set_indexed_immutable(self):
        self.ts.set_indexed('test_indexed', 'a', 1)
        self.ts.set_indexed('test_indexed', 'a', 1)  # Same value is okay
        with self.assertRaises(ImmutableKeyError):
            self.ts.set_indexed('test_indexed', 'a', 2)
        self.assertEqual(self.ts.get_indexed('test_indexed', 'a'), 1)

    def test_indexed_wrong_type(self):
        self.ts['test_immutable'] = 1
        self.ts.add('test_mutable', 2)
        with self.assertRaises(ImmutableKeyError):
            self.ts.set_indexed('test_immutable', 'a', 1)
        with self.assertRaises(ImmutableKeyError):
            self.ts.set_indexed('test_mutable', 'a', 1)
        with self.assertRaises(ImmutableKeyError):
            self.ts.get_indexed('test_immutable', 'a')
        with self.assertRaises(ImmutableKeyError):
            self.ts.get_indexed('test_mutable', 'a')

    def test_namespace_indexed(self):
        self.ts.set_indexed('test_indexed', 'a', 1)
        self.assertEqual(self.ns.get('test_indexed'), {'a': 1})
        self.assertEqual(self.ns.get_indexed('test_indexed', 'a'), 1)
        self.ns.set_indexed('test_indexed', 'b', 2)
        self.assertEqual(self.ns.get('test_indexed'), {'b': 2})
        self.assertEqual(self.ns.get_indexed('test_indexed', 'b'), 2)
        # Namespace key must completely shadow root
        self.assertIsNone(self.ns.get_indexed('test_indexed', 'a'))


class TestTelescopeStateRedis(TestTelescopeState):
    def make_telescope_state(self):
        def make_fakeredis(**kwargs):
            return fakeredis.FakeStrictRedis()

        with mock.patch('redis.StrictRedis', side_effect=make_fakeredis) as mock_redis:
            ts = TelescopeState('example.com', 1)
            mock_redis.assert_called_with(host='example.com', db=1, socket_timeout=mock.ANY)
        return ts


class TestTelescopeStateRedisUrl(TestTelescopeState):
    def make_telescope_state(self):
        def make_fakeredis(cls, **kwargs):
            return fakeredis.FakeStrictRedis()

        with mock.patch('redis.StrictRedis.from_url', side_effect=make_fakeredis) as mock_redis:
            ts = TelescopeState('redis://example.com', db=1)
            mock_redis.assert_called_with('redis://example.com', db=1, socket_timeout=mock.ANY)
        return ts
