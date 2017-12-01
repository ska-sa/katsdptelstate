"""Tests for the sdp telescope state client."""

from __future__ import print_function, division, absolute_import
import threading
import time
import unittest
import mock
import numpy as np
try:
    import cPickle as pickle
except ImportError:
    import pickle

from katsdptelstate import (TelescopeState, InvalidKeyError, ImmutableKeyError,
                            TimeoutError, CancelledError,
                            PICKLE_PROTOCOL)


class TestSDPTelescopeState(unittest.TestCase):
    def setUp(self):
        self.ts = TelescopeState()
        self.ts._r.flushdb()
         # make sure we are clean
        self.ns = self.ts.view('ns')

    def tearDown(self):
        self.ts._r.flushdb()

    def test_namespace(self):
        self.assertEqual(self.ts.prefixes, ('',))
        self.assertEqual(self.ns.prefixes, ('ns.', ''))
        ns2 = self.ns.view('ns.child.grandchild')
        self.assertEqual(ns2.prefixes, ('ns.child.grandchild.', 'ns.', ''))
        self.assertEqual(ns2.root().prefixes, ('',))

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
        self.assertEqual(self.ts['ns.test_key'], 1234.5)
        with self.assertRaises(KeyError):
            self.ts['test_key']

    def test_method_protection(self):
        with self.assertRaises(InvalidKeyError):
            self.ts.add('get', 1234.5)

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
        self.assertEqual([], self.ts.keys())

    def test_return_pickle(self):
        import numpy as np
        x = np.array([(1.0, 2), (3.0, 4)], dtype=[('x', float), ('y', int)])
        self.ts.add('test_key_rt', x, immutable=True)
        x_unpickled = self.ts.get('test_key_rt')
        self.assertTrue((x_unpickled == x).all())
        x_pickled = self.ts.get('test_key_rt', return_pickle=True)
        self.assertEqual(x_pickled, pickle.dumps(x, protocol=PICKLE_PROTOCOL))

    def test_return_pickle_range(self):
        import numpy as np
        import time
        test_values = ['Test Value: {}'.format(x) for x in range(5)]
        for i,test_value in enumerate(test_values): self.ts.add('test_key',test_value,i)
        stored_values = self.ts.get_range('test_key', st=0)
        self.assertEqual(stored_values[2][0], test_values[2])
        stored_values_pickled = self.ts.get_range('test_key', st=0, return_pickle=True)
        self.assertEqual(stored_values_pickled[2][0],
                         pickle.dumps(test_values[2], protocol=PICKLE_PROTOCOL))
        self.assertEqual(stored_values_pickled[2][1], 2)
         # check timestamp

    def test_immutable(self):
        self.ts.add('test_immutable', 1234.5, immutable=True)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_immutable', 1234.5)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_immutable', 5432.1, immutable=True)
        # Same value may be set
        self.ts.add('test_immutable', 1234.5, immutable=True)
        self.ts.add('test_mutable', 1234.5)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_mutable', 2345.6, immutable=True)
        # None values work correctly
        self.ts.add('test_none', None, immutable=True)
        self.assertIsNone(self.ts.get('test_none'))
        self.assertIsNone(self.ts.get('test_none', 'not_none'))
        self.assertIsNone(self.ts.test_none)
        self.assertIsNone(self.ts['test_none'])

    def test_namespace_immutable(self):
        self.ts.add('parent_immutable', 1234.5, immutable=True)
        self.ns.add('child_immutable', 2345.5, immutable=True)
        with self.assertRaises(KeyError):
            self.ts['child_immutable']
        self.assertEqual(self.ns.get('child_immutable'), 2345.5)
        self.assertEqual(self.ns.get('parent_immutable'), 1234.5)

    def test_is_immutable(self):
        self.ts.add('parent_immutable', 1, immutable=True)
        self.ns.add('child_immutable', 2, immutable=True)
        self.ts.add('parent', 3)
        self.ns.add('child', 4)

        self.assertTrue(self.ts.is_immutable('parent_immutable'))
        self.assertFalse(self.ts.is_immutable('parent'))
        self.assertFalse(self.ts.is_immutable('child_immutable'))  # Unreachable
        self.assertFalse(self.ts.is_immutable('child'))  # Unreachable
        self.assertFalse(self.ts.is_immutable('not_a_key'))

        self.assertTrue(self.ns.is_immutable('parent_immutable'))
        self.assertFalse(self.ns.is_immutable('parent'))
        self.assertTrue(self.ns.is_immutable('child_immutable'))
        self.assertFalse(self.ns.is_immutable('child'))
        self.assertFalse(self.ns.is_immutable('not_a_key'))

    def test_keys(self):
        self.ts.add('key1', 'a')
        self.ns.add('key2', 'b')
        self.ns.add('key2', 'c')
        self.ts.add('immutable', 'd', immutable=True)
        self.assertEqual(self.ts.keys(), [b'immutable', b'key1', b'ns.key2'])
        self.assertEqual(self.ts.keys('ns.*'), [b'ns.key2'])

    def test_complex_store(self):
        import numpy as np
        x = np.array([(1.0, 2), (3.0, 4)], dtype=[('x', float), ('y', int)])
        self.ts.delete('test_key')
        self.ts.add('test_key', x)
        self.assertTrue((self.ts.test_key == x).all())

    def test_has_key(self):
        self.ts.add('test_key', 1234.5)
        self.assertTrue(self.ts.has_key('test_key'))
        self.assertFalse(self.ts.has_key('nonexistent_test_key'))

    def test_contains(self):
        self.ts.add('test_key', 1234.5)
        self.assertTrue('test_key' in self.ts)
        self.assertFalse('nonexistent_test_key' in self.ts)

    def test_return_format(self):
        """Test recarray return format of get_range method:
              Tests that values returned by db are identical to the input values."""
        arr = [[1., 2., 3.], [0., 4., 0.], [10., 9., 7.]]
        self.ts.delete('test_x')
        self.ts.add('test_x', arr[0])
        self.ts.add('test_x', arr[1])
        self.ts.add('test_x', arr[2])
        val = self.ts.get_range('test_x', st=0, return_format='recarray')['value']
        self.assertTrue((val == arr).all())

    def test_return_format_type(self):
        """Test recarray return format of get_range method:
              Tests that values returned by db have identical types to the input values."""
        arr = [[1., 2., 3.], [0., 4., 0.]]
        arr_type = type(arr[0][0])
        self.ts.delete('test_x')
        self.ts.add('test_x', arr[0])
        self.ts.add('test_x', arr[1])
        val = self.ts.get_range('test_x', st=0, return_format='recarray')['value']
        self.assertTrue(val.dtype == arr_type)

    def test_return_format_type_string(self):
        """Test recarray return format of get_range method:
              Tests that array of variable length strings are correctly returned."""
        self.ts.delete('test_x')
        self.ts.add('test_x', 'hi')
        self.ts.add('test_x', 'how')
        self.ts.add('test_x', 'are')
        self.ts.add('test_x', 'you?')
        val = self.ts.get_range('test_x', st=0, return_format='recarray')['value']
        self.assertEqual('you?', val[3])

    def test_time_range(self):
        self.ts.delete('test_key')
        self.ts.add('test_key', 8192, 1)
        self.ts.add('test_key', 16384, 2)
        self.ts.add('test_key', 4096, 3)
        self.ts.add('test_key', 2048, 4)
        self.ts.add('test_immutable', 12345, immutable=True)
        self.assertEqual([(2048, 4)], self.ts.get_range('test_key'))
        self.assertEqual([(16384, 2)], self.ts.get_range('test_key', et=3))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3)], self.ts.get_range('test_key', st=2, et=4, include_previous=True))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3), (2048, 4)], self.ts.get_range('test_key', st=0))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3)], self.ts.get_range('test_key', st=0, et=3.5))
        self.assertEqual([(8192, 1)], self.ts.get_range('test_key', st=-1, et=1.5))
        self.assertEqual([(16384, 2), (4096, 3), (2048, 4)], self.ts.get_range('test_key', st=2))
        self.assertEqual([(8192, 1)], self.ts.get_range('test_key', et=1.5))
        self.assertEqual([], self.ts.get_range('test_key', 3.5, 1.5))
        self.assertEqual([], self.ts.get_range('test_key', et=-0.))
        self.assertEqual([(8192, 1), (16384, 2), (4096, 3), (2048, 4)], self.ts.get_range('test_key', st=1.5, include_previous=True))
        self.assertEqual([(2048, 4)], self.ts.get_range('test_key', st=5, et=6, include_previous=True))
        self.assertRaises(KeyError, self.ts.get_range, 'not_a_key')
        self.assertRaises(ImmutableKeyError, self.ts.get_range, 'test_immutable')

    def test_wait_key_already_done_sensor(self):
        """Calling wait_key with a condition that is met must return (sensor version)."""
        self.ts.add('test_key', 123)
        value, timestamp = self.ts.get_range('test_key')[0]
        self.ts.wait_key('test_key', lambda v, t: v == value and t == timestamp)

    def test_wait_key_already_done_attr(self):
        """Calling wait_key with a condition that is met must return (attribute version)."""
        self.ts.add('test_key', 123, immutable=True)
        self.ts.wait_key('test_key', lambda v, t: v == self.ts['test_key'] and t is None)

    def test_wait_key_timeout(self):
        """wait_key must time out in the given time if the condition is not met"""
        with self.assertRaises(TimeoutError):
            self.ts.wait_key('test_key', timeout=0.1)

    def test_wait_key_delayed(self):
        """wait_key must succeed when given a timeout that does not expire before the condition is met"""
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
        def set_key():
            time.sleep(0.1)
            self.ts.add('test_key', 123, immutable=True)
        thread = threading.Thread(target=set_key)
        thread.start()
        self.ts.wait_key('test_key', timeout=2)
        self.assertEqual(123, self.ts['test_key'])
        thread.join()

    def test_wait_key_already_cancelled(self):
        """wait_key must raise :exc:`CancelledError` if the provided `cancel_future` is already done."""
        future = mock.MagicMock()
        future.done.return_value = True
        with self.assertRaises(CancelledError):
            self.ts.wait_key('test_key', cancel_future=future)

    def test_wait_key_already_done_and_cancelled(self):
        """wait_key is successful if both the condition and the cancellation are done on entry"""
        future = mock.MagicMock()
        future.done.return_value = True
        self.ts.add('test_key', 123)
        self.ts.wait_key('test_key', lambda value, ts: value == 123, cancel_future=future)

    def test_wait_key_cancel(self):
        """wait_key must return when cancelled"""
        def cancel():
            time.sleep(0.1)
            future.done.return_value = True
        future = mock.MagicMock()
        future.done.return_value = True
        thread = threading.Thread(target=cancel)
        thread.start()
        with self.assertRaises(CancelledError):
            self.ts.wait_key('test_key', cancel_future=future)
