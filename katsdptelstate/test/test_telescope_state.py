"""Tests for the sdp telescope state client."""

import unittest
import redis
import os, time
import mock

from katsdptelstate import TelescopeState, InvalidKeyError, ImmutableKeyError, ArgumentParser

class TestSDPTelescopeState(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        try:
            self.ts = TelescopeState()
             # expects a reachable redis instance to be running locally
        except redis.ConnectionError:
            print "No local redis db available. Are you sure it is running ?"
            raise
        self.ts._r.delete('test_key')
        self.ts._r.delete('test_immutable')
         # make sure we are clean

    @classmethod
    def tearDownClass(self):
        self.ts._r.delete('test_key')
        self.ts._r.delete('test_immutable')

    def test_basic_add(self):
        self.ts.add('test_key',1234.5)
        self.assertEqual(self.ts.test_key, 1234.5)
        self.assertEqual(self.ts['test_key'], 1234.5)
        
        self.ts.delete('test_key')
        with self.assertRaises(AttributeError):
            self.ts.test_key

    def test_method_protection(self):
        with self.assertRaises(InvalidKeyError):
            self.ts.add('get',1234.5)

    def test_delete(self):
        self.ts.add('test_key',1234.5)
        self.ts.delete('test_key')
        self.ts.delete('test_key')

    def test_time_range(self):
        self.ts.delete('test_key')
        self.ts.add('test_key',8192)
        st = time.time()
        self.ts.add('test_key',16384)
        self.assertEqual(1,len(self.ts.get_range('test_key',st=st, et=time.time())))

    def test_immutable(self):
        self.ts.delete('test_immutable')
        self.ts.add('test_immutable',1234.5,immutable=True)
        with self.assertRaises(ImmutableKeyError):
            self.ts.add('test_immutable',1234.5)
        
    def test_complex_store(self):
        import numpy as np
        x = np.array([(1.0, 2), (3.0, 4)], dtype=[('x', float), ('y', int)])
        self.ts.delete('test_key')
        self.ts.add('test_key',x)
        self.assertTrue((self.ts.test_key == x).all())

    def test_return_format(self):
        """Test recarray return format of get_range method:
              Tests that values returned by db are identical to the input values."""
        arr = [[1.,2.,3.],[0.,4.,0.],[10.,9.,7.]]
        self.ts.delete('x')
        self.ts.add('x',arr[0])
        self.ts.add('x',arr[1])
        self.ts.add('x',arr[2])
        val = self.ts.get_range('x',st=0,return_format='recarray')['value']
        self.assertTrue((val == arr).all())

    def test_return_format_type(self):
        """Test recarray return format of get_range method:
              Tests that values returned by db have identical types to the input values."""
        arr = [[1.,2.,3.],[0.,4.,0.]]
        arr_type = type(arr[0][0])
        self.ts.delete('x')
        self.ts.add('x',arr[0])
        self.ts.add('x',arr[1])
        val = self.ts.get_range('x',st=0,return_format='recarray')['value']
        self.assertTrue(val.dtype == arr_type)
        
    def test_get_previous(self):
        """Test get_previous method:
              Tests that correct (most recent) value is returned."""
        arr = [[1.,2.,3.],[0.,4.,0.],[10.,9.,7.]]
        arr_type = type(arr[0][0])
        self.ts.delete('x')
        self.ts.add('x',arr[0],ts=1.0)
        self.ts.add('x',arr[1],ts=2.5)
        self.ts.add('x',arr[2],ts=5.0)
        val = self.ts.get_previous('x',2.8)
        self.assertEqual(val[0],arr[1])

class TestArgumentParser(unittest.TestCase):
    def _stub_get(self, name, default=None):
        if name == 'config':
            return self.config
        else:
            return default

    def setUp(self):
        # Set up a mock version of TelescopeState which applies for the whole test
        patcher = mock.patch('katsdptelstate.telescope_state.TelescopeState', autospec=True)
        self.addCleanup(patcher.stop)
        self.TelescopeState = patcher.start()
        # Create a fixture
        self.parser = ArgumentParser()
        self.parser.add_argument('positional', type=str)
        self.parser.add_argument('--int-arg', type=int, default=5)
        self.parser.add_argument('--float-arg', type=float, default=3.5)
        self.parser.add_argument('--no-default', type=str)
        self.parser.add_argument('--bool-arg', action='store_true', default=False)
        self.config = {
            'int_arg': 10,
            'float_arg': 4.5,
            'no_default': 'telstate',
            'bool_arg': True,
            'not_arg': 'should not be seen',
            'telstate': 'example.org',
            'level1': {
                'int_arg': 11,
                'level2': {
                    'float_arg': 12.5
                }
            }
        }

    def test_no_telstate(self):
        """Passing explicit arguments but no telescope model sets the arguments."""
        args = self.parser.parse_args(['hello', '--int-arg=3', '--float-arg=2.5', '--no-default=test', '--bool-arg'])
        self.assertIsNone(args.telstate)
        self.assertEqual('', args.name)
        self.assertEqual('hello', args.positional)
        self.assertEqual(3, args.int_arg)
        self.assertEqual(2.5, args.float_arg)
        self.assertEqual('test', args.no_default)
        self.assertEqual(True, args.bool_arg)

    def test_no_telstate_defaults(self):
        """Passing no optional arguments sets those arguments"""
        args = self.parser.parse_args(['hello'])
        self.assertIsNone(args.telstate)
        self.assertEqual('', args.name)
        self.assertEqual('hello', args.positional)
        self.assertEqual(5, args.int_arg)
        self.assertEqual(3.5, args.float_arg)
        self.assertIsNone(args.no_default)
        self.assertEqual(False, args.bool_arg)

    def test_telstate_no_name(self):
        """Passing --telstate but not --name loads from root config"""
        self.TelescopeState.return_value.get = mock.MagicMock(side_effect=self._stub_get)
        args = self.parser.parse_args(['hello', '--telstate=example.com'])
        self.assertIs(self.TelescopeState.return_value, args.telstate)
        self.assertEqual('', args.name)
        self.assertEqual(10, args.int_arg)
        self.assertEqual(4.5, args.float_arg)
        self.assertEqual('telstate', args.no_default)
        self.assertEqual(True, args.bool_arg)
        self.assertNotIn('help', vars(args))
        self.assertNotIn('not_arg', vars(args))

    def test_telstate_nested(self):
        """Passing a nested name loads from all levels of the hierarchy"""
        self.TelescopeState.return_value.get = mock.MagicMock(side_effect=self._stub_get)
        args = self.parser.parse_args(['hello', '--telstate=example.com', '--name=level1.level2'])
        self.assertIs(self.TelescopeState.return_value, args.telstate)
        self.assertEqual('level1.level2', args.name)
        self.assertEqual('hello', args.positional)
        self.assertEqual(11, args.int_arg)
        self.assertEqual(12.5, args.float_arg)
        self.assertEqual('telstate', args.no_default)

    def test_telstate_override(self):
        """Command-line parameters override telescope state"""
        self.TelescopeState.return_value.get = mock.MagicMock(side_effect=self._stub_get)
        args = self.parser.parse_args(['hello', '--int-arg=0', '--float-arg=0', '--telstate=example.com', '--name=level1.level2'])
        self.assertIs(self.TelescopeState.return_value, args.telstate)
        self.assertEqual('level1.level2', args.name)
        self.assertEqual('hello', args.positional)
        self.assertEqual(0, args.int_arg)
        self.assertEqual(0.0, args.float_arg)
        self.assertEqual('telstate', args.no_default)

    def test_default_telstate(self):
        """Calling `set_default` with `telstate` keyword works"""
        self.TelescopeState.return_value.get = mock.MagicMock(side_effect=self._stub_get)
        self.parser.set_defaults(telstate='example.com')
        args = self.parser.parse_args(['hello'])
        self.TelescopeState.assert_called_once_with('example.com')
        self.assertIs(self.TelescopeState.return_value, args.telstate)
        self.assertEqual(10, args.int_arg)
