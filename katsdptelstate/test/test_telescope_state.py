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
            raise redis.ConnectionError
        self.ts._r.delete('test_key')
        self.ts._r.delete('test_immutable')
         # make sure we are clean

    @classmethod
    def tearDownClass(self):
        self.ts._r.delete('test_key')
        self.ts._r.delete('tes_immutable')

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

@mock.patch('katsdptelstate.telescope_state.TelescopeState', autospec=True)
class TestArgumentParser(unittest.TestCase):
    def _stub_get(self, name, default=None):
        if name == 'config':
            return self.config
        else:
            return default

    def setUp(self):
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
            'level1': {
                'int_arg': 11,
                'level2': {
                    'float_arg': 12.5
                }
            }
        }

    def test_no_telstate(self, TelescopeState):
        """Passing explicit arguments but no telescope model sets the arguments."""
        args = self.parser.parse_args(['hello', '--int-arg=3', '--float-arg=2.5', '--no-default=test', '--bool-arg'])
        self.assertIsNone(args.telstate)
        self.assertEqual('', args.name)
        self.assertEqual('hello', args.positional)
        self.assertEqual(3, args.int_arg)
        self.assertEqual(2.5, args.float_arg)
        self.assertEqual('test', args.no_default)
        self.assertEqual(True, args.bool_arg)

    def test_no_telstate_defaults(self, TelescopeState):
        """Passing no optional arguments sets those arguments"""
        args = self.parser.parse_args(['hello'])
        self.assertIsNone(args.telstate)
        self.assertEqual('', args.name)
        self.assertEqual('hello', args.positional)
        self.assertEqual(5, args.int_arg)
        self.assertEqual(3.5, args.float_arg)
        self.assertIsNone(args.no_default)
        self.assertEqual(False, args.bool_arg)

    def test_telstate_no_name(self, TelescopeState):
        """Passing --telstate but not --name loads from root config"""
        TelescopeState.return_value.get = mock.MagicMock(side_effect=self._stub_get)
        args = self.parser.parse_args(['hello', '--telstate=example.com'])
        self.assertIs(TelescopeState.return_value, args.telstate)
        self.assertEqual('', args.name)
        self.assertEqual(10, args.int_arg)
        self.assertEqual(4.5, args.float_arg)
        self.assertEqual('telstate', args.no_default)
        self.assertEqual(True, args.bool_arg)
        self.assertNotIn('help', vars(args))
        self.assertNotIn('not_arg', vars(args))

    def test_telstate_nested(self, TelescopeState):
        """Passing a nested name loads from all levels of the hierarchy"""
        TelescopeState.return_value.get = mock.MagicMock(side_effect=self._stub_get)
        args = self.parser.parse_args(['hello', '--telstate=example.com', '--name=level1.level2'])
        self.assertIs(TelescopeState.return_value, args.telstate)
        self.assertEqual('level1.level2', args.name)
        self.assertEqual('hello', args.positional)
        self.assertEqual(11, args.int_arg)
        self.assertEqual(12.5, args.float_arg)
        self.assertEqual('telstate', args.no_default)

    def test_telstate_override(self, TelescopeState):
        """Command-line parameters override telescope state"""
        TelescopeState.return_value.get = mock.MagicMock(side_effect=self._stub_get)
        args = self.parser.parse_args(['hello', '--int-arg=0', '--float-arg=0', '--telstate=example.com', '--name=level1.level2'])
        self.assertIs(TelescopeState.return_value, args.telstate)
        self.assertEqual('level1.level2', args.name)
        self.assertEqual('hello', args.positional)
        self.assertEqual(0, args.int_arg)
        self.assertEqual(0.0, args.float_arg)
        self.assertEqual('telstate', args.no_default)
