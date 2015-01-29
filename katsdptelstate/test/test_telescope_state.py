"""Tests for the sdp telescope state client."""

import unittest
import redis
import os, time

from katsdptelstate import TelescopeState, InvalidKeyError, ImmutableKeyError

class TestSDPTelescopeState(unittest.TestCase):
    def setUp(self):
        try:
            self.ts = TelescopeState()
             # expects a reachable redis instance to be running locally
        except redis.ConnectionError:
            print "No local redis db available. Are you sure it is running ?"
            raise redis.ConnectionError
        self.ts._r.delete('test_key')
        self.ts._r.delete('test_immutable')
         # make sure we are clean

    def tearDown(self):
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

    def test_has_key(self):
        self.assertFalse(self.ts.has_key('test_key'))
        self.ts.add('test_key', 1234)
        self.ts.add('test_immutable', 1234.5, immutable=True)
        self.assertTrue(self.ts.has_key('test_key'))
        self.assertTrue(self.ts.has_key('test_immutable'))

    def test_override_local_defaults_optparse(self):
        import optparse
        parser = optparse.OptionParser()
        parser.add_option('-s', '--string-opt', type='str', default='cmdline')
        parser.add_option('--string-opt2', type='str', default='cmdline2')
        parser.add_option('-i', '--int-opt', type='int', default=3)
        parser.add_option('--int-opt2', type='int', default=3)
        parser.add_option('--flag', action='store_true', default=False)
        parser.add_option('--no-default', type='int')
        self.ts.add('test_key',
                {'string_opt': 'ts', 'int_opt': 2, 'int_opt2': 2, 'flag': True, 'other': 5.0,
                    'no_default': 10},
                immutable=True)
        self.ts.override_local_defaults(parser, 'test_key')
        (opts, args) = parser.parse_args(['--int-opt2', '5'])
        # Telescope state default overrides command line default
        self.assertEqual('ts', opts.string_opt)
        self.assertEqual(2, opts.int_opt)
        self.assertEqual(True, opts.flag)
        # Arguments without a default are still overridden
        self.assertEqual(10, opts.no_default)
        # Command-line default applies if telescope state doesn't override
        self.assertEqual('cmdline2', opts.string_opt2)
        # Command-line argument overrides both
        self.assertEqual(5, opts.int_opt2)
        # Other telescope state data shouldn't affect things
        self.assertNotIn('other', opts.__dict__)

    def test_override_local_defaults_argparse(self):
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('positional', type=float)
        parser.add_argument('-s', '--string-opt', type=str, default='cmdline')
        parser.add_argument('--string-opt2', type=str, default='cmdline2')
        parser.add_argument('-i', '--int-opt', type=int, default=3)
        parser.add_argument('--int-opt2', type=int, default=3)
        parser.add_argument('--flag', action='store_true', default=False)
        parser.add_argument('--no-default', type=int)
        self.ts.add('test_key',
                {'string_opt': 'ts', 'int_opt': 2, 'int_opt2': 2, 'flag': True, 'other': 5.0,
                    'no_default': 10},
                immutable=True)
        self.ts.override_local_defaults(parser, 'test_key')
        args = parser.parse_args([0.0, '--int-opt2', '5'])
        # Telescope state default overrides command-line default
        self.assertEqual('ts', args.string_opt)
        self.assertEqual(2, args.int_opt)
        self.assertEqual(True, args.flag)
        # Arguments without a default are still overridden
        self.assertEqual(10, args.no_default)
        # Command-line default applies if telescope state doesn't override
        self.assertEqual('cmdline2', args.string_opt2)
        # Command-line argument overrides both
        self.assertEqual(5, args.int_opt2)
        # Other telescope state data shouldn't affect things
        self.assertNotIn('other', vars(args))
