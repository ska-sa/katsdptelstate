"""Tests for the sdp telescope state client."""

import unittest
import redis
import os, time

from katsdptelstate import TelescopeState, InvalidKeyError, ImmutableKeyError

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

        
        
