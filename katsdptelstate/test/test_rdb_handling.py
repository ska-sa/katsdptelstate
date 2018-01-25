"""Tests for the RDB handling (reading and writing) functionality."""

from __future__ import print_function, division, absolute_import
import time
import unittest
import struct
import os
import tempfile

from katsdptelstate.rdb_writer import RDBWriter
from katsdptelstate.tabloid_redis import TabloidRedis
from katsdptelstate import TelescopeState

class TestRDBHandling(unittest.TestCase):
    def setUp(self):
        self.tr = TabloidRedis()
         # an empty tabloid redis instance
        self.ts = TelescopeState()
        self.rdb_writer = RDBWriter(client=self.tr)
        self.base_dir = tempfile.mkdtemp()

    def base(self, filename):
        return "{}/{}".format(self.base_dir, filename)

    def tearDown(self):
        self.tr.flushdb()
        try:
            os.remove(self.base_dir)
        except OSError: pass

    def test_basic_operations(self):
        self.assertEqual(self.tr.keys(), [])
        self.tr.set('foo','bar')
        self.assertEqual(self.tr.get('foo'), b'bar')
        self.tr.flushdb()
        self.assertEqual(self.tr.get('fooled'), None)
        self.tr.flushdb()

    def test_zset_operations(self):
        for x in range(10): 
            self.tr.zadd('fooz',0,'item_{}'.format(x))
        self.assertEqual(self.tr.zcard('fooz'), 10)
        self.assertEqual(self.tr.zrange('fooz',3,3)[0], b'item_3')
        self.tr.flushdb()

    def _enc_ts(self, ts):
        return struct.pack('>d', ts)

    def _add_test_vec(self, key, ts):
        self.tr.zadd(key,0,self._enc_ts(ts) + b'first')
        self.tr.zadd(key,0,self._enc_ts(ts + 2) + b'third')
        self.tr.zadd(key,0,self._enc_ts(ts + 1) + b'second')
        
    def test_zset_lex(self):
        base_ts = int(time.time())
        self._add_test_vec('foozl', base_ts)
        sorted_items = self.tr.zrangebylex('foozl','-','+')
        self.assertEqual(len(sorted_items), 3)
        self.assertEqual(sorted_items[1][8:], b'second')
       
         # test fully open interval
        sorted_single = self.tr.zrangebylex('foozl',b'(' + self._enc_ts(base_ts + 0.001), b'(' + self._enc_ts(base_ts + 2))
        self.assertEqual(len(sorted_single), 1)
        self.assertEqual(sorted_single[0][8:], b'second')

         # reverse half open
        sorted_rev = self.tr.zrevrangebylex('foozl',b'[' + self._enc_ts(base_ts + 2.001), b'(' + self._enc_ts(base_ts + 0.001))
        self.assertEqual(len(sorted_rev), 2)
        self.assertEqual(sorted_rev[0][8:], b'third')
        self.tr.flushdb()

    def test_writer_reader(self):
        base_ts = int(time.time())
        self._add_test_vec('writezl', base_ts)
        test_str = b"some string\x00\xa3\x17\x43and now valid\xff"
        self.tr.set('write', test_str)
        self.assertEqual(self.rdb_writer.save(self.base('all.rdb'))[0], 2)
        self.assertEqual(self.rdb_writer.save(self.base('one.rdb'),keys=['writezl'])[0], 1)
        self.assertEqual(self.rdb_writer.save(self.base('broken.rdb'),keys=['does_not_exist'])[0], 0)
        self.tr.flushall()

        local_tr = TabloidRedis()
        local_tr.load_from_file(self.base('all.rdb'))
        self.assertEqual(local_tr.load_from_file(self.base('all.rdb')), 2)
        self.assertEqual(local_tr.get('write'), test_str)
        sorted_pair = local_tr.zrangebylex('writezl',b'(' + self._enc_ts(base_ts + 0.001), b'[' + self._enc_ts(base_ts + 2.001))
        self.assertEqual(sorted_pair[1][8:], b'third')
        self.tr.flushall()

        local_tr = TabloidRedis()
        local_tr.load_from_file(self.base('one.rdb'))
        self.assertEqual(len(local_tr.keys()), 1)
        self.assertEqual(local_tr.zcard('writezl'), 3)
        self.tr.flushall()

        self.assertEqual(self.ts.load_from_file(self.base('all.rdb')), 2)
        self.tr.flushall()
