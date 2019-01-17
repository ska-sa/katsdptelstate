"""Tests for the RDB handling (reading and writing) functionality."""

from __future__ import print_function, division, absolute_import

import time
import unittest
import struct
import shutil
import os
import tempfile

from katsdptelstate.rdb_writer import RDBWriter
from katsdptelstate.rdb_reader import load_from_file
from katsdptelstate.tabloid_redis import TabloidRedis
from katsdptelstate.compat import zadd
from katsdptelstate import TelescopeState


class TestRDBHandling(unittest.TestCase):
    def setUp(self):
        self.tr = TabloidRedis()
         # an empty tabloid redis instance
        self.ts = TelescopeState()
        self.rdb_writer = RDBWriter(client=self.tr)
        self.base_dir = tempfile.mkdtemp()

    def base(self, filename):
        return os.path.join(self.base_dir, filename)

    def tearDown(self):
        self.tr.flushdb()
        try:
            shutil.rmtree(self.base_dir)
        except OSError:
            pass

    def _enc_ts(self, ts):
        return struct.pack('>d', ts)

    def _add_test_vec(self, key, ts):
        zadd(self.tr, key, {self._enc_ts(ts) + b'first': 0})
        zadd(self.tr, key, {self._enc_ts(ts + 2) + b'third': 0})
        zadd(self.tr, key, {self._enc_ts(ts + 1) + b'second': 0})

    def test_writer_reader(self):
        base_ts = int(time.time())
        self._add_test_vec('writezl', base_ts)
        test_str = b"some string\x00\xa3\x17\x43and now valid\xff"
        self.tr.set('write', test_str)
        self.assertEqual(self.rdb_writer.save(self.base('all.rdb'))[0], 2)
        self.assertEqual(self.rdb_writer.save(self.base('one.rdb'), keys=['writezl'])[0], 1)
        self.assertEqual(self.rdb_writer.save(self.base('broken.rdb'), keys=['does_not_exist'])[0], 0)
        self.tr.flushall()

        local_tr = TabloidRedis()
        load_from_file(local_tr, self.base('all.rdb'))
        self.assertEqual(load_from_file(local_tr, self.base('all.rdb')), 2)
        self.assertEqual(local_tr.get('write'), test_str)
        sorted_pair = local_tr.zrangebylex('writezl', b'(' + self._enc_ts(base_ts + 0.001),
                                           b'[' + self._enc_ts(base_ts + 2.001))
        self.assertEqual(sorted_pair[1][8:], b'third')
        self.tr.flushall()

        local_tr = TabloidRedis()
        load_from_file(local_tr, self.base('one.rdb'))
        self.assertEqual(len(local_tr.keys()), 1)
        self.assertEqual(local_tr.zcard('writezl'), 3)
        self.tr.flushall()

        self.assertEqual(self.ts.load_from_file(self.base('all.rdb')), 2)
        self.tr.flushall()
