################################################################################
# Copyright (c) 2018-2019, National Research Foundation (Square Kilometre Array)
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

"""Tests for the RDB handling (reading and writing) functionality."""

from __future__ import print_function, division, absolute_import

import unittest
import shutil
import os
import tempfile

import redis
import fakeredis

from katsdptelstate.rdb_writer import RDBWriter
from katsdptelstate.rdb_reader import load_from_file, RedisCallback
from katsdptelstate.tabloid_redis import TabloidRedis
from katsdptelstate.compat import zadd
from katsdptelstate.redis import RedisBackend
from katsdptelstate import TelescopeState, RdbParseError


class TestRDBHandling(unittest.TestCase):
    """Test that data can be written by :class:`~.TabloidRedis` then read back"""
    def setUp(self):
        # an empty tabloid redis instance
        self.tr = TabloidRedis()
        self.rdb_writer = RDBWriter(client=self.tr)
        self.base_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.base_dir)

    def base(self, filename):
        return os.path.join(self.base_dir, filename)

    def _add_test_vec(self, key):
        # TabloidRedis does not properly support non-zero scores
        zadd(self.tr, key, {b'first': 0.0, b'second': 0.0, b'third\n\0': 0.0})

    def test_writer_reader(self):
        self._add_test_vec('writezl')
        test_str = b"some string\x00\xa3\x17\x43and now valid\xff"
        self.tr.set('write', test_str)
        self.assertEqual(self.rdb_writer.save(self.base('all.rdb'))[0], 2)
        self.assertEqual(self.rdb_writer.save(self.base('one.rdb'), keys=['writezl'])[0], 1)
        self.assertEqual(self.rdb_writer.save(self.base('broken.rdb'), keys=['does_not_exist'])[0], 0)

        local_tr = TabloidRedis()
        self.assertEqual(load_from_file(RedisCallback(local_tr), self.base('all.rdb')), 2)
        self.assertEqual(set(local_tr.keys()), {b'write', b'writezl'})
        self.assertEqual(local_tr.get('write'), test_str)
        vec = local_tr.zrange('writezl', 0, -1, withscores=True)
        self.assertEqual(vec, [(b'first', 0.0), (b'second', 0.0), (b'third\n\0', 0.0)])

        local_tr = TabloidRedis()
        load_from_file(RedisCallback(local_tr), self.base('one.rdb'))
        self.assertEqual(local_tr.keys(), [b'writezl'])
        vec = local_tr.zrange('writezl', 0, -1, withscores=True)
        self.assertEqual(vec, [(b'first', 0.0), (b'second', 0.0), (b'third\n\0', 0.0)])

    def _test_zset(self, items):
        zadd(self.tr, 'my_zset', {x: 0.0 for x in items})
        self.rdb_writer.save(self.base('zset.rdb'))

        local_tr = TabloidRedis()
        load_from_file(RedisCallback(local_tr), self.base('zset.rdb'))
        self.assertEqual(local_tr.keys(), [b'my_zset'])
        vec = local_tr.zrange('my_zset', 0, -1, withscores=True)
        self.assertEqual(vec, [(item, 0.0) for item in items])

    def test_zset_many_entries(self):
        """Zset with more than 127 entries.

        This uses the more general encoding, rather than ziplist.
        """
        self._test_zset([b'item%03d' % i for i in range(200)])

    def test_zset_with_big_entry(self):
        """Ziplist with large entry (has different encoding)"""
        self._test_zset([b'?' * 100000])

    # Disabled because it uses too much memory in Jenkins
    # def test_zset_4gb(self):
    #     """Ziplist with >4GB of data (can't be encoded as ziplist)"""
    #     self._test_zset([(b'%03d' % i) + b'?' * 500000000 for i in range(10)])


class TestLoadFromFile(unittest.TestCase):
    """Test :meth:`TelescopeState.load_from_file`."""

    def setUp(self):
        # an empty tabloid redis instance
        self.base_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.base_dir)
        self.filename = os.path.join(self.base_dir, 'dump.rdb')

    def make_telescope_state(self):
        return TelescopeState()

    def save_to_file(self, file):
        write_ts = self.make_telescope_state()
        write_ts['immutable'] = ['some value']
        write_ts.add('mutable', 'first', 12.0)
        write_ts.add('mutable', 'second', 15.5)
        # Write data to file
        rdb_writer = RDBWriter(client=write_ts.backend)
        rdb_writer.save(file)

    def load_from_file_and_check(self, file):
        # Load RDB file back into some backend
        read_ts = self.make_telescope_state()
        read_ts.load_from_file(file)
        self.assertEqual(read_ts.keys(), ['immutable', 'mutable'])
        self.assertTrue(read_ts.is_immutable('immutable'))
        self.assertEqual(read_ts['immutable'], ['some value'])
        self.assertEqual(read_ts.get_range('mutable', st=0),
                         [('first', 12.0), ('second', 15.5)])

    def test_load_from_file(self):
        self.save_to_file(self.filename)
        # Check loading from filenames and file-like objects
        self.load_from_file_and_check(self.filename)
        self.load_from_file_and_check(open(self.filename, 'rb'))
        # Check that malformed RDB file raises the appropriate exception
        file = open(self.filename, 'rb')
        file.read(1)
        with self.assertRaises(RdbParseError):
            self.load_from_file_and_check(file)
        # Check what happens if file does not exist
        with self.assertRaises(OSError):
            self.load_from_file_and_check(self.filename + '.nonexistent')


class TestLoadFromFileRedis(TestLoadFromFile):
    """Test :meth:`TelescopeState.load_from_file` with redis backend."""
    def make_telescope_state(self, **kwargs):
        return TelescopeState(RedisBackend(TabloidRedis(**kwargs)))

    def test_callback_errors_are_preserved(self):
        """Check that a redis.ConnectionError doesn't mutate into RdbParseError."""
        self.save_to_file(self.filename)
        server = fakeredis.FakeServer()
        read_ts = self.make_telescope_state(server=server)
        server.connected = False
        with self.assertRaises(redis.ConnectionError):
            read_ts.load_from_file(self.filename)
