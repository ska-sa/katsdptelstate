"""Limited testing for :mod:`katsdptelstate.aio.rdb_writer`.

Most of the core functionality is tested via
:mod:`katsdptelstate.test.test_rdb_handling`. These tests are specifically for
the async bits.
"""

import os
import shutil
import tempfile

import asynctest

import katsdptelstate
from katsdptelstate import KeyType
from katsdptelstate.aio import TelescopeState
from katsdptelstate.aio.rdb_writer import RDBWriter


class TestRdbWriter(asynctest.TestCase):
    async def setUp(self):
        self.telstate = TelescopeState()
        await self.telstate.set('immutable', 'abc')
        await self.telstate.add('mutable', 'def', ts=1234567890.0)
        await self.telstate.set_indexed('indexed', 'subkey', 'xyz')
        base_dir = tempfile.mkdtemp()
        self.filename = os.path.join(base_dir, 'test.rdb')
        self.addCleanup(shutil.rmtree, base_dir)
        self.reader = katsdptelstate.TelescopeState()  # Synchronous

    async def test_write_all(self):
        writer = RDBWriter(self.filename)
        await writer.save(self.telstate)
        writer.close()
        self.assertEqual(writer.keys_written, 3)
        self.assertEqual(writer.keys_failed, 0)
        self.reader.load_from_file(self.filename)
        self.assertEqual(set(self.reader.keys()), {'immutable', 'indexed', 'mutable'})
        self.assertEqual(self.reader.key_type('immutable'), KeyType.IMMUTABLE)
        self.assertEqual(self.reader.get('immutable'), 'abc')
        self.assertEqual(self.reader.get_range('mutable', st=0), [('def', 1234567890.0)])
        self.assertEqual(self.reader.get('indexed'), {'subkey': 'xyz'})

    async def test_write_some(self):
        writer = RDBWriter(self.filename)
        with self.assertLogs('katsdptelstate.rdb_writer_base', level='ERROR'):
            await writer.save(self.telstate, ['immutable', 'missing'])
        writer.close()
        self.assertEqual(writer.keys_written, 1)
        self.assertEqual(writer.keys_failed, 1)
        self.reader.load_from_file(self.filename)
        self.assertEqual(self.reader.keys(), ['immutable'])
        self.assertEqual(self.reader.get('immutable'), 'abc')
