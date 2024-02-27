################################################################################
# Copyright (c) 2020-2022, National Research Foundation (Square Kilometre Array)
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

"""Limited testing for :mod:`katsdptelstate.aio.rdb_writer`.

Most of the core functionality is tested via
:mod:`katsdptelstate.test.test_rdb_handling`. These tests are specifically for
the async bits.
"""

import logging
import os
from typing import AsyncGenerator

import pytest
import pytest_asyncio

import katsdptelstate
from katsdptelstate import KeyType
from katsdptelstate.aio import TelescopeState
from katsdptelstate.aio.rdb_writer import RDBWriter


class TestRdbWriter:
    @pytest_asyncio.fixture
    async def telstate(self) -> AsyncGenerator[TelescopeState, None]:
        telstate = TelescopeState()
        await telstate.set('immutable', 'abc')
        await telstate.add('mutable', 'def', ts=1234567890.0)
        await telstate.set_indexed('indexed', 'subkey', 'xyz')
        yield telstate
        telstate.backend.close()
        await telstate.backend.wait_closed()

    @pytest.fixture
    def reader(self) -> katsdptelstate.TelescopeState:  # Synchronous
        return katsdptelstate.TelescopeState()

    @pytest.fixture
    def filename(self, tmpdir) -> str:
        return os.path.join(tmpdir, 'test.rdb')

    async def test_write_all(
            self,
            filename: str,
            telstate: TelescopeState,
            reader: katsdptelstate.TelescopeState) -> None:
        writer = RDBWriter(filename)
        await writer.save(telstate)
        writer.close()
        assert writer.keys_written == 3
        assert writer.keys_failed == 0
        reader.load_from_file(filename)
        assert set(reader.keys()) == {'immutable', 'indexed', 'mutable'}
        assert reader.key_type('immutable') == KeyType.IMMUTABLE
        assert reader.get('immutable') == 'abc'
        assert reader.get_range('mutable', st=0) == [('def', 1234567890.0)]
        assert reader.get('indexed') == {'subkey': 'xyz'}

    async def test_write_some(
            self,
            filename: str,
            telstate: TelescopeState,
            reader: katsdptelstate.TelescopeState,
            caplog) -> None:
        writer = RDBWriter(filename)
        with caplog.at_level(logging.ERROR, logger='katsdptelstate.rdb_writer_base'):
            await writer.save(telstate, ['immutable', 'missing'])
        assert caplog.record_tuples[-1] == (
            'katsdptelstate.rdb_writer_base',
            logging.ERROR,
            "Failed to save key 'missing': 'Key not found in Redis'"
        )
        writer.close()
        assert writer.keys_written == 1
        assert writer.keys_failed == 1
        reader.load_from_file(filename)
        assert reader.keys() == ['immutable']
        assert reader.get('immutable') == 'abc'
