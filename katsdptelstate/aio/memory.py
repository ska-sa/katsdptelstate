################################################################################
# Copyright (c) 2019, National Research Foundation (Square Kilometre Array)
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

import logging
from typing import List, Tuple, Dict, Union, Optional

from .. import utils
from .backend import Backend
from ..memory import MemoryBackend as SyncMemoryBackend


logger = logging.getLogger(__name__)
_Value = Union[bytes, Dict[bytes, bytes], List[bytes]]


class MemoryBackend(Backend):
    """Telescope state backend that keeps data in memory.

    See :class:`katsdptelstate.memory.MemoryBackend` for details. This class
    is a thin asynchronous wrapper around that version.
    """

    def __init__(self) -> None:
        self._sync = SyncMemoryBackend()

    async def exists(self, key: bytes) -> bool:
        return key in self._sync

    async def keys(self, filter: bytes) -> List[bytes]:
        return self._sync.keys(filter)

    async def delete(self, key: bytes) -> None:
        self._sync.delete(key)

    async def clear(self) -> None:
        self._sync.clear()

    async def key_type(self, key: bytes) -> Optional[utils.KeyType]:
        return self._sync.key_type(key)

    async def set_immutable(self, key: bytes, value: bytes) -> Optional[bytes]:
        return self._sync.set_immutable(key, value)

    async def get(self, key: bytes) -> Union[
            Tuple[None, None],
            Tuple[bytes, None],
            Tuple[bytes, float],
            Tuple[Dict[bytes, bytes], None]]:
        return self._sync.get(key)

    async def add_mutable(self, key: bytes, value: bytes, timestamp: float) -> None:
        self._sync.add_mutable(key, value, timestamp)

    async def set_indexed(self, key: bytes, sub_key: bytes, value: bytes) -> Optional[bytes]:
        return self._sync.set_indexed(key, sub_key, value)

    async def get_indexed(self, key: bytes, sub_key: bytes) -> Optional[bytes]:
        return self._sync.get_indexed(key, sub_key)

    async def get_range(self, key: bytes, start_time: float, end_time: float,
                        include_previous: bool,
                        include_end: bool) -> Optional[List[Tuple[bytes, float]]]:
        return self._sync.get_range(key, start_time, end_time, include_previous, include_end)

    async def dump(self, key: bytes) -> Optional[bytes]:
        return self._sync.dump(key)

    def close(self) -> None:
        self._sync.clear()

    async def wait_closed(self) -> None:
        pass
