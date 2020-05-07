################################################################################
# Copyright (c) 2015-2020, National Research Foundation (Square Kilometre Array)
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

from abc import ABC, abstractmethod
import asyncio
import time
from typing import List, Tuple, Dict, Generator, BinaryIO, Iterable, AsyncGenerator, Optional, Union

from ..utils import KeyType, _PathType
from ..backend import KeyUpdateBase


class Backend(ABC):
    """Low-level interface for telescope state backends.

    The backend interface does not deal with namespaces or encodings, which are
    handled by the frontend :class:`TelescopeState` class. A backend must be
    able to store the same types as :class:`~katsdptelstate.TelescopeState`,
    but keys and values will be :class:`bytes` rather than arbitrary Python
    objects.
    """

    @abstractmethod
    async def exists(self, key: bytes) -> bool:
        """Return if `key` is in the backend."""

    @abstractmethod
    async def keys(self, filter: bytes) -> List[bytes]:
        """Return all keys matching `filter`.

        The filter is a redis pattern. Backends might only support ``b'*'`` as
        a filter.
        """

    @abstractmethod
    async def delete(self, key: bytes) -> None:
        """Delete a key (no-op if it does not exist)"""

    @abstractmethod
    async def clear(self) -> None:
        """Remove all keys"""

    @abstractmethod
    async def key_type(self, key: bytes) -> Optional[KeyType]:
        """Get type of `key`, or ``None`` if it does not exist."""

    @abstractmethod
    async def set_immutable(self, key: bytes, value: bytes) -> Optional[bytes]:
        """Set the value of an immutable key.

        If the key already exists (and is immutable), returns the existing
        value and does not update it. Otherwise, returns ``None``.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is not immutable.
        """

    @abstractmethod
    async def get(self, key: bytes) -> Union[
            Tuple[None, None],
            Tuple[bytes, None],
            Tuple[bytes, float],
            Tuple[Dict[bytes, bytes], None]]:
        """Get the value and timestamp of a key.

        The return value depends on the key type:

        immutable
          The value.
        mutable
          The most recent value.
        indexed
          A dictionary of all values (with undefined iteration order).
        absent
          None

        The timestamp will be ``None`` for types other than mutable.
        """

    @abstractmethod
    async def add_mutable(self, key: bytes, value: bytes, timestamp: float) -> None:
        """Set a (value, timestamp) pair in a mutable key.

        The `timestamp` will be a non-negative float value.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is not mutable
        """

    @abstractmethod
    async def set_indexed(self, key: bytes, sub_key: bytes, value: bytes) -> Optional[bytes]:
        """Add value in an indexed immutable key.

        If the sub-key already exists, returns the existing value and does not
        update it. Otherwise, returns ``None``.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is not indexed.
        """

    @abstractmethod
    async def get_indexed(self, key: bytes, sub_key: bytes) -> Optional[bytes]:
        """Get the value of an indexed immutable key.

        Returns ``None`` if the key exists but the sub-key does not exist.

        Raises
        ------
        KeyError
            If the key does not exist.
        ImmutableKeyError
            If the key exists and is not indexed.
        """

    @abstractmethod
    async def get_range(self, key: bytes, start_time: float, end_time: float,
                        include_previous: bool,
                        include_end: bool) -> Optional[List[Tuple[bytes, float]]]:
        """Obtain a range of values from a mutable key.

        If the key does not exist, returns None.

        Parameters
        ----------
        key : bytes
            Key to search
        start_time : float
            Start of the range (inclusive).
        end_time : float
            End of the range. It is guaranteed to be non-negative.
        include_previous : bool
            If true, also return the last entry prior to `start_time`.
        include_end : bool
            If true, treat `end_time` as inclusive, otherwise exclusive.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is not mutable
        """

    @abstractmethod
    async def dump(self, key: bytes) -> Optional[bytes]:
        """Return a key in the same format as the Redis DUMP command, or None if not present."""

    async def monitor_keys(self, keys: Iterable[bytes]) -> AsyncGenerator[KeyUpdateBase, None]:
        """Report changes to keys in `keys`.

        Returns an asynchronous iterator that yields an infinite stream of
        update notifications. When no longer needed it should be closed.
        """
        # This is a valid but usually suboptimal implementation
        while True:
            await asyncio.sleep(1)
            yield KeyUpdateBase()
        timeout = yield None
        while True:
            assert timeout is not None
            time.sleep(timeout)
            yield KeyUpdateBase()
