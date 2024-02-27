################################################################################
# Copyright (c) 2015-2020, National Research Foundation (SARAO)
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
from typing import List, Tuple, Dict, Generator, BinaryIO, Iterable, Optional, Union

from .utils import KeyType, _PathType


class KeyUpdateBase:
    """Indicates that the caller of the monitor should check again.

    This base class contains no information about what changed in the
    database. Sub-classes contain more specific information.
    """
    pass


class KeyUpdate(ABC, KeyUpdateBase):
    """Update notification for a specific key.

    This is a base class for type-specific notification classes and should not
    be instantiated directly.
    """

    def __init__(self, key: bytes, value: bytes) -> None:
        self.key = key
        self.value = value

    @property
    @abstractmethod
    def key_type(self) -> KeyType:
        pass       # noqa: cover


class MutableKeyUpdate(KeyUpdate):
    """Update notification for a mutable key."""

    def __init__(self, key: bytes, value: bytes, timestamp: float) -> None:
        super().__init__(key, value)
        self.timestamp = timestamp

    @property
    def key_type(self) -> KeyType:
        return KeyType.MUTABLE


class ImmutableKeyUpdate(KeyUpdate):
    """Update notification for an immutable key."""

    @property
    def key_type(self) -> KeyType:
        return KeyType.IMMUTABLE


class IndexedKeyUpdate(KeyUpdate):
    """Update notification for an indexed key."""

    def __init__(self, key: bytes, sub_key: bytes, value: bytes):
        super().__init__(key, value)
        self.sub_key = sub_key

    @property
    def key_type(self) -> KeyType:
        return KeyType.INDEXED


class Backend(ABC):
    """Low-level interface for telescope state backends.

    The backend interface does not deal with namespaces or encodings, which are
    handled by the frontend :class:`TelescopeState` class. A backend must be
    able to store the same types as :class:`~katsdptelstate.TelescopeState`,
    but keys and values will be :class:`bytes` rather than arbitrary Python
    objects.
    """

    @abstractmethod
    def load_from_file(self, file: Union[_PathType, BinaryIO]) -> int:
        """Implements :meth:`TelescopeState.load_from_file`."""

    @abstractmethod
    def __contains__(self, key: bytes) -> bool:
        """Return if `key` is in the backend."""

    @abstractmethod
    def keys(self, filter: bytes) -> List[bytes]:
        """Return all keys matching `filter`.

        The filter is a redis pattern. Backends might only support ``b'*'`` as
        a filter.
        """

    @abstractmethod
    def delete(self, key: bytes) -> None:
        """Delete a key (no-op if it does not exist)"""

    @abstractmethod
    def clear(self) -> None:
        """Remove all keys"""

    @abstractmethod
    def key_type(self, key: bytes) -> Optional[KeyType]:
        """Get type of `key`, or ``None`` if it does not exist."""

    @abstractmethod
    def set_immutable(self, key: bytes, value: bytes) -> Optional[bytes]:
        """Set the value of an immutable key.

        If the key already exists (and is immutable), returns the existing
        value and does not update it. Otherwise, returns ``None``.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is not immutable.
        """

    @abstractmethod
    def get(self, key: bytes) -> Union[
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
    def add_mutable(self, key: bytes, value: bytes, timestamp: float) -> None:
        """Set a (value, timestamp) pair in a mutable key.

        The `timestamp` will be a non-negative float value.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is not mutable
        """

    @abstractmethod
    def set_indexed(self, key: bytes, sub_key: bytes, value: bytes) -> Optional[bytes]:
        """Add value in an indexed immutable key.

        If the sub-key already exists, returns the existing value and does not
        update it. Otherwise, returns ``None``.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is not indexed.
        """

    @abstractmethod
    def get_indexed(self, key: bytes, sub_key: bytes) -> Optional[bytes]:
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
    def get_range(self, key: bytes, start_time: float, end_time: float,
                  include_previous: bool, include_end: bool) -> Optional[List[Tuple[bytes, float]]]:
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
    def dump(self, key: bytes) -> Optional[bytes]:
        """Return a key in the same format as the Redis DUMP command, or None if not present."""

    @abstractmethod
    def monitor_keys(self, keys: Iterable[bytes]) \
            -> Generator[Optional[KeyUpdateBase], Optional[float], None]:
        """Report changes to keys in `keys`.

        Returns a generator. The first yield from the generator may be either
        ``None`` or an instance of :class:`KeyUpdateBase`; in the latter case,
        the caller should immediately check the condition again. After that,
        the caller sends a timeout and gets back an update event (of type
        :class:`KeyUpdateBase` or a subclass). If there is no event within the
        timeout, returns ``None``.

        It is acceptable (but undesirable) for this function to miss the
        occasional update e.g. due to a network connection outage. The caller
        takes care to use a low timeout and retry rather than blocking for
        long periods.

        The generator runs until it is closed.
        """
        # Just so that this is recognised as a generator
        yield None        # pragma: nocover
