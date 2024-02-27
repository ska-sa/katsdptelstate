################################################################################
# Copyright (c) 2019-2020, National Research Foundation (Square Kilometre Array)
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

import bisect
import re
import logging
import contextlib
import threading
from datetime import datetime
from typing import Pattern, List, Tuple, Dict, Generator, Iterable, BinaryIO, Union, Optional

from . import utils
from .utils import _PathType
from .backend import Backend, KeyUpdateBase
from .errors import ImmutableKeyError
from .rdb_utility import dump_string, dump_zset, dump_hash
try:
    from . import rdb_reader
    from .rdb_reader import BackendCallback
except ImportError as _rdb_reader_import_error:   # noqa: F841
    rdb_reader = None            # type: ignore
    BackendCallback = object     # type: ignore   # So that MemoryCallback can still be defined


logger = logging.getLogger(__name__)
_Value = Union[bytes, Dict[bytes, bytes], List[bytes]]


def _compile_pattern(pattern: bytes) -> Pattern[bytes]:
    """Compile a glob pattern (e.g. for keys) to a bytes regex.

    fnmatch.fnmatchcase doesn't work for this, because it uses different
    escaping rules to redis, uses ! instead of ^ to negate a character set,
    and handles invalid cases (such as a [ without a ]) differently. This
    implementation was written by studying the redis implementation.
    """
    # This is copy-pasted from fakeredis. It was entirely written by
    # Bruce Merry, so the fakeredis license terms need not apply.

    # It's easier to work with text than bytes, because indexing bytes
    # doesn't behave the same in Python 3. Latin-1 will round-trip safely.
    pattern_str = pattern.decode('latin-1')
    parts = ['^']
    i = 0
    L = len(pattern_str)
    while i < L:
        c = pattern_str[i]
        i += 1
        if c == '?':
            parts.append('.')
        elif c == '*':
            parts.append('.*')
        elif c == '\\':
            if i == L:
                i -= 1
            parts.append(re.escape(pattern_str[i]))
            i += 1
        elif c == '[':
            parts.append('[')
            if i < L and pattern_str[i] == '^':
                i += 1
                parts.append('^')
            parts_len = len(parts)  # To detect if anything was added
            while i < L:
                if pattern_str[i] == '\\' and i + 1 < L:
                    i += 1
                    parts.append(re.escape(pattern_str[i]))
                elif pattern_str[i] == ']':
                    i += 1
                    break
                elif i + 2 < L and pattern_str[i + 1] == '-':
                    start = pattern_str[i]
                    end = pattern_str[i + 2]
                    if start > end:
                        start, end = end, start
                    parts.append(re.escape(start) + '-' + re.escape(end))
                    i += 2
                else:
                    parts.append(re.escape(pattern_str[i]))
                i += 1
            if len(parts) == parts_len:
                if parts[-1] == '[':
                    # Empty group - will never match
                    parts[-1] = '(?:$.)'
                else:
                    # Negated empty group - matches any character
                    assert parts[-1] == '^'
                    parts.pop()
                    parts[-1] = '.'
            else:
                parts.append(']')
        else:
            parts.append(re.escape(c))
    parts.append('\\Z')
    regex = ''.join(parts).encode('latin-1')
    return re.compile(regex, re.S)


class MemoryCallback(BackendCallback):
    """RDB callback that stores keys in :class:`~katsdptelstate.memory.MemoryBackend`."""

    def __init__(self, data: Dict[bytes, _Value]) -> None:
        super().__init__()
        self.data = data

    def set(self, key: bytes, value: bytes, expiry: Optional[datetime], info: dict) -> None:
        self.data[key] = value
        self.n_keys += 1

    def start_sorted_set(self, key: bytes, length: int,
                         expiry: Optional[datetime], info: dict) -> None:
        self.data[key] = []
        self.n_keys += 1

    def zadd(self, key: bytes, score: float, member: bytes) -> None:
        self.data[key].append(member)       # type: ignore

    def end_sorted_set(self, key: bytes) -> None:
        self.data[key].sort()               # type: ignore

    def start_hash(self, key: bytes, length: int, expiry: Optional[datetime], info: dict) -> None:
        self.data[key] = {}
        self.n_keys += 1

    def hset(self, key: bytes, field: bytes, value: bytes) -> None:
        self.data[key][field] = value       # type: ignore


class MemoryBackend(Backend):
    """Telescope state backend that keeps data in memory.

    It is optimised for read-only use, loading data from a .rdb file.
    Write operations are supported only to facilitate testing, but are not
    intended for production use. For that, use a
    :class:`~katsdptelstate.redis.RedisBackend` with an in-memory Redis
    emulation.

    Mutable keys are stored as sorted lists, and encode timestamps in-place
    using the same packing as :class:`~katsdptelstate.redis.RedisBackend`.
    """
    def __init__(self) -> None:
        self._data = {}       # type: Dict[bytes, _Value]
        self._generation = 0  # incremented each time a change is made
        self._condition = threading.Condition()

    @contextlib.contextmanager
    def _write(self) -> Generator[None, None, None]:
        """Context manager to use for writes."""
        with self._condition:
            yield
            self._generation += 1
            self._condition.notify_all()

    @contextlib.contextmanager
    def _read(self) -> Generator[None, None, None]:
        """Context manager to use for reads."""
        with self._condition:
            yield

    def load_from_file(self, file: Union[_PathType, BinaryIO]) -> int:
        if rdb_reader is None:
            raise _rdb_reader_import_error   # noqa: F821
        with self._write():
            return rdb_reader.load_from_file(MemoryCallback(self._data), file)

    def __contains__(self, key: bytes) -> bool:
        with self._read():
            return key in self._data

    def keys(self, filter: bytes) -> List[bytes]:
        with self._read():
            if filter == b'*':
                return list(self._data.keys())
            else:
                regex = _compile_pattern(filter)
                return [key for key in self._data.keys() if regex.match(key)]

    def delete(self, key: bytes) -> None:
        with self._write():
            self._data.pop(key, None)

    def clear(self) -> None:
        with self._write():
            self._data.clear()

    def key_type(self, key: bytes) -> Optional[utils.KeyType]:
        with self._read():
            value = self._data.get(key)
            if value is None:
                return None
            elif isinstance(value, bytes):
                return utils.KeyType.IMMUTABLE
            elif isinstance(value, dict):
                return utils.KeyType.INDEXED
            elif isinstance(value, list):
                return utils.KeyType.MUTABLE
            else:
                assert False

    def set_immutable(self, key: bytes, value: bytes) -> Optional[bytes]:
        with self._write():
            old = self._data.get(key)
            if old is None:
                self._data[key] = value
                return None
            elif isinstance(old, bytes):
                return old
            else:
                raise ImmutableKeyError

    def get(self, key: bytes) -> Union[
            Tuple[None, None],
            Tuple[bytes, None],
            Tuple[bytes, float],
            Tuple[Dict[bytes, bytes], None]]:
        with self._read():
            value = self._data.get(key)
            if isinstance(value, list):
                return utils.split_timestamp(value[-1])
            elif isinstance(value, dict):
                # Have to copy because once we exit the _read context manager
                # the dictionary could be mutated under us.
                return dict(value), None
            else:
                # mypy is not smart enough to figure out that this is compatible
                return value, None     # type: ignore

    def add_mutable(self, key: bytes, value: bytes, timestamp: float) -> None:
        with self._write():
            str_val = utils.pack_timestamp(timestamp) + value
            items = self._data.get(key)
            if items is None:
                self._data[key] = [str_val]
            elif isinstance(items, list):
                # To match redis behaviour, we need to avoid inserting the item
                # if it already exists.
                pos = bisect.bisect_left(items, str_val)
                if pos == len(items) or items[pos] != str_val:
                    items.insert(pos, str_val)
            else:
                raise ImmutableKeyError

    def set_indexed(self, key: bytes, sub_key: bytes, value: bytes) -> Optional[bytes]:
        with self._write():
            item = self._data.setdefault(key, {})
            if not isinstance(item, dict):
                raise ImmutableKeyError
            if sub_key in item:
                return item[sub_key]
            else:
                item[sub_key] = value
                return None

    def get_indexed(self, key: bytes, sub_key: bytes) -> Optional[bytes]:
        with self._read():
            item = self._data[key]
            if not isinstance(item, dict):
                raise ImmutableKeyError
            return item.get(sub_key)

    @classmethod
    def _bisect(cls, items: List[bytes], timestamp: float,
                is_end: bool, include_end: bool = False) -> int:
        packed = utils.pack_query_timestamp(timestamp, is_end, include_end)
        if packed == b'-':
            return 0
        elif packed == b'+':
            return len(items)
        else:
            # pack_query_timestamp adds a prefix of [ or (, which we don't need
            return bisect.bisect_left(items, packed[1:])

    def get_range(self, key: bytes, start_time: float, end_time: float,
                  include_previous: bool, include_end: bool) -> Optional[List[Tuple[bytes, float]]]:
        with self._read():
            items = self._data.get(key)
            if items is None:
                return None
            elif not isinstance(items, list):
                raise ImmutableKeyError

            start_pos = self._bisect(items, start_time, False)
            if include_previous and start_pos > 0:
                start_pos -= 1
            end_pos = self._bisect(items, end_time, True, include_end)
            return [utils.split_timestamp(value) for value in items[start_pos:end_pos]]

    def dump(self, key: bytes) -> Optional[bytes]:
        with self._read():
            value = self._data.get(key)
            if value is None:
                return None
            elif isinstance(value, bytes):
                return dump_string(value)
            elif isinstance(value, list):
                return dump_zset(value)
            elif isinstance(value, dict):
                return dump_hash(value)
            else:
                assert False

    def monitor_keys(self, keys: Iterable[bytes]) \
            -> Generator[Optional[KeyUpdateBase], Optional[float], None]:
        with self._condition:
            generation = self._generation
        # The condition may have been satisfied before we recorded the
        # generation, so caller should immediately check.
        timeout = yield KeyUpdateBase()
        while True:
            with self._condition:
                assert timeout is not None
                updated = self._condition.wait_for(lambda: self._generation > generation, timeout)
                ret = KeyUpdateBase() if updated else None
                generation = self._generation
            timeout = yield ret
