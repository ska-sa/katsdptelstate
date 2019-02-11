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

from __future__ import print_function, division, absolute_import

import bisect
import re
import logging
import os

try:
    from rdbtools import RdbParser, RdbCallback
except ImportError as _rdbtools_import_error:
    RdbCallback = object     # So that _Callback can still be defined
    RdbParser = None

from .telescope_state import Backend, ImmutableKeyError


_INF = float('inf')
logger = logging.getLogger(__name__)


def _compile_pattern(pattern):
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
    pattern = pattern.decode('latin-1')
    parts = ['^']
    i = 0
    L = len(pattern)
    while i < L:
        c = pattern[i]
        i += 1
        if c == '?':
            parts.append('.')
        elif c == '*':
            parts.append('.*')
        elif c == '\\':
            if i == L:
                i -= 1
            parts.append(re.escape(pattern[i]))
            i += 1
        elif c == '[':
            parts.append('[')
            if i < L and pattern[i] == '^':
                i += 1
                parts.append('^')
            parts_len = len(parts)  # To detect if anything was added
            while i < L:
                if pattern[i] == '\\' and i + 1 < L:
                    i += 1
                    parts.append(re.escape(pattern[i]))
                elif pattern[i] == ']':
                    i += 1
                    break
                elif i + 2 < L and pattern[i + 1] == '-':
                    start = pattern[i]
                    end = pattern[i + 2]
                    if start > end:
                        start, end = end, start
                    parts.append(re.escape(start) + '-' + re.escape(end))
                    i += 2
                else:
                    parts.append(re.escape(pattern[i]))
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


class _Callback(RdbCallback):
    def __init__(self, data):
        super(_Callback, self).__init__(string_escape=None)
        self.data = data
        self.n_keys = 0

    def set(self, key, value, expiry, info):
        self.data[key] = value
        self.n_keys += 1

    def start_sorted_set(self, key, length, expiry, info):
        self.data[key] = []
        self.n_keys += 1

    def zadd(self, key, score, member):
        self.data[key].append(member)

    def end_sorted_set(self, key):
        self.data[key].sort()


class MemoryBackend(Backend):
    """Telescope state backend that keeps data in memory.

    It is optimised for read-only use, loading data from a .rdb file.
    Write operations are supported only to facilitate testing, but are not
    intended for production use. For that, use a :class:`.RedisBackend`
    with an in-memory Redis emulation. The :meth:`monitor_keys`,
    :meth:`send_message` and :meth:`get_message` methods are not implemented.

    Mutable keys are stored as sorted lists, and encode timestamps in-place
    using the same packing as :class:`.RedisBackend`.
    """
    def __init__(self):
        self._data = {}

    def load_from_file(self, filename):
        if RdbParser is None:
            raise _rdbtools_import_error
        logger.debug("Loading data from RDB dump of %d bytes", os.path.getsize(filename))
        callback = _Callback(self._data)
        parser = RdbParser(callback)
        parser.parse(filename)
        return callback.n_keys

    def __contains__(self, key):
        return key in self._data

    def keys(self, filter):
        if filter == b'*':
            return list(self._data.keys())
        else:
            regex = _compile_pattern(filter)
            return [key for key in self._data.keys() if regex.match(key)]

    def delete(self, key):
        self._data.pop(key, None)

    def clear(self):
        self._data.clear()

    def is_immutable(self, key):
        return isinstance(self._data[key], bytes)

    def set_immutable(self, key, value):
        old = self._data.get(key)
        if old is None:
            self._data[key] = value
            return None
        elif isinstance(old, bytes):
            return old
        else:
            raise ImmutableKeyError

    def get_immutable(self, key):
        value = self._data.get(key)
        if isinstance(value, list):
            raise ImmutableKeyError
        return value

    def add_mutable(self, key, value, timestamp):
        str_val = self.pack_timestamp(timestamp) + value
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

    @classmethod
    def _bisect(cls, items, timestamp, is_end, include_end=False):
        packed = cls.pack_query_timestamp(timestamp, is_end, include_end)
        if packed == b'-':
            return 0
        elif packed == b'+':
            return len(items)
        else:
            # pack_query_timestamp adds a prefix of [ or (, which we don't need
            return bisect.bisect_left(items, packed[1:])

    def get_range(self, key, start_time, end_time, include_previous, include_end):
        items = self._data.get(key)
        if items is None:
            return None
        elif isinstance(items, bytes):
            raise ImmutableKeyError

        start_pos = self._bisect(items, start_time, False)
        if include_previous and start_pos > 0:
            start_pos -= 1
        end_pos = self._bisect(items, end_time, True, include_end)
        return [self.split_timestamp(value) for value in items[start_pos:end_pos]]
