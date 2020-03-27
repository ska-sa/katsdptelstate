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

import struct
import math
import functools

import six


# Behave gracefully in case someone uses non-UTF-8 binary in a key on PY3
ensure_str = functools.partial(six.ensure_str, errors='surrogateescape')
ensure_binary = functools.partial(six.ensure_binary, errors='surrogateescape')


def display_str(s):
    """Return most human-readable and yet accurate version of *s*."""
    try:
        return '{!r}'.format(six.ensure_str(s))
    except UnicodeDecodeError:
        return f'{s!r}'


def pack_query_timestamp(time, is_end, include_end=False):
    """Create a query value for a ZRANGEBYLEX query.

    When packing the time for the start of a range, set `is_end` and
    `include_end` to False. When packing the time for the end of a range,
    set `is_end` to True, and `include_end` indicates whether the endpoint
    is inclusive. The latter is implemented by incrementing the time by the
    smallest possible amount and then treating it as exclusive.
    """
    if time == math.inf:
        # The special positively infinite string represents the end of time
        return b'+'
    elif time < 0.0 or (time == 0.0 and not include_end):
        # The special negatively infinite string represents the dawn of time
        return b'-'
    else:
        packed_time = pack_timestamp(time)
        if include_end:
            # Increment to the next possible encoded value. Note that this
            # cannot overflow because the sign bit is initially clear.
            packed_time = struct.pack('>Q', struct.unpack('>Q', packed_time)[0] + 1)
        return (b'(' if is_end else b'[') + packed_time


def pack_timestamp(timestamp):
    """Encode a timestamp to a bytes that sorts correctly"""
    assert timestamp >= 0
    # abs forces -0 to +0, which encodes differently
    return struct.pack('>d', abs(timestamp))


def split_timestamp(packed):
    """Split out the value and timestamp from a packed item.

    The item contains 8 bytes with the timestamp in big-endian IEEE-754
    double precision, followed by the value.
    """
    assert len(packed) >= 8
    timestamp = struct.unpack('>d', packed[:8])[0]
    return (packed[8:], timestamp)
