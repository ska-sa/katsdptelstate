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

import struct


def encode_len(length):
    """Encodes the specified length as 1,2 or 5 bytes of
       RDB specific length encoded byte.
       For values less than 64 (i.e two MSBs zero - encode directly in the byte)
       For values less than 16384 use two bytes, leading MSBs are 01 followed by 14 bits encoding the value
       For values less than (2^32 -1) use 5 bytes, leading MSBs are 10. Length encoded only in the lowest 32 bits.
    """
    if length > (2**32 - 1):
        raise ValueError("Cannot encode item of length {} as it is greater than 2^32 -1".format(length))
    if length < 64:
        return struct.pack('B', length)
    if length < 16384:
        return struct.pack(">h", 0x4000 + length)
    return struct.pack('>q', 0x8000000000 + length)[3:]


def encode_prev_length(length):
    """Special helper for zset previous entry lengths.
       If length < 253 then use 1 byte directly, otherwise
       set first byte to 254 and add 4 trailing bytes as an
       unsigned integer.
    """
    if length < 254:
        return struct.pack('B', length)
    return b'\xfe' + struct.pack(">q", length)
