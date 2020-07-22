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

import logging
import os
from typing import Optional, TypeVar

from .rdb_utility import encode_len
from .utils import display_str, _PathType

# Basic RDB header. First 5 bytes are the standard REDIS magic
# Next 4 bytes store the RDB format version number (6 in this case)
# 0xFE flags the start of the DB selector (which is set to 0)
RDB_HEADER = b'REDIS0006\xfe\x00'

RDB_TERMINATOR = b'\xFF'

# Fast CRC-64 implementations on Python seem rare
# RDB is happy with zero checksum - we will protect the RDB dump in other ways
RDB_CHECKSUM = b'\x00\x00\x00\x00\x00\x00\x00\x00'

logger = logging.getLogger(__name__)

_T = TypeVar('_T', bound='RDBWriterBase')


def encode_item(key: bytes, dumped_value: bytes) -> bytes:
    """Encode key and corresponding DUMPed value to RDB format.

    The first byte indicates the encoding used for the value of this key. This
    is essentially just the Redis type. Subsequent bytes represent the key name,
    which consists of a length encoding followed by the actual byte
    representation of the string. Thereafter follows the value, encoded
    according to its appropriate schema. As a shortcut, the Redis DUMP command
    is used to generate the encoded value.

    Note: Redis provides a mechanism for optional key expiry, which we ignore here.
    """
    try:
        key_len = encode_len(len(key))
    except ValueError as exc:
        err = ValueError('Failed to encode key length: {}'.format(exc))
        raise err from None
    # The DUMPed value includes a leading type descriptor,
    # the encoded value itself (including length specifier),
    # a trailing version specifier (2 bytes) and finally an 8 byte checksum.
    # The version specified and checksum are discarded.
    key_type = dumped_value[:1]
    encoded_value = dumped_value[1:-10]
    return key_type + key_len + key + encoded_value


class RDBWriterBase:
    """RDB file resource that stores keys from one (or more) Redis DBs.

    Upon initialisation this opens the RDB file and writes the header.
    It is necessary to call :meth:`close` to write the trailer of the file
    and to close it properly (or use this object as a context manager).

    Parameters
    ----------
    filename : path-like
        Destination filename. Will be opened in 'wb'.

    Attributes
    ----------
    keys_written : int
        Number of keys written to the file.
    keys_failed : int
        Number of keys that failed to be written.
    """
    def __init__(self, filename: _PathType) -> None:
        self.filename = filename
        self.keys_written = 0
        self.keys_failed = 0
        self._fileobj = open(filename, 'wb')
        try:
            self._fileobj.write(RDB_HEADER)
        except Exception:
            # Make sure that file is at least closed if header failed
            self._fileobj.close()
            raise

    def __enter__(self: _T) -> _T:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def write_key(self, key: bytes, dumped_value: Optional[bytes]) -> None:
        """Used by sub-classes to write a single key to the file."""
        try:
            if not dumped_value:
                raise KeyError('Key not found in Redis')
            encoded_str = encode_item(key, dumped_value)
        except (ValueError, KeyError) as e:
            self.keys_failed += 1
            logger.error("Failed to save key %s: %s", display_str(key), e)
            return
        self._fileobj.write(encoded_str)
        self.keys_written += 1

    def close(self) -> None:
        """Close off RDB file and delete it if it contains no keys."""
        try:
            self._fileobj.write(RDB_TERMINATOR + RDB_CHECKSUM)
        finally:
            self._fileobj.close()
            if not self.keys_written:
                logger.error("No valid keys found - removing empty file")
                os.remove(self.filename)
