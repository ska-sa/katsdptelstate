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

from typing import Optional

from .utils import _PathType


class TelstateError(RuntimeError):
    """Base class for errors from this package."""


class ConnectionError(TelstateError):
    """The initial connection to the Redis server failed."""


class RdbParseError(TelstateError):
    """Error parsing RDB file."""

    def __init__(self, filename: Optional[_PathType] = None) -> None:
        self.filename = filename

    def __str__(self) -> str:
        name = repr(self.filename) if self.filename else 'object'
        return 'Invalid RDB file {}'.format(name)


class InvalidKeyError(TelstateError):
    """A key collides with a class attribute.

    This is kept only for backwards compatibility. It is no longer considered an error.
    """


class InvalidTimestampError(TelstateError):
    """Negative or non-finite timestamp"""


class ImmutableKeyError(TelstateError):
    """An attempt was made to modify an immutable key"""


class TimeoutError(TelstateError):
    """A wait for a key timed out"""


class CancelledError(TelstateError):
    """A wait for a key was cancelled"""


class DecodeError(ValueError, TelstateError):
    """An encoded value found in telstate could not be decoded"""


class EncodeError(ValueError, TelstateError):
    """A value could not be encoded"""
