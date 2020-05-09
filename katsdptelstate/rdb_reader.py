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
import os.path
from typing import Optional, Union, BinaryIO, cast

from rdbtools import RdbParser, RdbCallback

from .errors import RdbParseError
from .utils import _PathType


logger = logging.getLogger(__name__)


class BackendCallback(RdbCallback):
    """A callback adapter that stores keys in backend as RDB file is parsed."""

    def __init__(self) -> None:
        super().__init__(string_escape=None)
        # Counter keeping track of number of keys inserted into backend
        self.n_keys = 0
        # Flag that helps to disambiguate callback errors from parser errors
        self.client_busy = False


def _parse_rdb_file(parser: RdbParser, callback: BackendCallback, fd: BinaryIO,
                    filename: Optional[_PathType] = None) -> None:
    """Apply RDB parser to file descriptor, raising RdbParseError on error."""
    try:
        parser.parse_fd(fd)
    except Exception as exc:
        # Don't remap exception to RdbParseError if it originates from callback
        if callback.client_busy:
            raise
        raise RdbParseError(filename) from exc


def load_from_file(callback: BackendCallback, file: Union[_PathType, BinaryIO]) -> int:
    """Load keys from the specified RDB-compatible dump file into backend.

    Parameters
    ----------
    callback : :class:`katsdptelstate.rdb_reader.BackendCallback`
        Backend-specific callback that stores keys as RDB file is parsed
    file : str or file-like object
        Filename of .rdb file to import, or object representing contents of RDB

    Returns
    -------
    int
        Number of keys loaded into backend

    Raises
    ------
    RdbParseError
        If `file` does not represent a valid RDB file
    """
    size = 'unknown'                      # type: object
    try:
        size = os.path.getsize(cast(_PathType, file))
    except TypeError:
        # One could maybe seek() and tell() on file object but is it worth it?
        pass
    logger.debug("Loading data from RDB dump of %s bytes", size)
    parser = RdbParser(callback)
    try:
        fd = open(cast(_PathType, file), 'rb')
    except TypeError:
        _parse_rdb_file(parser, callback, cast(BinaryIO, file))
    else:
        with fd:
            _parse_rdb_file(parser, callback, fd, cast(_PathType, file))
    return callback.n_keys
