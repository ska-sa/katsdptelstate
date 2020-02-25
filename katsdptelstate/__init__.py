################################################################################
# Copyright (c) 2015-2019, National Research Foundation (Square Kilometre Array)
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

import warnings

import six

from .telescope_state import (TelescopeState, ConnectionError, InvalidKeyError,
                              ImmutableKeyError, TimeoutError, CancelledError,
                              DecodeError, EncodeError, RdbParseError,
                              PICKLE_PROTOCOL, encode_value, decode_value, set_allow_pickle,
                              ALLOWED_ENCODINGS, ENCODING_DEFAULT,
                              ENCODING_PICKLE, ENCODING_MSGPACK)


if six.PY2:
    _PY2_WARNING = (
        "Python 2 has reached End-of-Life, and a future version of "
        "katsdptelstate will remove support for it. Please update your "
        "scripts to Python 3 as soon as possible."
    )
    warnings.warn(_PY2_WARNING, FutureWarning)


# BEGIN VERSION CHECK
# Get package version when locally imported from repo or via -e develop install
try:
    import katversion as _katversion
except ImportError:
    import time as _time
    __version__ = "0.0+unknown.{}".format(_time.strftime('%Y%m%d%H%M'))
else:
    __version__ = _katversion.get_version(__path__[0])
# END VERSION CHECK
