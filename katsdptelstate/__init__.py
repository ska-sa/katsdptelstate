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

from .errors import (ConnectionError, InvalidKeyError,                  # noqa: F401
                     ImmutableKeyError, TimeoutError, CancelledError,
                     DecodeError, EncodeError, RdbParseError)
from .telescope_state import TelescopeState                             # noqa: F401
from .encoding import (PICKLE_PROTOCOL, encode_value, decode_value,     # noqa: F401
                       set_allow_pickle,
                       ALLOWED_ENCODINGS, ENCODING_DEFAULT,
                       ENCODING_PICKLE, ENCODING_MSGPACK)


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
