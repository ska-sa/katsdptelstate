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

import logging
from typing import Tuple, Optional, Union, Generic, TypeVar, Any

from .utils import ensure_str, ensure_binary
from .encoding import decode_value, equal_encoded_values
from .errors import ImmutableKeyError, DecodeError


logger = logging.getLogger(__name__)
_B = TypeVar('_B')    # Generic backend
_T = TypeVar('_T', bound='TelescopeStateBase')
_Key = Union[bytes, str]


def check_immutable_change(key: str, old_enc: bytes, new_enc: bytes, new: Any) -> None:
    """Check if an immutable is being changed to the same value.

    If not, raise :exc:`ImmutableKeyError`, otherwise just log a message.
    This is intended to be called by subclasses.

    Parameters
    ----------
    key : str
        Human-readable version of the key
    old_enc : bytes
        Previous value, encoded
    new_enc : bytes
        New value, encoded
    new
        New value, prior to encoding
    """
    try:
        if not equal_encoded_values(new_enc, old_enc):
            raise ImmutableKeyError(
                'Attempt to change value of immutable key {} from '
                '{!r} to {!r}'.format(key, decode_value(old_enc), new))
        else:
            logger.info('Attribute {} updated with the same value'
                        .format(key))
    except DecodeError as error:
        raise ImmutableKeyError(
            'Attempt to set value of immutable key {} to {!r} but '
            'failed to decode the previous value to compare: {}'
            .format(key, new, error)) from error


class TelescopeStateBase(Generic[_B]):
    """Base class for synchronous and asynchronous telescope state classes.

    It contains parts common to :class:`katsdptelstate.TelescopeState` and
    :class:`katsdptelstate.aio.TelescopeState`, mainly relating to setting up
    namespaces.
    """

    SEPARATOR = '_'
    SEPARATOR_BYTES = b'_'

    def __init__(self: _T, backend: Optional[_B] = None,
                 prefixes: Tuple[_Key, ...] = (b'',),
                 base: Optional[_T] = None) -> None:
        if base is not None and backend is not None:
            raise ValueError('cannot specify both base and backend')
        if backend is not None:
            self._backend = backend
        elif base is not None:
            self._backend = base.backend
        else:
            raise ValueError('must specify either base or backend')
        # Ensure all prefixes are bytes internally for consistency
        self._prefixes = tuple(ensure_binary(prefix) for prefix in prefixes)

    @property
    def prefixes(self) -> Tuple[str, ...]:
        """The active key prefixes as a tuple of strings."""
        return tuple(ensure_str(prefix) for prefix in self._prefixes)

    @property
    def backend(self) -> _B:
        return self._backend

    @classmethod
    def join(cls, *names: str) -> str:
        """Join string components of key with supported separator."""
        return cls.SEPARATOR.join(names)

    def view(self: _T, name: _Key, add_separator: bool = True, exclusive: bool = False) -> _T:
        """Create a view with an extra name in the list of namespaces.

        Returns a new view with `name` added as the first prefix, or the
        only prefix if `exclusive` is true. If `name` is non-empty and does not
        end with the separator, it is added (unless `add_separator` is
        false).
        """
        name = ensure_binary(name)
        if name != b'' and name[-1:] != self.SEPARATOR_BYTES and add_separator:
            name += self.SEPARATOR_BYTES
        if exclusive:
            prefixes = (name,)    # type: Tuple[_Key, ...]
        else:
            prefixes = (name,) + self._prefixes
        return self.__class__(prefixes=prefixes, base=self)

    def root(self: _T) -> _T:
        """Create a view containing only the root namespace."""
        return self.__class__(base=self)
