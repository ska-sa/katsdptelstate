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

from abc import ABC, abstractmethod
import time


class Backend(ABC):
    """Low-level interface for telescope state backends.

    The backend interface does not deal with namespaces or encodings, which are
    handled by the frontend :class:`TelescopeState` class. A backend must be
    able to store

    - immutables: key-value pairs (both :class:`bytes`), whose value cannot
      change once set.

    - mutables: a key associated with a set of (value, timestamp) pairs, where
      the timestamps are non-negative finite floats and the values are
      :class:`bytes`.
    """

    @abstractmethod
    def load_from_file(self, file):
        """Implements :meth:`TelescopeState.load_from_file`."""

    @abstractmethod
    def __contains__(self, key):
        """Return if `key` is in the backend."""

    @abstractmethod
    def keys(self, filter):
        """Return all keys matching `filter`.

        The filter is a redis pattern. Backends might only support ``b'*'`` as
        a filter.
        """

    @abstractmethod
    def delete(self, key):
        """Delete a key (no-op if it does not exist)"""

    @abstractmethod
    def clear(self):
        """Remove all keys"""

    @abstractmethod
    def key_type(self, key):
        """Get type of `key`, or ``None`` if it does not exist."""

    @abstractmethod
    def set_immutable(self, key, value):
        """Set the value of an immutable key.

        If the key already exists (and is immutable), returns the existing
        value and does not update it. Otherwise, returns ``None``.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is not immutable.
        """

    @abstractmethod
    def get(self, key):
        """Get the value and timestamp of a key.

        If the key is mutable, returns the most recent value. If it
        is immutable, returns the value with a timestamp of ``None``.

        If the key does not exist, both the value and timestamp will be
        ``None``.
        """

    @abstractmethod
    def add_mutable(self, key, value, timestamp):
        """Set a (value, timestamp) pair in a mutable key.

        The `timestamp` will be a non-negative float value.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is immutable
        """

    @abstractmethod
    def set_indexed(self, key, sub_key, value):
        """Add value in an indexed immutable key.

        If the sub-key already exists, returns the existing value and does not
        update it. Otherwise, returns ``None``.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is not plain immutable.
        """

    @abstractmethod
    def get_indexed(self, key, sub_key):
        """Get the value of an indexed immutable key.

        Returns ``None`` if the key or sub-key does not exist.

        Raises
        ------
        ImmutableKeyError
            If the key is mutable
        """

    @abstractmethod
    def get_range(self, key, start_time, end_time, include_previous, include_end):
        """Obtain a range of values from a mutable key.

        If the key does not exist, returns None.

        Parameters
        ----------
        key : bytes
            Key to search
        start_time : float
            Start of the range (inclusive).
        end_time : float
            End of the range. It is guaranteed to be non-negative.
        include_previous : bool
            If true, also return the last entry prior to `start_time`.
        include_end : bool
            If true, treat `end_time` as inclusive, otherwise exclusive.

        Raises
        ------
        ImmutableKeyError
            If the key exists and is immutable
        """

    @abstractmethod
    def dump(self, key):
        """Return a key in the same format as the Redis DUMP command, or None if not present"""

    def monitor_keys(self, keys):
        """Report changes to keys in `keys`.

        Returns a generator. The first yield from the generator is a no-op.
        After that, the caller sends a timeout and gets back an update event.
        Each update event is a tuple, of the form (key, value) or (key, value,
        timestamp) depending on whether the update is to an immutable or a
        mutable key; or of the form (key,) to indicate that a key may have
        been updated but there is insufficient information to provide the
        latest value. If there is no event within the timeout, returns
        ``None``.

        It is acceptable (but undesirable) for this function to miss the
        occasional update e.g. due to a network connection outage. The caller
        takes care to use a low timeout and retry rather than blocking for
        long periods.

        The generator runs until it is closed.
        """
        # This is a valid but usually suboptimal implementation
        timeout = yield None
        while True:
            time.sleep(timeout)
            timeout = yield (keys[0],)
