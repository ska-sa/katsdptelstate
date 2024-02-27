################################################################################
# Copyright (c) 2015-2021, National Research Foundation (SARAO)
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

import time
import math
import logging
from typing import List, Tuple, Dict, Callable, Union, Optional, TypeVar, Any

from ..errors import ImmutableKeyError, InvalidTimestampError
from ..encoding import ENCODING_DEFAULT, ENCODING_MSGPACK, encode_value, decode_value
from ..utils import ensure_str, ensure_binary, display_str, KeyType
from .backend import Backend
from ..backend import KeyUpdate, KeyUpdateBase, IndexedKeyUpdate
from ..telescope_state_base import TelescopeStateBase, check_immutable_change
from .memory import MemoryBackend


logger = logging.getLogger(__name__)
_Key = Union[bytes, str]
_T = TypeVar('_T', bound='TelescopeState')


class TelescopeState(TelescopeStateBase[Backend]):
    """Interface to attributes and sensors stored in a database.

    Refer to the README for a description of the types of keys supported.

    A Redis database used with this class must *only* be used with this class,
    as it assumes that all keys were encoded by this package. It should
    however be robust to malicious data, failing gracefully rather than
    executing arbitrary code or consuming unreasonable amounts of time or
    memory.

    Each instance of this class has an associated list of prefixes. Lookups
    try each key in turn until a match is found. Writes use the first prefix in
    the list. Conventionally, keys are arranged into a hierarchy, separated by
    underscores. A :meth:`view` convenience method helps with constructing
    prefix lists by automatically adding the trailing underscore to prefixes.

    Care should be used when attributes share a suffix. They may shadow
    each other for some views, causing the attribute to appear to have
    changed value. This class does not prevent it, because there is no way to
    know which namespaces may be shared in a view, and because doing it in a
    race-free way would be prohibitively expensive.

    Parameters
    ----------
    backend : :class:`~katsdptelstate.aio.Backend`
        Backend supplying the storage. If not specified (and `base` is also
        not specified), an in-memory backend is created.
    prefixes : tuple of str/bytes
        Prefixes that will be tried in turn for key lookup. While this can be
        specified directly for advanced cases, it is normally generated by
        :meth:`view`. Writes are made using the first prefix in the list.
    base : :class:`~katsdptelstate.aio.telescope_state.TelescopeState`
        Existing telescope state instance, from which the backend will be
        taken. This allows new views to be created by specifying `prefixes`,
        without creating new backends.

    Raises
    ------
    ValueError
        If both `base` and `backend` are specified
    """

    def __init__(self, backend: Optional[Backend] = None,
                 prefixes: Tuple[_Key, ...] = (b'',),
                 base: Optional['TelescopeState'] = None) -> None:
        if base is None and backend is None:
            backend = MemoryBackend()
        super().__init__(backend, prefixes, base)

    async def __getitem__(self, key: _Key) -> Any:
        return await self._get(key)

    async def set(self, key: _Key, value: Any) -> None:
        await self.add(key, value, immutable=True)

    async def exists(self, key: _Key) -> bool:
        """Check to see if the specified key exists in the database."""
        key = ensure_binary(key)
        for prefix in self._prefixes:
            if await self._backend.exists(prefix + key):
                return True
        return False

    async def key_type(self, key: _Key) -> Optional[KeyType]:
        """Get the type of a key.

        If the key does not exist, returns ``None``.
        """
        key = ensure_binary(key)
        for prefix in self._prefixes:
            key_type = await self._backend.key_type(prefix + key)
            if key_type is not None:
                return key_type
        return None

    async def keys(self, filter: _Key = '*') -> List[str]:
        """Return a list of keys currently in the model.

        This function ignores the prefix list and returns all keys with
        fully-qualified names.

        Parameters
        ----------
        filter : str or bytes, optional
            Wildcard string passed to Redis to restrict keys

        Returns
        -------
        keys : list of str
            The key names, in sorted order
        """
        keys = await self._backend.keys(ensure_binary(filter))
        return sorted(ensure_str(key) for key in keys)

    async def delete(self, key: _Key) -> None:
        """Remove a key, and all values, from the model.

        The key is deleted from every namespace in the prefix list.

        .. note::

            This function should be used rarely, ideally only in tests, as it
            violates the immutability of keys added with ``immutable=True``.
        """
        key = ensure_binary(key)
        for prefix in self._prefixes:
            await self._backend.delete(prefix + key)

    async def clear(self) -> None:
        """Remove all keys in all namespaces.

        .. note::

            This function should be used rarely, ideally only in tests, as it
            violates the immutability of keys added with ``immutable=True``.
        """
        await self._backend.clear()

    async def add(self, key: _Key, value: Any, ts: Optional[float] = None,
                  immutable: bool = False, encoding: bytes = ENCODING_DEFAULT) -> None:
        """Add a new key / value pair to the model.

        If `immutable` is true, then either the key must not previously have
        been set, or it must have been previously set immutable with exactly the
        same value (see :meth:`equal_encoded_values`). Thus, immutable keys only
        ever have one value for the lifetime of the telescope state. They also
        have no associated timestamp.

        Parameters
        ----------
        key : str or bytes
            Key name, which must not collide with a class attribute
        value : object
            Arbitrary value (must be encodable with `encoding`)
        ts : float, optional
            Timestamp associated with the update, ignored for immutables. If not
            specified, defaults to ``time.time()``.
        immutable : bool, optional
            See description above.
        encoding : bytes
            See :func:`encode_value`

        Raises
        ------
        ImmutableKeyError
            if an attempt is made to change the value of an immutable
        ImmutableKeyError
            if the key already exists and is not an immutable
        redis.ResponseError
            if there is some other error from the Redis server
        """
        key = ensure_binary(key)
        full_key = self._prefixes[0] + key
        key_str = display_str(full_key)
        str_val = encode_value(value, encoding)
        if immutable:
            try:
                old = await self._backend.set_immutable(full_key, str_val)
            except ImmutableKeyError:
                raise ImmutableKeyError('Attempt to change key {} to immutable'
                                        .format(key_str))
            if old is not None:
                # The key already exists. Check if the value is the same.
                check_immutable_change(key_str, old, str_val, value)
        else:
            # TODO: this validation code is duplicated with sync version.
            # Abstract it.
            ts = float(ts) if ts is not None else time.time()
            if math.isnan(ts) or math.isinf(ts):
                raise InvalidTimestampError('Non-finite timestamps ({}) are not supported'
                                            .format(ts))
            if ts < 0.0:
                raise InvalidTimestampError('Negative timestamps ({}) are not supported'
                                            .format(ts))
            try:
                await self._backend.add_mutable(full_key, str_val, ts)
            except ImmutableKeyError:
                raise ImmutableKeyError('Attempt to change key {} to mutable'
                                        .format(key_str))

    async def set_indexed(self, key: _Key, sub_key: Any, value: Any,
                          encoding: bytes = ENCODING_DEFAULT) -> None:
        """Set a sub-key of an indexed key.

        Parameters
        ----------
        key : str or bytes
            Main key
        sub_key : object
            Sub-key within `key` to associate with the value. It must be both
            hashable and serialisable.
        encoding : bytes
            Encoding used for `value` (see :func:`encode_value`). Note that it
            does not affect the encoding of `sub_key`.

        Raises
        ------
        ImmutableKeyError
            if the sub-key already exists with a different value
        ImmutableKeyError
            if the key already exists and is not indexed
        redis.ResponseError
            if there is some other error from the Redis server
        """
        # Raises a TypeError if it's not hashable, to prevent trouble
        # retrieving it later.
        hash(sub_key)
        key = ensure_binary(key)
        full_key = self._prefixes[0] + key
        key_str = display_str(full_key)
        # Sub-keys will always be encoded with ENCODING_MSGPACK, so that
        # lookups don't need to worry whether they are using the matching
        # encoding.
        sub_key_enc = encode_value(sub_key, ENCODING_MSGPACK)
        str_val = encode_value(value, encoding)
        try:
            old = await self.backend.set_indexed(full_key, sub_key_enc, str_val)
        except ImmutableKeyError:
            raise ImmutableKeyError('Attempt to change key {} to indexed immutable'
                                    .format(key_str))
        if old is not None:
            # The key already exists. Check if the value is the same.
            key_descr = '{}[{!r}]'.format(key_str, sub_key)
            check_immutable_change(key_descr, old, str_val, value)

    async def get_indexed(self, key: _Key, sub_key: Any, default: Any = None,
                          return_encoded: bool = False) -> Any:
        """Retrieve an indexed value set with :meth:`set_indexed`.

        Parameters
        ----------
        key : str or bytes
            Main key
        sub_key : object
            Sub-key within `key`, which must be hashable and serialisable
        default : object
            Value to return if the sub-key is not found
        return_encoded : bool, optional
            Default 'False' - return values are first decoded from internal storage
            'True' - return values are retained in encoded form.
        """
        key = ensure_binary(key)
        sub_key_enc = encode_value(sub_key, ENCODING_MSGPACK)
        for prefix in self._prefixes:
            full_key = prefix + key
            try:
                raw_value = await self.backend.get_indexed(full_key, sub_key_enc)
                if raw_value is None:
                    return default
                elif return_encoded:
                    return raw_value
                else:
                    return decode_value(raw_value)
            except KeyError:
                pass  # Key does not exist, try the next prefix
        return default

    async def _check_condition(self, key: bytes,
                               condition: Optional[Callable[[Any, Optional[float]], bool]],
                               message: Optional[KeyUpdate] = None):
        """Check whether key exists and satisfies a condition (if any).

        Parameters
        ----------
        key : bytes
            Unqualified key name to check
        condition : callable, optional
            See :meth:`wait_key`'s docstring for the details
        message : :class:`.KeyUpdateBase`, optional
            A non-empty update returned by :meth:`.Backend.wait_key`.

            If specified, this is used to find the latest value and timestamp
            (if available) of the key instead of retrieving it from the backend.
        """
        if condition is None:
            return message is not None or await self.exists(key)

        for prefix in self._prefixes:
            full_key = prefix + key
            if (message is not None and full_key == message.key
                    and message.key_type != KeyType.INDEXED):
                value = message.value     # type: Union[bytes, Dict[bytes, bytes]]
                timestamp = getattr(message, 'timestamp', None)    # type: Optional[float]
            else:
                value2, timestamp = await self._backend.get(full_key)
                if value2 is None:
                    continue      # Key does not exist, so try the next one
                value = value2
            if isinstance(value, dict):
                # Handle indexed items
                value = {decode_value(k): decode_value(v) for k, v in value.items()}
            else:
                value = decode_value(value)
            return condition(value, timestamp)
        return False    # Key does not exist

    async def wait_key(self, key: _Key,
                       condition: Optional[Callable[[Any, Optional[float]], bool]] = None):
        """Wait for a key to exist, possibly with some condition.

        This can block for an arbitrary amount of time, but can be safely
        cancelled.

        Parameters
        ----------
        key : str or bytes
            Key name to monitor
        condition : callable, signature `bool = condition(value, ts)`, optional
            If not specified, wait until the key exists. Otherwise, the
            callable should have the signature `bool = condition(value, ts)`
            where `value` is the latest value of the key, `ts` is its
            associated timestamp (or None if immutable), and the return value
            indicates whether the condition is satisfied.
        """
        key = ensure_binary(key)
        # First check if condition is already satisfied, in which case we
        # don't need to create a pubsub connection.
        if await self._check_condition(key, condition):
            return
        monitor = self._backend.monitor_keys([prefix + key
                                              for prefix in self._prefixes])
        try:
            message: Optional[KeyUpdateBase]
            async for message in monitor:
                if not isinstance(message, KeyUpdate):
                    # The monitor thinks it's worth checking again, but doesn't
                    # have enough information to be useful.
                    message = None
                if await self._check_condition(key, condition, message):
                    return
        finally:
            await monitor.aclose()

    async def _check_indexed_condition(self, key: bytes, sub_key: bytes,
                                       condition: Optional[Callable[[Any], bool]],
                                       message: Optional[IndexedKeyUpdate] = None):
        """Check whether key exists and satisfies a condition (if any).

        Parameters
        ----------
        key : bytes
            Unqualified key name to check
        sub_key : bytes
            Encoded sub-key to check
        condition : callable, optional
            See :meth:`wait_indexed`'s docstring for the details
        message : :class:`.KeyUpdateBase`, optional
            A non-empty update returned by :meth:`.Backend.wait_key`. If
            specified, it must match the given `sub_key`.

            If specified, this is used to find the latest value instead of
            retrieving it from the backend.
        """
        if message is not None and message.sub_key != sub_key:
            return False           # This update is not applicable
        for prefix in self._prefixes:
            full_key = prefix + key
            if message is not None and full_key == message.key:
                value = message.value
            else:
                try:
                    # TODO: this could be more efficient if the backend
                    # provided a has_indexed that didn't retrieve the
                    # value.
                    value2 = await self._backend.get_indexed(full_key, sub_key)
                except KeyError:
                    continue       # Key does not exist, try next prefix
                if value2 is None:
                    return False   # Key exists, but sub-key does not exist
                value = value2
            if not condition:
                return True
            else:
                return condition(decode_value(value))
        return False    # Key does not exist (for any prefix)

    async def wait_indexed(self, key: _Key, sub_key: Any,
                           condition: Optional[Callable[[Any], bool]] = None) -> None:
        """Wait for a sub-key of an indexed key to exist, possibly with some condition.

        This can block for an arbitrary amount of time, but can be safely
        cancelled.

        Parameters
        ----------
        key : str or bytes
            Key name to monitor
        sub_key : object
            Sub-key to monitor within `key`.
        condition : callable, signature `bool = condition(value)`, optional
            If not specified, wait until the sub-key exists. Otherwise, the
            callable should have the signature `bool = condition(value)`
            where `value` is the value associated with the sub-key, and the
            return value indicates whether the condition is satisfied.

        Raises
        ------
        TimeoutError
            if a timeout was specified and was exceeded
        CancelledError
            if a cancellation future was specified and done
        ImmutableKeyError
            if the key exists (or is created while waiting) but is not indexed
        """
        key = ensure_binary(key)
        sub_key_enc = encode_value(sub_key, ENCODING_MSGPACK)
        # First check if condition is already satisfied, in which case we
        # don't need to create a pubsub connection.
        if await self._check_indexed_condition(key, sub_key_enc, condition):
            return
        key_str = '{}[{!r}]'.format(display_str(key), sub_key)

        monitor = self._backend.monitor_keys([prefix + key
                                              for prefix in self._prefixes])
        try:
            message: Optional[KeyUpdateBase]
            async for message in monitor:
                if not isinstance(message, KeyUpdate):
                    # The monitor thinks it's worth checking again, but doesn't
                    # have enough information to be useful.
                    message = None
                elif not isinstance(message, IndexedKeyUpdate):
                    raise ImmutableKeyError('wait_indexed called on non-indexed key {}'
                                            .format(key_str))
                if await self._check_indexed_condition(key, sub_key_enc, condition, message):
                    return
        finally:
            await monitor.aclose()

    async def _get(self, key: _Key, return_encoded: bool = False) -> Any:
        key = ensure_binary(key)
        for prefix in self._prefixes:
            full_key = prefix + key
            raw_value = (await self._backend.get(full_key))[0]
            if raw_value is not None:
                if return_encoded:
                    return raw_value
                elif isinstance(raw_value, dict):
                    # Indexed immutable
                    return {
                        decode_value(key): decode_value(value)
                        for (key, value) in raw_value.items()
                    }
                else:
                    return decode_value(raw_value)
        raise KeyError('{} not found'.format(display_str(key)))

    async def get(self, key: _Key, default: Any = None, return_encoded: bool = False) -> Any:
        """Get a single value from the model.

        Parameters
        ----------
        key : str or bytes
            Key to retrieve
        default : object, optional
            Object to return if key not found
        return_encoded : bool, optional
            Default 'False' - return values are first decoded from internal storage
            'True' - return values are retained in encoded form.

        Returns
        -------
        value
            for non-immutable key return the most recent value
        """
        try:
            return await self._get(key, return_encoded)
        except KeyError:
            return default

    async def get_range(self, key: _Key,
                        st: Optional[float] = None, et: Optional[float] = None,
                        include_previous: Optional[bool] = None,
                        include_end: bool = False,
                        return_encoded: bool = False) -> List[Tuple[Any, float]]:
        """Get the range of values specified by the key and timespec from the model.

        Parameters
        ----------
        key : str or bytes
            Database key to extract
        st : float, optional
            Start time, default returns the most recent value prior to et
        et: float, optional
            End time, defaults to the end of time
        include_previous : bool, optional
            If True, the method also returns the last value
            prior to the start time (if any). This defaults to False if st is
            specified and True if st is unspecified.
        include_end : bool, optional
            If False (default), returns values in [st, et), otherwise [st, et].
        return_encoded : bool, optional
            Default 'False' - return values are first decoded from internal storage
            'True' - return values are retained in encoded form.

        Returns
        -------
        list
            list of (value, time) records in specified time range

        Raises
        ------
        KeyError
            if `key` does not exist (with any prefix)
        ImmutableKeyError
            if `key` refers to an existing key which is not mutable

        Notes
        -----
        By default, timestamps exactly equal to the start time are included,
        while those equal to the end time are excluded.

        Usage examples:

        get_range('key')
            returns most recent record

        get_range('key',st=0)
            returns list of all records in the telescope state database

        get_range('key',st=0,et=t1)
            returns list of all records before time t1

        get_range('key',st=t0,et=t1)
            returns list of all records in the range [t0,t1)

        get_range('key',st=t0)
            returns list of all records after time t0

        get_range('key',et=t1)
            returns the most recent record prior to time t1
        """
        # TODO: this setup logic is duplicated by sync class. Abstract it.
        # set up include_previous and st default values
        if include_previous is None:
            include_previous = True if st is None else False
        if et is None:
            et = math.inf
        else:
            et = float(et)
        if st is None:
            st = et
        else:
            st = float(st)
        if math.isnan(st) or math.isnan(et):
            raise InvalidTimestampError('cannot use NaN start or end time')

        key = ensure_binary(key)
        for prefix in self._prefixes:
            full_key = prefix + key
            try:
                ret_vals = await self._backend.get_range(full_key, st, et,
                                                         include_previous, include_end)
            except ImmutableKeyError:
                raise ImmutableKeyError('{} is immutable, cannot use get_range'
                                        .format(display_str(full_key)))
            if ret_vals is not None:
                if not return_encoded:
                    ret_vals = [(decode_value(value), timestamp)
                                for value, timestamp in ret_vals]
                return ret_vals
        raise KeyError('{} not found'.format(display_str(key)))
