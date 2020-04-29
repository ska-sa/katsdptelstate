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

import time
import math
import logging
import contextlib
import warnings

import redis

from .endpoint import Endpoint, endpoint_parser
from .errors import (InvalidKeyError, ImmutableKeyError, TimeoutError, CancelledError,
                     DecodeError, InvalidTimestampError)
from .encoding import (ENCODING_DEFAULT, ENCODING_MSGPACK, encode_value, decode_value,
                       equal_encoded_values)
from .utils import ensure_str, ensure_binary, display_str, KeyType
from .backend import Backend


logger = logging.getLogger(__name__)


class TelescopeState:
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
    endpoint : str, :class:`~katsdptelstate.endpoint.Endpoint` or :class:`Backend`
        It can be

        - an endpoint: specifies the address of the Redis server

        - an URL (i.e., contains ``://``): passed to :meth:`redis.StrictRedis.from_url`

        - an empty string: a :class:`~katsdptelstate.memory.MemoryBackend` is created

        - any other string: passed to :class:`~katsdptelstate.endpoint.Endpoint` to
          create an endpoint

        - a :class:`Backend`: used directly.
    db : int
        Database number within the Redis server.
    prefixes : tuple of str/bytes
        Prefixes that will be tried in turn for key lookup. While this can be
        specified directly for advanced cases, it is normally generated by
        :meth:`view`. Writes are made using the first prefix in the list.
    base : :class:`~katsdptelstate.telescope_state.TelescopeState`
        Existing telescope state instance, from which the backend will be
        taken. This allows new views to be created by specifying `prefixes`,
        without creating new backends.

    Raises
    ------
    ConnectionError
        If the initial connection to the (real) Redis server fails
    ValueError
        If a `base` is specified and either `endpoint` or `db` is non-default
    ValueError
        If `endpoint` is a :class:`Backend` and `db` is non-default
    """
    SEPARATOR = '_'
    SEPARATOR_BYTES = b'_'

    def __init__(self, endpoint='', db=0, prefixes=(b'',), base=None):
        if base is not None:
            if endpoint != '':
                raise ValueError('Cannot specify both base and endpoint')
            if db != 0:
                raise ValueError('Cannot specify both base and db')
            self._backend = base._backend
        elif isinstance(endpoint, Backend):
            if db != 0:
                raise ValueError('Cannot specify both a backend and a db')
            self._backend = endpoint
        elif not endpoint:
            from .memory import MemoryBackend
            if db != 0:
                raise ValueError('Cannot specify a db when using the default backend')
            self._backend = MemoryBackend()
        else:
            from .redis import RedisBackend
            if isinstance(endpoint, str) and '://' in endpoint:
                r = redis.StrictRedis.from_url(endpoint, db=db, socket_timeout=5)
            else:
                if not isinstance(endpoint, Endpoint):
                    endpoint = endpoint_parser(default_port=None)(endpoint)
                redis_kwargs = dict(host=endpoint.host, db=db, socket_timeout=5)
                # If no port is provided, redis will pick its default port
                if endpoint.port is not None:
                    redis_kwargs['port'] = endpoint.port
                r = redis.StrictRedis(**redis_kwargs)
            self._backend = RedisBackend(r)
        # Ensure all prefixes are bytes internally for consistency
        self._prefixes = tuple(ensure_binary(prefix) for prefix in prefixes)

    @property
    def prefixes(self):
        """The active key prefixes as a tuple of strings."""
        return tuple(ensure_str(prefix) for prefix in self._prefixes)

    @property
    def backend(self):
        return self._backend

    def load_from_file(self, file):
        """Load keys from a Redis-compatible RDB snapshot file.

        Redis keys are extracted sequentially from the RDB file and inserted
        directly into the backend without any checks and ignoring the view.
        It is therefore a bad idea to insert keys that already exist in telstate
        and this will lead to undefined behaviour. The standard approach is
        to call this method on an empty telstate.

        If there is an error reading or parsing the RDB file (indicating either
        a broken file or a non-RDB file), an `RdbParseError` is raised. Errors
        raised while opening the file (like `OSError`) and errors raised by the
        backend itself (like redis errors) can also occur.

        Parameters
        ----------
        file : str or file object
            Filename or file object representing RDB file

        Returns
        -------
        keys_loaded : int
            Number of keys loaded from RDB file into telstate

        Raises
        ------
        ImportError
            If the rdbtools package is not installed
        RdbParseError
            If the file could not be parsed (truncated / malformed / not RDB)
        """
        keys_loaded = self._backend.load_from_file(file)
        logger.info("Loading {} keys from {}".format(keys_loaded, file))
        return keys_loaded

    @classmethod
    def join(cls, *names):
        """Join string components of key with supported separator."""
        return cls.SEPARATOR.join(names)

    def view(self, name, add_separator=True, exclusive=False):
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
            prefixes = (name,)
        else:
            prefixes = (name,) + self._prefixes
        return self.__class__(prefixes=prefixes, base=self)

    def root(self):
        """Create a view containing only the root namespace."""
        return self.__class__(base=self)

    def __getattr__(self, key):
        try:
            return self._get(key)
        except KeyError as error:
            raise AttributeError(str(error))

    def __getitem__(self, key):
        return self._get(key)

    def __setattr__(self, key, value):
        if key.startswith('_'):
            super().__setattr__(key, value)
        elif key in self.__class__.__dict__:
            raise AttributeError("The specified key already exists as a "
                                 "class method and thus cannot be used.")
        else:
            self.add(key, value, immutable=True)

    def __setitem__(self, key, value):
        self.add(key, value, immutable=True)

    def __contains__(self, key):
        """Check to see if the specified key exists in the database."""
        key = ensure_binary(key)
        for prefix in self._prefixes:
            if prefix + key in self._backend:
                return True
        return False

    def is_immutable(self, key):
        """Check to see if the specified key is an immutable.

        Note that indexed keys are not considered immutable for this purpose.
        If the key does not exist, ``False`` is returned.

        .. deprecated:: 0.10
            :meth:`is_immutable` is deprecated and may be removed in a future release.
            Use :meth:`key_type` instead.
        """
        warnings.warn('is_immutable is deprecated; use key_type instead', FutureWarning)
        return self.key_type(key) == KeyType.IMMUTABLE

    def key_type(self, key):
        """Get the type of a key.

        If the key does not exist, returns ``None``.
        """
        key = ensure_binary(key)
        for prefix in self._prefixes:
            key_type = self._backend.key_type(prefix + key)
            if key_type is not None:
                return key_type
        return None

    def keys(self, filter='*'):
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
        keys = self._backend.keys(ensure_binary(filter))
        return sorted(ensure_str(key) for key in keys)

    def _ipython_key_completions_(self):
        """List of keys used in IPython (version >= 5) tab completion.

        This respects the prefix list and presents keys with prefixes removed.
        """
        keys = []
        keys_b = self._backend.keys(b'*')
        for prefix_b in self._prefixes:
            keys.extend(ensure_str(k[len(prefix_b):])
                        for k in keys_b if k.startswith(prefix_b))
        return keys

    def delete(self, key):
        """Remove a key, and all values, from the model.

        The key is deleted from every namespace in the prefix list.

        .. note::

            This function should be used rarely, ideally only in tests, as it
            violates the immutability of keys added with ``immutable=True``.
        """
        key = ensure_binary(key)
        for prefix in self._prefixes:
            self._backend.delete(prefix + key)

    def clear(self):
        """Remove all keys in all namespaces.

        .. note::

            This function should be used rarely, ideally only in tests, as it
            violates the immutability of keys added with ``immutable=True``.
        """
        return self._backend.clear()

    def _check_immutable_change(self, key, old_enc, new_enc, new):
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
                return True
        except DecodeError as error:
            raise ImmutableKeyError(
                'Attempt to set value of immutable key {} to {!r} but '
                'failed to decode the previous value to compare: {}'
                .format(key, new, error))

    def add(self, key, value, ts=None, immutable=False, encoding=ENCODING_DEFAULT):
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
        InvalidKeyError
            if `key` collides with a class member name
        ImmutableKeyError
            if an attempt is made to change the value of an immutable
        ImmutableKeyError
            if the key already exists and is not an immutable
        redis.ResponseError
            if there is some other error from the Redis server
        """
        # check that we are not going to munge a class method
        if key in self.__class__.__dict__:
            raise InvalidKeyError("The specified key already exists as a "
                                  "class method and thus cannot be used.")
        key = ensure_binary(key)
        full_key = self._prefixes[0] + key
        key_str = display_str(full_key)
        str_val = encode_value(value, encoding)
        if immutable:
            try:
                old = self._backend.set_immutable(full_key, str_val)
            except ImmutableKeyError:
                raise ImmutableKeyError('Attempt to change key {} to immutable'
                                        .format(key_str))
            if old is not None:
                # The key already exists. Check if the value is the same.
                self._check_immutable_change(key_str, old, str_val, value)
        else:
            ts = float(ts) if ts is not None else time.time()
            if math.isnan(ts) or math.isinf(ts):
                raise InvalidTimestampError('Non-finite timestamps ({}) are not supported'
                                            .format(ts))
            if ts < 0.0:
                raise InvalidTimestampError('Negative timestamps ({}) are not supported'
                                            .format(ts))
            try:
                self._backend.add_mutable(full_key, str_val, ts)
            except ImmutableKeyError:
                raise ImmutableKeyError('Attempt to change key {} to mutable'
                                        .format(key_str))

    def set_indexed(self, key, sub_key, value, encoding=ENCODING_DEFAULT):
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
        InvalidKeyError
            if `key` collides with a class member name
        ImmutableKeyError
            if the sub-key already exists with a different value
        ImmutableKeyError
            if the key already exists and is not indexed
        redis.ResponseError
            if there is some other error from the Redis server
        """
        # check that we are not going to munge a class method
        if key in self.__class__.__dict__:
            raise InvalidKeyError("The specified key already exists as a "
                                  "class method and thus cannot be used.")
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
            old = self.backend.set_indexed(full_key, sub_key_enc, str_val)
        except ImmutableKeyError:
            raise ImmutableKeyError('Attempt to change key {} to indexed immutable'
                                    .format(key_str))
        if old is not None:
            # The key already exists. Check if the value is the same.
            key_descr = '{}[{!r}]'.format(key_str, sub_key)
            self._check_immutable_change(key_descr, old, str_val, value)

    def get_indexed(self, key, sub_key, default=None, return_encoded=False):
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
                raw_value = self.backend.get_indexed(full_key, sub_key_enc)
                if raw_value is None:
                    return default
                elif return_encoded:
                    return raw_value
                else:
                    return decode_value(raw_value)
            except KeyError:
                pass  # Key does not exist, try the next prefix
        return default

    def _check_condition(self, key, condition, message=None):
        """Check whether key exists and satisfies a condition (if any).

        Parameters
        ----------
        key : str or bytes
            Unqualified key name to check
        condition : callable, optional
            See :meth:`wait_key`'s docstring for the details
        message : tuple, optional
            A tuple of the form (key, value) or (key, value, timestamp).
            The first indicates an update to an immutable, and the second an
            update to a mutable.

            If specified, this is used to find the latest value and timestamp
            (if available) of the key instead of retrieving it from the backend.
        """
        if condition is None:
            return message is not None or key in self

        for prefix in self._prefixes:
            full_key = prefix + ensure_binary(key)
            if message is not None and full_key == message[1] and message[0] != KeyType.MUTABLE:
                value = message[2]
                timestamp = None if message[0] != KeyType.MUTABLE else message[3]
            else:
                value, timestamp = self._backend.get(full_key)
                if value is None:
                    continue      # Key does not exist, so try the next one
            if isinstance(value, dict):
                # Handle indexed items
                value = {decode_value(k): decode_value(v) for k, v in value.items()}
            else:
                value = decode_value(value)
            return condition(value, timestamp)
        return False    # Key does not exist

    def wait_key(self, key, condition=None, timeout=None, cancel_future=None):
        """Wait for a key to exist, possibly with some condition.

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
        timeout : float, optional
            If specified and the condition is not met within the time limit,
            an exception is thrown.
        cancel_future : future, optional
            If not ``None``, a future object (e.g.
            :class:`concurrent.futures.Future` or :class:`trollius.Future`). If
            ``cancel_future.done()`` is true before the timeout, raises
            :exc:`CancelledError`. In the current implementation, it is only
            polled once a second, rather than waited for.

        Raises
        ------
        TimeoutError
            if a timeout was specified and was exceeded
        CancelledError
            if a cancellation future was specified and done
        """
        key = ensure_binary(key)
        # First check if condition is already satisfied, in which case we
        # don't need to create a pubsub connection.
        if self._check_condition(key, condition):
            return
        key_str = display_str(key)

        def check_cancelled():
            if cancel_future is not None and cancel_future.done():
                raise CancelledError('Wait for {} cancelled'.format(key_str))

        check_cancelled()
        monitor = self._backend.monitor_keys([prefix + key
                                              for prefix in self._prefixes])
        with contextlib.closing(monitor):
            monitor.send(None)   # Just to start the generator going
            start = time.time()
            while True:
                # redis-py automatically reconnects to the server if the connection
                # goes down, but we might miss messages in that case. So rather
                # than waiting an arbitrarily long time, we make sure to poll from
                # time to time. This also allows the cancellation future to be
                # polled.
                check_cancelled()
                get_timeout = 1.0
                if timeout is not None:
                    remain = (start + timeout) - time.time()
                    if remain <= 0:
                        raise TimeoutError('Timed out waiting for {} after {}s'
                                           .format(key_str, timeout))
                    get_timeout = min(get_timeout, remain)
                message = monitor.send(get_timeout)
                print(message)
                if message is None:
                    continue
                if message == ():
                    # The monitor thinks it's worth checking again, but doesn't
                    # have enough information to be useful.
                    message = None
                if self._check_condition(key, condition, message):
                    return

    def _get(self, key, return_encoded=False):
        key = ensure_binary(key)
        for prefix in self._prefixes:
            full_key = prefix + key
            raw_value = self._backend.get(full_key)[0]
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

    def get(self, key, default=None, return_encoded=False):
        """Get a single value from the model.

        Parameters
        ----------
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
            return self._get(key, return_encoded)
        except KeyError:
            return default

    def get_range(self, key, st=None, et=None,
                  include_previous=None, include_end=False, return_encoded=False):
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
                ret_vals = self._backend.get_range(full_key, st, et,
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
