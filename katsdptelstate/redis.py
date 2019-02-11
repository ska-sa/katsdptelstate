from __future__ import print_function, division, absolute_import

import contextlib
import struct

import redis

from .telescope_state import ConnectionError, ImmutableKeyError, Backend
from .compat import zadd
try:
    from . import rdb_reader
except ImportError as _rdb_reader_import_error:
    rdb_reader = None


_INF = float('inf')
_MESSAGE_CHANNEL = b'tm_info'


@contextlib.contextmanager
def _handle_wrongtype():
    """Map redis WRONGTYPE error to ImmutableKeyError"""
    try:
        yield
    except redis.ResponseError as error:
        if not error.args[0].startswith('WRONGTYPE '):
            raise
        raise ImmutableKeyError


class RedisBackend(Backend):
    """Backend for :class:`TelescopeState` using redis for storage."""
    def __init__(self, client):
        self.client = client
        self._ps = self.client.pubsub(ignore_subscribe_messages=True)
        try:
            # This is the first command to the server and therefore
            # the first test of its availability
            self._ps.subscribe(_MESSAGE_CHANNEL)
        except (redis.TimeoutError, redis.ConnectionError) as e:
            # redis.TimeoutError: bad host
            # redis.ConnectionError: good host, bad port
            raise ConnectionError("could not connect to redis server: {}".format(e))

    def load_from_file(self, filename):
        if rdb_reader is None:
            raise _rdb_reader_import_error
        return rdb_reader.load_from_file(self.client, filename)

    def __contains__(self, key):
        return self.client.exists(key)

    def keys(self, filter):
        return self.client.keys(filter)

    def delete(self, key):
        self.client.delete(key)

    def clear(self):
        self.client.flushdb()

    def is_immutable(self, key):
        type_ = self.client.type(key)
        if type_ == b'none':
            raise KeyError
        else:
            return type_ == b'string'

    def set_immutable(self, key, value):
        while True:
            result = self.client.setnx(key, value)
            if result:
                self.client.publish(b'update/' + key, value)
                return None
            with _handle_wrongtype():
                old = self.client.get(key)
                # If someone deleted the key between SETNX and GET, we will
                # get None here, in which case we can just try again. This
                # race condition could be eliminated with a Lua script.
                if old is not None:
                    return old

    def get_immutable(self, key):
        with _handle_wrongtype():
            return self.client.get(key)

    def add_mutable(self, key, value, timestamp):
        str_val = self.pack_timestamp(timestamp) + value
        with _handle_wrongtype():
            ret = zadd(self.client, key, {str_val: 0})
        self.client.publish(b'update/' + key, str_val)

    def get_range(self, key, start_time, end_time, include_previous, include_end):
        # TODO: use a transaction to avoid race conditions and multiple
        # round trips.
        with _handle_wrongtype():
            packed_st = self.pack_query_timestamp(start_time, False)
            packed_et = self.pack_query_timestamp(end_time, True, include_end)
            ret_vals = []
            if include_previous and packed_st != b'-':
                ret_vals += self.client.zrevrangebylex(key, packed_st, b'-', 0, 1)
            # Avoid talking to Redis if it is going to be futile
            if packed_st != packed_et:
                ret_vals += self.client.zrangebylex(key, packed_st, packed_et)
            ans = [self.split_timestamp(val) for val in ret_vals]
        # We can't immediately distinguish between there being nothing in
        # range versus the key not existing.
        if not ans and not self.client.exists(key):
            return None
        return ans

    def send_message(self, data):
        self.client.publish(_MESSAGE_CHANNEL, data)

    def get_message(self):
        msg = self._ps.get_message(_MESSAGE_CHANNEL)
        if msg is not None:
            msg = msg['data']
        return msg

    def monitor_keys(self, keys):
        p = self.client.pubsub()
        with contextlib.closing(p):
            for key in keys:
                p.subscribe(b'update/' + key)
            timeout = yield None
            while True:
                message = p.get_message(timeout=timeout)
                if message is None:
                    timeout = yield None
                else:
                    assert message['channel'].startswith(b'update/')
                    key = message['channel'][7:]
                    if message['type'] == 'subscribe':
                        # An update may have occurred before our subscription
                        # happened. Also, if we lost connection and
                        # reconnected, we will get the subscribe message
                        # again.
                        timeout = yield (key,)
                    elif message['type'] == 'message':
                        # TODO: eventually change the protocol to indicate
                        # mutability in the message.
                        type_ = self.client.type(key)
                        if type_ == b'string':
                            timeout = yield (key, message['data'])
                        else:
                            value, timestamp = self.split_timestamp(message['data'])
                            timeout = yield (key, value, timestamp)
