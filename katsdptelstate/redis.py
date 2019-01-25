import contextlib
import struct

import redis

from .telescope_state import ConnectionError, ImmutableKeyError
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


class RedisBackend(object):
    """Backend for :class:`TelescopeState` using redis for storage."""
    def __init__(self, client):
        self._r = client
        self._ps = self._r.pubsub(ignore_subscribe_messages=True)
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
        return rdb_reader.load_from_file(self._r, filename)

    def __contains__(self, key):
        return self._r.exists(key)

    def is_immutable(self, key):
        type_ = self._r.type(key)
        if type_ == b'none':
            raise KeyError
        else:
            return type_ == b'string'

    def keys(self, filter):
        return self._r.keys(filter)

    def delete(self, key):
        self._r.delete(key)

    def clear(self):
        self._r.flushdb()

    def set_immutable(self, key, value):
        while True:
            result = self._r.setnx(key, value)
            if result:
                self._r.publish(b'update/' + key, value)
                return None
            with _handle_wrongtype():
                old = self._r.get(key)
                # If someone deleted the key between SETNX and GET, we will
                # get None here, in which case we can just try again. This
                # race condition could be eliminated with a Lua script.
                if old is not None:
                    return old

    def get_immutable(self, key):
        with _handle_wrongtype():
            return self._r.get(key)

    def add_mutable(self, key, value, timestamp):
        assert timestamp >= 0
        # abs forces -0 to +0, which encodes differently
        packed_ts = struct.pack('>d', abs(timestamp))
        str_val = packed_ts + value
        with _handle_wrongtype():
            ret = zadd(self._r, key, {str_val: 0})
        self._r.publish(b'update/' + key, str_val)

    @classmethod
    def _pack_query_time(cls, time, is_end, include_end=False):
        """Create a query value for a ZRANGEBYLEX query.

        If include_end is true, the time is incremented by the smallest possible
        amount, so that when used as an end it will be inclusive rather than
        exclusive.
        """
        if time == _INF:
            # The special positively infinite string represents the end of time
            return b'+'
        elif time < 0.0 or (time == 0.0 and not include_end):
            # The special negatively infinite string represents the dawn of time
            return b'-'
        else:
            # abs ensures that -0.0 becomes +0.0, which encodes differently
            packed_time = struct.pack('>d', abs(time))
            if include_end:
                # Increment to the next possible encoded value. Note that this
                # cannot overflow because the sign bit is initially clear.
                packed_time = struct.pack('>Q', struct.unpack('>Q', packed_time)[0] + 1)
            return (b'(' if is_end else b'[') + packed_time

    def _split_timestamp(self, packed):
        assert len(packed) >= 8
        timestamp = struct.unpack('>d', packed[:8])[0]
        return (packed[8:], timestamp)

    def get_range(self, key, start_time, end_time, include_previous, include_end):
        # TODO: use a transaction to avoid race conditions and multiple
        # round trips.
        with _handle_wrongtype():
            packed_st = self._pack_query_time(start_time, False)
            packed_et = self._pack_query_time(end_time, True, include_end)
            ret_vals = []
            if include_previous and packed_st != b'-':
                ret_vals += self._r.zrevrangebylex(key, packed_st, b'-', 0, 1)
            # Avoid talking to Redis if it is going to be futile
            if packed_st != packed_et:
                ret_vals += self._r.zrangebylex(key, packed_st, packed_et)
            ans = [self._split_timestamp(val) for val in ret_vals]
        # We can't immediately distinguish between there being nothing in
        # range versus the key not existing.
        if not ans and not self._r.exists(key):
            return None
        return ans

    def send_message(self, data):
        self._r.publish(_MESSAGE_CHANNEL, data)

    def get_message(self):
        msg = self._ps.get_message(_MESSAGE_CHANNEL)
        if msg is not None:
            msg = msg['data']
        return msg

    def monitor_keys(self, keys):
        p = self._r.pubsub()
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
                        timeout = yield (key,)
                    elif message['type'] == 'message':
                        # TODO: eventually change the protocol to indicate
                        # mutability in the message.
                        type_ = self._r.type(key)
                        if type_ == b'string':
                            timeout = yield (key, message['data'])
                        else:
                            value, timestamp = self._split_timestamp(message['data'])
                            timeout = yield (key, value, timestamp)
