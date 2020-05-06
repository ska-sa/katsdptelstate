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

"""Handles details of encoding and decoding strings stored in telescope state."""

import struct
import io
import os
import functools
import threading
import warnings
import pickle
from typing import Optional, Any

import msgpack
import numpy as np

from .errors import EncodeError, DecodeError
from .utils import ensure_binary


PICKLE_PROTOCOL = 2

# Header byte indicating a pickle-encoded value. This is present in pickle
# v2+ (it is the PROTO opcode, which is required to be first). Values below
# 0x80 are assumed to be pickle as well, since v0 uses only ASCII, and v1
# was never used in telstate. Values above 0x80 are reserved for future
# encoding forms.
ENCODING_PICKLE = b'\x80'

#: Header byte indicating a msgpack-encoded value
ENCODING_MSGPACK = b'\xff'

#: Default encoding for :func:`encode_value`
ENCODING_DEFAULT = ENCODING_MSGPACK

#: All encodings that can be used with :func:`encode_value`
ALLOWED_ENCODINGS = frozenset([ENCODING_PICKLE, ENCODING_MSGPACK])

MSGPACK_EXT_TUPLE = 1
MSGPACK_EXT_COMPLEX128 = 2
MSGPACK_EXT_NDARRAY = 3           # .npy format
MSGPACK_EXT_NUMPY_SCALAR = 4      # dtype descriptor then raw value

_allow_pickle = False
_warn_on_pickle = False
_pickle_lock = threading.Lock()       # Protects _allow_pickle, _warn_on_pickle


PICKLE_WARNING = ('The telescope state contains pickled values. This is a security risk, '
                  'but you have enabled it with set_allow_pickle.')
PICKLE_ERROR = ('The telescope state contains pickled values. This is a security risk, '
                'so is disabled by default. If you trust the source of the data, you '
                'can allow the pickles to be loaded by setting '
                'KATSDPTELSTATE_ALLOW_PICKLE=1 in the environment. This is needed for '
                'MeerKAT data up to March 2019.')


# See https://stackoverflow.com/questions/11305790
_pickle_loads = functools.partial(pickle.loads, encoding='latin1')


def set_allow_pickle(allow: bool, warn: bool = False) -> None:
    """Control whether pickles are allowed.

    This overrides the defaults which are determined from the environment.

    Parameters
    ----------
    allow : bool
        If true, allow pickles to be loaded.
    warn : bool
        If true, warn the user the next time a pickle is loaded (after which
        it becomes false). This has no effect if `allow` is false.
    """
    global _allow_pickle, _warn_on_pickle

    with _pickle_lock:
        _allow_pickle = allow
        _warn_on_pickle = warn


def _init_allow_pickle() -> None:
    env = os.environ.get('KATSDPTELSTATE_ALLOW_PICKLE')
    allow = False
    if env == '1':
        allow = True
    elif env == '0':
        allow = False
    elif env is not None:
        warnings.warn('Unknown value {!r} for KATSDPTELSTATE_ALLOW_PICKLE'.format(env))
    set_allow_pickle(allow)


_init_allow_pickle()


def _encode_ndarray(value: np.ndarray) -> bytes:
    fp = io.BytesIO()
    try:
        np.save(fp, value, allow_pickle=False)
    except ValueError as error:
        # Occurs if value has object type
        raise EncodeError(str(error))
    return fp.getvalue()


def _decode_ndarray(data: bytes) -> np.ndarray:
    fp = io.BytesIO(data)
    try:
        return np.load(fp, allow_pickle=False)
    except ValueError as error:
        raise DecodeError(str(error))


def _encode_numpy_scalar(value: np.generic) -> bytes:
    if value.dtype.hasobject:
        raise EncodeError('cannot encode dtype {} as it contains objects'
                          .format(value.dtype))
    descr = np.lib.format.dtype_to_descr(value.dtype)
    return _msgpack_encode(descr) + value.tobytes()


def _decode_numpy_scalar(data: bytes) -> np.generic:
    try:
        descr = _msgpack_decode(data)
        raw = b''
    except msgpack.ExtraData as exc:
        descr = exc.unpacked
        raw = exc.extra
    dtype = np.dtype(descr)
    if dtype.hasobject:
        raise DecodeError('cannot decode dtype {} as it contains objects'
                          .format(dtype))
    value = np.frombuffer(raw, dtype, 1)
    return value[0]


def _msgpack_default(value: Any) -> msgpack.ExtType:
    if isinstance(value, tuple):
        return msgpack.ExtType(MSGPACK_EXT_TUPLE, _msgpack_encode(list(value)))
    elif isinstance(value, np.ndarray):
        return msgpack.ExtType(MSGPACK_EXT_NDARRAY, _encode_ndarray(value))
    elif isinstance(value, np.generic):
        return msgpack.ExtType(MSGPACK_EXT_NUMPY_SCALAR, _encode_numpy_scalar(value))
    elif isinstance(value, complex):
        return msgpack.ExtType(MSGPACK_EXT_COMPLEX128,
                               struct.pack('>dd', value.real, value.imag))
    else:
        raise EncodeError('do not know how to encode type {}'
                          .format(value.__class__.__name__))


def _msgpack_ext_hook(code: int, data: bytes) -> Any:
    if code == MSGPACK_EXT_TUPLE:
        content = _msgpack_decode(data)
        if not isinstance(content, list):
            raise DecodeError('incorrectly encoded tuple')
        return tuple(content)
    elif code == MSGPACK_EXT_COMPLEX128:
        if len(data) != 16:
            raise DecodeError('wrong length for COMPLEX128')
        return complex(*struct.unpack('>dd', data))
    elif code == MSGPACK_EXT_NDARRAY:
        return _decode_ndarray(data)
    elif code == MSGPACK_EXT_NUMPY_SCALAR:
        return _decode_numpy_scalar(data)
    else:
        raise DecodeError('unknown extension type {}'.format(code))


def _msgpack_encode(value: Any) -> bytes:
    return msgpack.packb(value, use_bin_type=True, strict_types=True,
                         default=_msgpack_default)


def _msgpack_decode(value: bytes) -> Any:
    # The max_*_len prevent a corrupted or malicious input from consuming
    # memory significantly in excess of the input size before it determines
    # that there isn't actually enough data to back it.
    max_len = len(value)
    return msgpack.unpackb(value, raw=False, ext_hook=_msgpack_ext_hook,
                           max_str_len=max_len,
                           max_bin_len=max_len,
                           max_array_len=max_len,
                           max_map_len=max_len,
                           max_ext_len=max_len)


def encode_value(value: Any, encoding: bytes = ENCODING_DEFAULT) -> bytes:
    """Encode a value to a byte array for storage in redis.

    Parameters
    ----------
    value
        Value to encode
    encoding
        Encoding method to use, one of the values in :const:`ALLOWED_ENCODINGS`

    Raises
    ------
    ValueError
        If `encoding` is not a recognised encoding
    EncodeError
        EncodeError if the value was not encodable with the chosen encoding.
    """
    if encoding == ENCODING_PICKLE:
        return pickle.dumps(value, protocol=PICKLE_PROTOCOL)
    elif encoding == ENCODING_MSGPACK:
        return ENCODING_MSGPACK + _msgpack_encode(value)
    else:
        raise ValueError('Unknown encoding {:#x}'.format(ord(encoding)))


def decode_value(value: bytes, allow_pickle: Optional[bool] = None) -> Any:
    """Decode a value encoded with :func:`encode_value`.

    The encoded value is self-describing, so it is not necessary to specify
    which encoding was used.

    Parameters
    ----------
    value : bytes
        Encoded value to decode
    allow_pickle : bool, optional
        If false, :const:`ENCODING_PICKLE` is disabled. This is useful for
        security as pickle decoding can execute arbitrary code. If the default
        of ``None`` is used, it is controlled by the
        KATSDPTELSTATE_ALLOW_PICKLE environment variable. If that is not set,
        the default is false. The default may also be overridden with
        :func:`set_allow_pickle`.

    Raises
    ------
    DecodeError
        if `allow_pickle` is false and `value` is a pickle
    DecodeError
        if there was an error in the encoding of `value`
    """
    global _warn_on_pickle

    if not value:
        raise DecodeError('empty value')
    elif value[:1] == ENCODING_MSGPACK:
        try:
            return _msgpack_decode(value[1:])
        except Exception as error:
            raise DecodeError(str(error))
    elif value[:1] <= ENCODING_PICKLE:
        if allow_pickle is None:
            with _pickle_lock:
                allow_pickle = _allow_pickle
                if _warn_on_pickle:
                    warnings.warn(PICKLE_WARNING, FutureWarning)
                    _warn_on_pickle = False
        if allow_pickle:
            try:
                return _pickle_loads(value)
            except Exception as error:
                raise DecodeError(str(error))
        else:
            raise DecodeError(PICKLE_ERROR)
    else:
        raise DecodeError('value starts with unrecognised header byte {!r} '
                          '(katsdptelstate may need to be updated)'.format(value[:1]))


def equal_encoded_values(a: bytes, b: bytes) -> bool:
    """Test whether two encoded values represent the same/equivalent objects.

    This is not a complete implementation. Mostly, it just checks that the
    encoded representation is the same, but to ease the transition to Python 3,
    it will also compare the values if both arguments decode to either
    ``bytes`` or ``str`` (and allows bytes and strings to be equal assuming
    UTF-8 encoding). However, it will not do this recursively in structured
    data.
    """
    if a == b:
        return True
    a = decode_value(a)
    b = decode_value(b)
    try:
        return ensure_binary(a) == ensure_binary(b)
    except TypeError:
        return False
