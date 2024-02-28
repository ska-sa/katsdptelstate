################################################################################
# Copyright (c) 2015-2023, National Research Foundation (SARAO)
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

import pytest
from unittest import mock
from typing import Any

import numpy as np

from ..encoding import encode_value, decode_value, ENCODING_PICKLE, ENCODING_MSGPACK
from ..errors import EncodeError, DecodeError


class _TestEncoding:
    """Test encode_value and decode_value.

    This must be subclassed to specify the encoding type.
    """

    encoding = b''      # Just to keep mypy happy - actual encoding provided by derived classes

    def _test_value(self, value: Any) -> None:
        encoded = encode_value(value, encoding=self.encoding)
        decoded = decode_value(encoded)
        assert type(value) == type(decoded)
        if isinstance(value, np.ndarray):
            np.testing.assert_array_equal(value, decoded)
        else:
            assert value == decoded

    def test_list_tuple(self) -> None:
        self._test_value(('a', 'tuple', ['with', ('embedded', 'list')]))

    def test_float(self) -> None:
        self._test_value(1.0)
        self._test_value(1.23456788901234567890)
        self._test_value(1e300)
        self._test_value(-1e-300)
        self._test_value(float('inf'))
        self._test_value(np.inf)

    def test_simple(self) -> None:
        self._test_value(True)
        self._test_value(False)
        self._test_value(None)

    def test_np_scalar(self) -> None:
        self._test_value(np.float32(5.5))
        self._test_value(np.float64(5.5))
        self._test_value(np.complex128(5.5 + 4.2j))
        self._test_value(np.bool_(True))
        self._test_value(np.bool_(False))
        self._test_value(np.int32(12345678))

    def test_complex(self) -> None:
        self._test_value(1.2 + 3.4j)

    def test_ndarray(self) -> None:
        self._test_value(np.array([1, 2, 3]))

    def test_structured_ndarray(self) -> None:
        dtype = np.dtype([('a', np.int32), ('b', np.int16, (3, 3))])
        self._test_value(np.zeros((2, 3), dtype))

    def test_nan(self) -> None:
        encoded = encode_value(np.nan, encoding=self.encoding)
        decoded = decode_value(encoded)
        assert np.isnan(decoded)

    @mock.patch('katsdptelstate.encoding._allow_pickle', False)
    # Ignore these warnings... If they ever become errors, they'll be turned into DecodeErrors.
    @pytest.mark.filterwarnings(r"ignore:Passing \(type, 1\):FutureWarning")
    @pytest.mark.filterwarnings(r"ignore:invalid escape sequence:DeprecationWarning")
    def test_fuzz(self) -> None:
        if self.encoding == ENCODING_PICKLE:
            pytest.skip("Pickles will exhaust memory or crash given a bad pickle")
        # Create an encoded string with a bit of everything
        orig = [('a str', b'bytes'), 3, 4.0, 5 + 6j, np.int32(1),
                True, False, None, np.array([[1, 2, 3]])]
        encoded = encode_value(orig, encoding=self.encoding)
        # Mess with it and check that no exceptions except DecodeError come back
        for i in range(len(encoded)):
            broken = bytearray(encoded)
            for j in range(256):
                broken[i] = j
                try:
                    decode_value(bytes(broken))
                except DecodeError:
                    pass
        # Same thing, but now truncate the message rather than mutating it
        for i in range(len(encoded)):
            try:
                decode_value(encoded[:i])
            except DecodeError:
                pass


@mock.patch('katsdptelstate.encoding._allow_pickle', True)
@mock.patch('katsdptelstate.encoding._warn_on_pickle', False)
class TestEncodingPickle(_TestEncoding):
    encoding = ENCODING_PICKLE


class TestEncodingMsgpack(_TestEncoding):
    encoding = ENCODING_MSGPACK

    def setup_method(self) -> None:
        self.object_dtype = np.dtype([('a', np.int32), ('b', np.object_)])
        self.object_array = np.zeros((3,), self.object_dtype)

    def test_ndarray_with_object(self) -> None:
        with pytest.raises(EncodeError):
            encode_value(self.object_array, encoding=self.encoding)

    def test_numpy_scalar_with_object(self) -> None:
        with pytest.raises(EncodeError):
            encode_value(self.object_array[0], encoding=self.encoding)

    def test_unhandled_type(self) -> None:
        with pytest.raises(EncodeError):
            encode_value(self, encoding=self.encoding)
