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

"""Tests for the Endpoint class."""

import pytest
from katsdptelstate.endpoint import (
    Endpoint, endpoint_parser, endpoint_list_parser, endpoints_to_str)


class TestEndpoint:
    def test_str(self) -> None:
        assert 'test.me:80' == str(Endpoint('test.me', 80))
        assert '[1080::8:800:200C:417A]:12345' == str(Endpoint('1080::8:800:200C:417A', 12345))

    def test_repr(self) -> None:
        assert "Endpoint('test.me', 80)" == repr(Endpoint('test.me', 80))

    def test_parser_default_port(self) -> None:
        parser = endpoint_parser(1234)
        assert Endpoint('hello', 1234) == parser('hello')
        assert Endpoint('192.168.0.1', 1234) == parser('192.168.0.1')
        assert Endpoint('1080::8:800:200C:417A', 1234) == parser('[1080::8:800:200C:417A]')

    def test_parser_port(self) -> None:
        parser = endpoint_parser(1234)
        assert Endpoint('hello', 80) == parser('hello:80')
        assert Endpoint('1080::8:800:200C:417A', 80) == parser('[1080::8:800:200C:417A]:80')

    def test_bad_ipv6(self) -> None:
        parser = endpoint_parser(1234)
        pytest.raises(ValueError, parser, '[notipv6]:1234')

    def test_iter(self) -> None:
        endpoint = Endpoint('hello', 80)
        assert ('hello', 80) == tuple(endpoint)

    def test_eq(self) -> None:
        assert Endpoint('hello', 80) == Endpoint('hello', 80)
        assert Endpoint('hello', 80) != Endpoint('hello', 90)
        assert Endpoint('hello', 80) != Endpoint('world', 80)
        assert Endpoint('hello', 80) != 'not_an_endpoint'

    def test_hash(self) -> None:
        assert hash(Endpoint('hello', 80)) == hash(Endpoint('hello', 80))
        assert hash(Endpoint('hello', 80)) != hash(Endpoint('hello', 90))


class TestEndpointList:
    def test_parser(self) -> None:
        parser = endpoint_list_parser(1234)
        endpoints = parser(
            'hello:80,world,[1080::8:800:200C:417A],192.168.0.255+4,10.0.255.255+3:60')
        expected = [
            Endpoint('hello', 80),
            Endpoint('world', 1234),
            Endpoint('1080::8:800:200C:417A', 1234),
            Endpoint('192.168.0.255', 1234),
            Endpoint('192.168.1.0', 1234),
            Endpoint('192.168.1.1', 1234),
            Endpoint('192.168.1.2', 1234),
            Endpoint('192.168.1.3', 1234),
            Endpoint('10.0.255.255', 60),
            Endpoint('10.1.0.0', 60),
            Endpoint('10.1.0.1', 60),
            Endpoint('10.1.0.2', 60)
        ]
        assert expected == endpoints

    def test_parser_bad_count(self) -> None:
        with pytest.raises(ValueError):
            endpoint_list_parser(1234)('192.168.0.1+-4')

    def test_parser_non_integer_count(self) -> None:
        with pytest.raises(ValueError):
            endpoint_list_parser(1234)('192.168.0.1+hello')

    def test_parser_count_without_ipv4(self) -> None:
        with pytest.raises(ValueError):
            endpoint_list_parser(1234)('hello.world+4')

    def test_parser_single_port(self) -> None:
        parser = endpoint_list_parser(1234, single_port=True)
        endpoints = parser('hello:1234,world')
        expected = [Endpoint('hello', 1234), Endpoint('world', 1234)]
        assert expected == endpoints

    def test_parser_single_port_bad(self) -> None:
        pytest.raises(ValueError, endpoint_list_parser(1234, single_port=True), 'x:123,y:456')


def test_endpoints_to_str() -> None:
    endpoints = [
        Endpoint('hostname', 1234),
        Endpoint('1.2.3.4', 7148),
        Endpoint('1.2.3.3', 7148),
        Endpoint('1.2.3.3', None),
        Endpoint('1.2.3.3', 7149),
        Endpoint('1.2.3.5', 7148),
        Endpoint('192.168.1.255', None),
        Endpoint('192.168.2.0', None),
        Endpoint('::10ff', 7148),
        Endpoint('::20ff', 7148),
        Endpoint('::0000:2100', 7148),
        Endpoint('hostname1', None)
    ]
    s = endpoints_to_str(endpoints)
    assert s == (
        '1.2.3.3,192.168.1.255+1,1.2.3.3+2:7148,1.2.3.3:7149,[::10ff]:7148,'
        '[::20ff]+1:7148,hostname:1234,hostname1'
    )
