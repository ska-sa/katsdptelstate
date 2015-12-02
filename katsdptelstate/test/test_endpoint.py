"""Tests for the Endpoint class."""

from nose.tools import assert_equal, assert_raises

from katsdptelstate.endpoint import Endpoint, endpoint_parser, endpoint_list_parser


class TestEndpoint(object):
    def test_str(self):
        assert_equal('test.me:80', str(Endpoint('test.me', 80)))
        assert_equal('[1080::8:800:200C:417A]:12345', str(Endpoint('1080::8:800:200C:417A', 12345)))

    def test_repr(self):
        assert_equal("Endpoint('test.me', 80)", repr(Endpoint('test.me', 80)))

    def test_parser_default_port(self):
        parser = endpoint_parser(1234)
        assert_equal(Endpoint('hello', 1234), parser('hello'))
        assert_equal(Endpoint('192.168.0.1', 1234), parser('192.168.0.1'))
        assert_equal(Endpoint('1080::8:800:200C:417A', 1234), parser('[1080::8:800:200C:417A]'))

    def test_parser_port(self):
        parser = endpoint_parser(1234)
        assert_equal(Endpoint('hello', 80), parser('hello:80'))
        assert_equal(Endpoint('1080::8:800:200C:417A', 80), parser('[1080::8:800:200C:417A]:80'))

    def test_bad_ipv6(self):
        parser = endpoint_parser(1234)
        assert_raises(ValueError, parser, '[notipv6]:1234')

    def test_iter(self):
        endpoint = Endpoint('hello', 80)
        assert_equal(('hello', 80), tuple(endpoint))

class TestEndpointList(object):
    def test_parser(self):
        parser = endpoint_list_parser(1234)
        endpoints = parser('hello:80,world,[1080::8:800:200C:417A],192.168.0.255+4,10.0.255.255+3:60')
        expected = [
            Endpoint('hello', 80),
            Endpoint('world', 1234),
            Endpoint('1080::8:800:200C:417A', 1234),
            Endpoint('192.168.0.255', 1234),
            Endpoint('192.168.1.0', 1234),
            Endpoint('192.168.1.1', 1234),
            Endpoint('192.168.1.2', 1234),
            Endpoint('10.0.255.255', 60),
            Endpoint('10.1.0.0', 60),
            Endpoint('10.1.0.1', 60)
        ]
        assert_equal(expected, endpoints)

    def test_parser_bad_count(self):
        assert_raises(ValueError, endpoint_list_parser(1234), '192.168.0.1+-4')

    def test_parser_non_integer_count(self):
        assert_raises(ValueError, endpoint_list_parser(1234), '192.168.0.1+hello')

    def test_parser_count_without_ipv4(self):
        assert_raises(ValueError, endpoint_list_parser(1234), 'hello.world+4')

    def test_parser_single_port(self):
        parser = endpoint_list_parser(1234, single_port=True)
        endpoints = parser('hello:1234,world')
        expected = [Endpoint('hello', 1234), Endpoint('world', 1234)]
        assert_equal(expected, endpoints)

    def test_parser_single_port_bad(self):
        assert_raises(ValueError, endpoint_list_parser(1234, single_port=True), 'x:123,y:456')
