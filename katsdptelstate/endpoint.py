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


import socket
import struct

import ipaddress
import netifaces


class Endpoint:
    """A TCP or UDP endpoint consisting of a host and a port"""

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def __eq__(self, other):
        return isinstance(other, Endpoint) and self.host == other.host and self.port == other.port

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.host, self.port))

    def __str__(self):
        if ':' in self.host:
            # IPv6 address - escape it
            return f'[{self.host}]:{self.port}'
        else:
            return f'{self.host}:{self.port}'

    def __repr__(self):
        return f'Endpoint({self.host!r}, {self.port!r})'

    def __iter__(self):
        """Support `tuple(endpoint)` for passing to a socket function"""
        return iter((self.host, self.port))

    def multicast_subscribe(self, sock):
        """If the address is an IPv4 multicast address, subscribe to the group
        on `sock`. Return `True` if the host is a multicast address.
        """
        try:
            raw = socket.inet_aton(self.host)
        except OSError:
            return False
        else:
            # IPv4 multicast is the range 224.0.0.0 - 239.255.255.255
            if raw[0] >= chr(224) and raw[0] <= chr(239):
                for iface in netifaces.interfaces():
                    for addr in netifaces.ifaddresses(iface).get(netifaces.AF_INET, []):
                        # Skip point-to-point links (includes loopback)
                        if 'peer' in addr:
                            continue
                        if_raw = socket.inet_aton(addr['addr'])
                        mreq = struct.pack("4s4s", raw, if_raw)
                        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                        break  # Should only need to subscribe once per interface
                return True
            else:
                return False


def endpoint_parser(default_port):
    """Return a factory function that parses a string. The string is either
    `hostname`, or `hostname:port`, where `port` is an integer. IPv6 addresses
    are written in square brackets (similar to RFC 2732) to disambiguate the
    embedded colons.
    """
    def parser(text):
        port = default_port
        # Find the last :, which should separate the port
        pos = text.rfind(':')
        # If the host starts with a bracket, do not match a : inside the
        # brackets.
        if len(text) and text[0] == '[':
            right = text.find(']')
            if right != -1:
                pos = text.rfind(':', right + 1)
        if pos != -1:
            host = text[:pos]
            port = int(text[pos+1:])
        else:
            host = text
        # Strip the []
        if len(host) and host[0] == '[' and host[-1] == ']':
            # Validate the IPv6 address
            host = host[1:-1]
            try:
                socket.inet_pton(socket.AF_INET6, host)
            except OSError as e:
                raise ValueError(str(e))
        return Endpoint(host, port)
    return parser


def endpoint_list_parser(default_port, single_port=False):
    """Return a factory function that parses a string. The string comprises
    a comma-separated list, each element of which is of the form taken by
    :func:`endpoint_parser`. Optionally, the hostname may be followed by
    `+count`, where `count` is an integer specifying a number of sequential
    IP addresses (in addition to the explicitly named one). This variation is
    only valid with IPv4 addresses.

    If `single_port` is true, then it will reject any list that contains
    more than one distinct port number, as well as an empty list. This allows
    the user to determine a unique port for the list.
    """
    def parser(text):
        sub_parser = endpoint_parser(default_port)
        parts = text.split(',')
        endpoints = []
        for part in parts:
            endpoint = sub_parser(part.strip())
            pos = endpoint.host.rfind('+')
            if pos != -1:
                start = endpoint.host[:pos]
                count = int(endpoint.host[pos+1:])
                if count < 0:
                    raise ValueError(f'bad count {count}')
                try:
                    start_raw = struct.unpack('>I', socket.inet_aton(start))[0]
                    for i in range(start_raw, start_raw + count + 1):
                        host = socket.inet_ntoa(struct.pack('>I', i))
                        endpoints.append(Endpoint(host, endpoint.port))
                except OSError:
                    raise ValueError(f'invalid IPv4 address in {start}')
            else:
                endpoints.append(endpoint)
        if single_port:
            if not endpoints:
                raise ValueError('empty list')
            else:
                for endpoint in endpoints:
                    if endpoint.port != endpoints[0].port:
                        raise ValueError('all endpoints must use the same port')
        return endpoints
    return parser


def endpoints_to_str(endpoints):
    """Convert a list of endpoints into a compact string that generates the
    same list. This is the inverse of
    :func:`katsdptelstate.endpoint.endpoint_list_parser`.
    """
    # Partition the endpoints by type
    ipv4 = []
    ipv6 = []
    other = []
    for endpoint in endpoints:
        # ipaddress module requires unicode, so convert if not already
        host = endpoint.host.decode('utf-8') if isinstance(endpoint.host, bytes) else endpoint.host
        try:
            ipv4.append(Endpoint(ipaddress.IPv4Address(host), endpoint.port))
        except ipaddress.AddressValueError:
            try:
                ipv6.append(Endpoint(ipaddress.IPv6Address(host), endpoint.port))
            except ipaddress.AddressValueError:
                other.append(endpoint)
    # We build a list of parts, each of which is either host:port, addr:port or
    # addr+n:port (where :port is omitted if None). These get comma-separated
    # at the end.
    parts = []
    for ip in (ipv4, ipv6):
        ip_parts = []    # lists of address, num, port (not tuples because mutated)
        # Group endpoints with the same port together, then order by IP address
        ip.sort(key=lambda endpoint: (endpoint.port is not None, endpoint.port, endpoint.host))
        for endpoint in ip:
            if (ip_parts and ip_parts[-1][2] == endpoint.port and
                    ip_parts[-1][0] + ip_parts[-1][1] == endpoint.host):
                ip_parts[-1][1] += 1
            else:
                ip_parts.append([endpoint.host, 1, endpoint.port])
        for (address, num, port) in ip_parts:
            if ip is ipv6:
                s = '[' + address.compressed + ']'
            else:
                s = address.compressed
            if num > 1:
                s += '+{}'.format(num - 1)
            if port is not None:
                s += f':{port}'
            parts.append(s)
    for endpoint in other:
        s = str(endpoint.host)
        if endpoint.port is not None:
            s += f':{endpoint.port}'
        parts.append(s)
    return ','.join(parts)
