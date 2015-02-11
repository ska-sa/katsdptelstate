import socket
import struct
import netifaces

class Endpoint(object):
    """A TCP or UDP endpoint consisting of a host and a port"""

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def __eq__(self, other):
        return self.host == other.host and self.port == other.port

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        if ':' in self.host:
            # IPv6 address - escape it
            return '[{0}]:{1}'.format(self.host, self.port)
        else:
            return '{0}:{1}'.format(self.host, self.port)

    def __repr__(self):
        return 'Endpoint({0!r}, {1!r})'.format(self.host, self.port)

    def __iter__(self):
        """Support `tuple(endpoint)` for passing to a socket function"""
        return iter((self.host, self.port))

    def multicast_subscribe(self, sock):
        """If the address is an IPv4 multicast address, subscribe to the group
        on `sock`. Return `True` if the host is a multicast address.
        """
        try:
            raw = socket.inet_aton(self.host)
        except socket.error:
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
            except socket.error as e:
                raise ValueError(str(e))
        return Endpoint(host, port)
    return parser

def endpoint_list_parser(default_port, single_port=False):
    """Return a factory function that parses a string. The string comprises
    a comma-separated list, each element of which is of the form taken by
    :func:`endpoint_parser`. Optionally, the hostname may be followed by
    `+count`, where `count` is an integer specifying a number of sequential
    IP addresses. This variation is only valid with IPv4 addresses.

    If `single_port` is true, then it will reject any list that contains
    more than one distinct port number, as well as an empty list. This allows
    the user to determine a unique port for the list.
    """
    def parser(text):
        sub_parser = endpoint_parser(default_port)
        parts = text.split(',')
        ret = []
        for part in parts:
            endpoint = sub_parser(part.strip())
            pos = endpoint.host.rfind('+')
            if pos != -1:
                start = endpoint.host[:pos]
                count = int(endpoint.host[pos+1:])
                if count <= 0:
                    raise ValueError('bad count {0}'.format(count))
                try:
                    start_raw = struct.unpack('>I', socket.inet_aton(start))[0]
                    for i in range(start_raw, start_raw + count):
                        host = socket.inet_ntoa(struct.pack('>I', i))
                        ret.append(Endpoint(host, endpoint.port))
                except socket.error:
                    raise ValueError('invalid IPv4 address in {0}'.format(start))
            else:
                ret.append(endpoint)
        if single_port:
            if not ret:
                raise ValueError('empty list')
            else:
                for endpoint in ret:
                    if endpoint.port != ret[0].port:
                        raise ValueError('all endpoints must use the same port')
        return ret
    return parser
