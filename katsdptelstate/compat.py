from __future__ import print_function, division, absolute_import

import itertools

import redis
import distutils.version


# There won't correctly handle a fakeredis 0.x client when redis-py 3.x is
# installed. However, the last fakeredis 0.x version has a dependency on
# redis<3 to prevent this.
if distutils.version.StrictVersion(redis.__version__) >= '3.0':
    def zadd(client, key, values):
        """Provide redis-py 3 interface to either v2 or v3 client"""
        return client.zadd(key, values)
else:
    def zadd(client, key, values):
        """Provide redis-py 3 interface to either v2 or v3 client"""
        # We can't just use **values, because the names might not be native
        # strings.
        it = itertools.chain(*((score, name) for (name, score) in values.items()))
        return client.zadd(key, *it)
