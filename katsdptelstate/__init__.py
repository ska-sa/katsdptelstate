from .telescope_state import TelescopeState

# Attempt to determine installed package version
# borrowed from katpoint
try:
    import pkg_resources as _pkg_resources
except ImportError:
    __version__ = "unknown"
else:
    try:
        dist = _pkg_resources.get_distribution("katpoint")
        # ver needs to be a list since tuples in Python <= 2.5 don't have
        # a .index method.
        ver = list(dist.parsed_version)
        __version__ = "r%d" % int(ver[ver.index("*r") + 1])
        del dist, ver
    except (_pkg_resources.DistributionNotFound, ValueError, IndexError, TypeError):
        __version__ = "unknown"
