from .telescope_state import TelescopeState, InvalidKeyError, ImmutableKeyError, ArgumentParser

# Attempt to determine installed package version
# borrowed from katpoint
try:
    import pip
except ImportError:
    __version__ = "unknown"
else:
    try:
        dist = next(d for d in pip.get_installed_distributions()
                    if d.key == "katsdptelstate")
        __version__ = dist.version
        del dist
    except StopIteration:
        __version__ = "unknown"
