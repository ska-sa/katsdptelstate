from .telescope_state import (TelescopeState, ConnectionError, InvalidKeyError,
                              ImmutableKeyError, TimeoutError, CancelledError,
                              DecodeError, EncodeError,
                              PICKLE_PROTOCOL, encode_value, decode_value,
                              ALLOWED_ENCODINGS, ENCODING_DEFAULT,
                              ENCODING_PICKLE, ENCODING_MSGPACK)

# BEGIN VERSION CHECK
# Get package version when locally imported from repo or via -e develop install
try:
    import katversion as _katversion
except ImportError:
    import time as _time
    __version__ = "0.0+unknown.{}".format(_time.strftime('%Y%m%d%H%M'))
else:
    __version__ = _katversion.get_version(__path__[0])
# END VERSION CHECK
