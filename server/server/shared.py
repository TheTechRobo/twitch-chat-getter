import functools, enum, asyncio

__all__ = [
    "logger", "ConnectionState", "int_or_none", "CONNECTIONS", "DISCONNECT_CLIENTS", "PAUSE_FLAG"
]

from common.log import logger

@functools.total_ordering # we only have to implement __lt__ to allow comparison operators
@enum.unique
class ConnectionState(enum.Enum):
    CLOSED = -100
    IGNORE = -1
    START = 0
    AUTHED = 1
    READY = 2
    TASK = 5
    UPLOAD = 10

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

def int_or_none(s):
    if s is None:
        return None
    try:
        return int(s)
    except ValueError:
        return None

CONNECTIONS = set()

# Stop flag: Stops item serves if set. Used when the server is shutting down
DISCONNECT_CLIENTS: asyncio.Event = asyncio.Event()
# Pause flag: Stops item serves if set. Set manually by the IRC bot
PAUSE_FLAG = asyncio.Event()

