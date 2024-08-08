# Message handlers

__all__ = ("ConnectionState", "HANDLER_FUNCTIONS")

import functools, enum, typing

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

HANDLER_FUNCTIONS = {}

def _generate_handler_functions_skeleton():
    if HANDLER_FUNCTIONS: return
    for state in ConnectionState.__members__.values():
        HANDLER_FUNCTIONS[state] = {}

def handler(func=None, *, states: typing.Union[None, ConnectionState, typing.Iterable[ConnectionState]], name: str):
    if func:
        _generate_handler_functions_skeleton()
        if states is None:
            states = list(ConnectionState.__members__.values())
        if isinstance(states, ConnectionState):
            states = (states,)
        for state in states:
            if name in HANDLER_FUNCTIONS[state]:
                raise ValueError(f"Duplicate command name {name} for the same state {state}")
            HANDLER_FUNCTIONS[state][name] = func
        return func
    return functools.partial(handler, states=states, name=name)

