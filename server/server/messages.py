# Message handlers

__all__ = ("HANDLER_FUNCTIONS", "handler")

import functools, typing

from .shared import *

HANDLER_FUNCTIONS = {}

def _generate_handler_functions_skeleton():
    for state in ConnectionState.__members__.values():
        HANDLER_FUNCTIONS[state] = {}
_generate_handler_functions_skeleton()

def handler(func=None, *, states: typing.Union[None, ConnectionState, typing.Iterable[ConnectionState]], name: str):
    if func:
        if states is None:
            states = list(ConnectionState.__members__.values())
        if isinstance(states, ConnectionState):
            states = (states,) # convert it to a tuple so it is iterable
        for state in states:
            if name in HANDLER_FUNCTIONS[state]:
                raise ValueError(f"Duplicate command name {name} for the same state {state}")
            HANDLER_FUNCTIONS[state][name] = func
        return func
    return functools.partial(handler, states=states, name=name)

