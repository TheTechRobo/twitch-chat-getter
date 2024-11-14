# Message handlers
# Thank you to thuban and arkiver for helping me decide on the implementation here!

__all__ = ["HANDLER_FUNCTIONS"]

import json, typing, functools, os.path, os

from common.irc import try_upload_file
from .shared import *
from .db import *
from . import irc

UPLOAD_URL = os.environ['TARGET_URL']

if typing.TYPE_CHECKING:
    # Circular imports are fun
    from .connection import Connection

NOPE = {"type": "item", "item": "", "started_by": None, "id": None}

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

# Connection state: READY

@handler(states=ConnectionState.READY, name="get")
async def get(self: "Connection", message: dict):
    if self.ctask:
        # There should not be an item running in the READY state
        self.error("State contradiction (READY vs ctask); bailing out")
        raise RuntimeError("State contradiction (READY vs ctask)")
    if DISCONNECT_CLIENTS.is_set() or PAUSE_FLAG.is_set():
        response = NOPE | {"suppl": "NO_NEW_SERVES"}
        await self.sock.send(json.dumps(response))
        return
    try:
        item = await request_item()
    except Exception as ename:
        self.error(f"Error when requesting item: {repr(ename)}")
        response = NOPE | {"suppl": "ERROR"}
        await self.sock.send(json.dumps(response))
        return
    if item:
        self.state = ConnectionState.TASK
        self.info(f"Sending {item}")
    else:
        # No items found
        item = NOPE
    self.ctask = item['id']
    await self.send_response("item", item)

# Connection state: TASK

# Why did I capitalise WLOG?
@handler(states=ConnectionState.TASK, name="WLOG")
async def log(self: "Connection", msg: dict):
    pass

@handler(states=ConnectionState.TASK, name="status")
async def update_status(self: "Connection", msg: dict):
    await self.send_response("ok")

@handler(states=ConnectionState.TASK, name="feed")
async def submit_to_backfeed(self: "Connection", msg: dict):
    item = msg['item']
    assert item == self.ctask
    item_for = msg['item_for']
    user = msg['person']
    reason = msg['reason']
    if " " in item or not item or not reason:
        self.warning(f"Bad backfeed! {repr(item)} : {repr(reason)}")
        await self.send_response("invalid_item_name")
        return
    await register_backfeed(item, item_for)
    await irc.send_message(f"!a {item} {reason}")
    ids, errors = await queue_item(item, reason, user, item_for)
    if errors:
        error_url = await try_upload_file("https://transfer.archivete.am/pebbles-queuing-errors", "\n".join(errors)+"\n")
        if ids:
            await irc.reply(user, f"{len(errors)} items could not be queued; check {error_url} for more details.")
        else:
            await irc.reply(user, f"No items could be queued ({len(errors)} errors); check {error_url} for more details.")
    if ids:
        await irc.reply(user, f"Queued {len(ids)} discovered items from item {item_for}.")
    await self.send_response("ok")

@handler(states=ConnectionState.TASK, name="error")
async def error(self: "Connection", msg: dict):
    id = msg['id']
    reason = msg['reason']
    await irc.fail_item(id, f"*{reason}")
    self.ctask = None

@handler(states=ConnectionState.TASK, name="warn")
async def warm(self: "Connection", msg: dict):
    await irc.warn(self.ctask, msg['msg'])
    await self.send_response("ok")

@handler(states=ConnectionState.TASK, name="upload")
async def negotiate(self: "Connection", _msg: dict):
    await self.send_response("upload", {"status": "ok", "url": UPLOAD_URL})

@handler(states=ConnectionState.TASK, name="done")
async def done(self: "Connection", msg: dict):
    assert self.ctask
    assert msg['id'] == self.ctask
    await irc.finish_item(self.ctask)
    self.ctask = None
    self.state = ConnectionState.READY
    await self.send_response("ok")

