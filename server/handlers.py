# Message handlers
# Thank you to thuban and arkiver for helping me decide on the implementation here!

__all__ = []

import json, typing

from .messages import handler
from .shared import *
from .db import *

if typing.TYPE_CHECKING:
    # Circular imports are fun
    from .connection import Connection

NOPE = {"type": "item", "item": "", "started_by": None, "id": None}

# Connection state: READY

@handler(states=ConnectionState.READY, name="get")
async def get(self: "Connection", message: dict):
    if self.ctask:
        # There should not be an item running in the READY state
        self.error("State contradiction (READY vs ctask); bailing out")
        raise RuntimeError("State contradiction (READY vs ctask)")
    if DISCONNECT_CLIENTS.is_set():
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
