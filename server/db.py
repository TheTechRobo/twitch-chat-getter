from rethinkdb import r
r.set_loop_type("asyncio")

from .shared import *

__all__ = ["failItem", "taskDisconnected", "request_item"]

# Queues earlier in this list will be drained before queues later in the list.
# When an item fails and is retried, it is placed in the next queue.
# (If there is no queue to move it to, the item is not moved.)
QUEUES = ["priority", "todo", "backfeed", "aux", "aux2"]

async def failItem(task, reason):
    # Allows up to 3 retries before moving item to the `error` table and sending details to IRC
    raise NotImplementedError()

async def taskDisconnected(cid, id):
    # Fail the item
    logger.info(f"Client {cid} disconnected while working on item {id}")
    await failItem(id, "Client disconnected")

async def _get_item(conn, queue: str):
    result = await r.db("twitch").table("todo").get_all(queue, index="status").sample(1) \
        .update({"status": "claims"}, return_changes=True).run(conn)
    if result['replaced'] == 0:
        return None
    changes: list[dict] = result['changes']
    if not changes:
        return None
    if len(changes) == 1:
        return changes[0]['old_val']
    logger.warning("DB returned invalid data")
    raise RuntimeError("RethinkDB checked out too many items.")

async def request_item():
    conn = await r.connect()
    try:
        for queue in QUEUES:
            if item := await _get_item(conn, queue):
                return item
        return None
    finally:
        try:
            await conn.close()
        except Exception:
            pass

