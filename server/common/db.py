import asyncio, re, typing, time
import aiohttp
from rethinkdb import r
r.set_loop_type("asyncio")

__all__ = ["fail_item", "task_disconnected", "request_item", "queue_item"]

# Queues earlier in this list will be drained before queues later in the list.
# When an item fails and is retried, it is placed in the next queue.
# (If there is no queue to move it to, the item is not moved.)
QUEUES = ["priority", "todo", "backfeed", "aux", "aux2"]

async def fail_item(task: str, reason: str):
    # Allows up to 3 retries before moving item to the `error` table and sending details to IRC
    raise NotImplementedError()

async def task_disconnected(cid, id):
    # Fail the item
    await task_disconnected(id, "Client disconnected")

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

AIOHTTP_SESSION = None

class QueuingError(Exception): pass
class EmptyFileError(Exception): pass

async def add_to_db(item: str, reason: str, user: str, expires: typing.Optional[int], parent_item: typing.Optional[str] = None):
    conn = await r.connect()
    try:
        previous_runs = r.db("twitch").table("todo").get_all(item, index="item").order_by(index=r.desc("expires")).run(conn)
        async for run in previous_runs:
            if item[0] != 'c':
                # Not a channel
                raise QueuingError(f"Item has already been run; please try !status {run['id']}")
            if run['status'] != "done":
                # Not complete
                raise QueuingError(f"Item is already queued or running; please try !status {run['id']}")
            if run.get("expires", 0) and run['expires'] > time.time():
                # Not expired
                raise QueuingError(f"Item has not yet expired; please try !status {run['id']}")
            break # only look at the first one
        entry = {
            "item": item,
            "started_by": user,
            "status": "todo",
            "queued_at": time.time(),
            "explain": reason,
            "expires": expires,
            "queued_for_item": parent_item
        }
        res = await r.db("twitch").table("todo").insert(entry).run(conn)
        return res['generated_keys'][0]
    finally:
        try:
            await conn.close()
        except Exception:
            pass

async def queue_item(item: str, reason: str, user: str, parent_item: typing.Optional[str] = None):
    global AIOHTTP_SESSION
    if not AIOHTTP_SESSION:
        AIOHTTP_SESSION = aiohttp.ClientSession(timeout=10)
    conn = await r.connect()
    if re.search(r"^https?://transfer.archivete\.am/(?:inline/)?[^/]", item):
        ids, errors = [], []
        async with AIOHTTP_SESSION.get(item) as response:
            async for line in response.content:
                try:
                    newid = await queue_item(line.decode(), reason, user, parent_item)
                except QueuingError as e:
                    errors.append(f"Item {line} could not be queued: {repr(e)}")
                else:
                    ids.append(newid)
        if not ids and not errors:
            raise EmptyFileError("File did not contain any items.")
        return ids, errors
    id = re.search(r"^https?://w?w?w?.?twitch.tv/videos/(\d+)", item)
    if id:
        expires = None
        is_channel = False
    else:
        id = re.search(r"^https?://w?w?w?\.?twitch\.tv/([\w]+)", item)
        is_channel = True
        expires = int(time.time()) + 48 * 3600 # expires in 48 hours
        if not id:
            raise QueuingError("Invalid VOD or channel URL")
    id = id.group(1).lower()
    if is_channel:
        id = f"c{id}"
    return [await add_to_db(item, reason, user, expires, parent_item)], []
