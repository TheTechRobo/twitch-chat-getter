import asyncio, re, typing, time
import aiohttp
from rethinkdb import r
r.set_loop_type("asyncio")

from .log import logger

__all__ = ["fail_item", "task_disconnected", "request_item", "queue_item", "register_backfeed"]

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
    assert not result['errors'], repr(result)
    changes: list[dict] = result['changes']
    assert changes
    if len(changes) == 1:
        return changes[0]['old_val']
    raise RuntimeError(f"RethinkDB checked out too many items. ctx=<<{repr(changes)}>>")

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
        if res['errors']:
            logger.error(f"Database write failed! {repr(res)}")
            raise RuntimeError(f"Database write failed!")
        return res['generated_keys'][0]
    finally:
        try:
            await conn.close()
        except Exception:
            pass

async def wrap_db_result(result):
    assert not (await result)['errors']

async def register_backfeed(item: str, parent_item: str):
    conn = r.connect()
    try:
        await wrap_db_result(r.db("twitch").table("ctx").insert(
            {"type": "backfeed", "item": item, "parent_item": parent_item}
        ).run(conn))
    finally:
        try:
            await conn.close()
        except Exception:
            pass

VOD_ID_REGEX = re.compile(r"^https?://w?w?w?.?twitch.tv/videos/(\d+)")
CHANNEL_ID_REGEX = re.compile(r"^https?://w?w?w?\.?twitch\.tv/([\w]+)")

async def queue_item(item: str, reason: str, user: str, parent_item: typing.Optional[str] = None):
    global AIOHTTP_SESSION
    if not AIOHTTP_SESSION:
        AIOHTTP_SESSION = aiohttp.ClientSession(timeout=10)
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
    id = VOD_ID_REGEX.search(item)
    if id:
        expires = None
        is_channel = False
    else:
        id = CHANNEL_ID_REGEX.search(item)
        is_channel = True
        expires = int(time.time()) + 48 * 3600 # expires in 48 hours
        if not id:
            raise QueuingError("Invalid VOD or channel URL")
    id = id.group(1).lower()
    if is_channel:
        id = f"c{id}"
    return [await add_to_db(item, reason, user, expires, parent_item)], []

async def get_item(ident: str):
    conn = await r.connect()
    try:
        if res := await r.db("twitch").table("todo").get(ident).run(conn):
            return res
        if res := await r.db("twitch").table("error").get(ident).run(conn):
            res['status'] = "error"
            return res
    finally:
        try:
            await conn.close()
        except Exception:
            pass

async def get_item_children(ident: str, filter=(lambda _ : True)) -> tuple[list[dict], list[dict]]:
    conn = await r.connect()
    try:
        items, errors = [], []
        async for item in r.db("twitch").table("todo").get_all(ident, index="queued_for_item").run(conn):
            if filter(item):
                items.append(item)
        async for item in r.db("twitch").table("error").get_all(ident, index="queued_for_item").run(conn):
            item['status'] = "error"
            if filter(item):
                errors.append(item)
        return items, errors
    finally:
        try:
            await conn.close()
        except Exception:
            pass

async def get_queue_status():
    conn = await r.connect()
    try:
        todo_count = await r.db("twitch").table("todo").get_all("todo", index="status").count().run(conn)
        claims_count = await r.db("twitch").table("todo").get_all("claims", index="status").count().run(conn)
        return {"todo": todo_count, "claims": claims_count}
    finally:
        try:
            await conn.close()
        except Exception:
            pass
