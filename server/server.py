import asyncio, logging, uuid, json, traceback

import websockets
from rethinkdb import r
r.set_loop_type("asyncio")

async def failItem(task, reason):
    # Allows up to 3 retries before moving item to the `error` table and sending details to IRC
    raise NotImplementedError()

async def taskDisconnected(cid, task):
    # Fail the item
    id = task['id']
    logging.info(f"Client {cid} disconnected while working on item {id}")
    await failItem(task, "Client disconnected")

WEB_CLIENTS = []

# Stop flag: Stops item serves if set
STOP_FLAG: asyncio.Event = asyncio.Event()

async def _get_item(conn, queue):
    result = await r.db("twitch").table("todo").getAll("todo", index="status").sample(1) \
        .update({"status": "claims"}, return_changes=True).run(conn)
    changes: list[dict] = result['changes']
    if not changes:
        return None
    if len(changes) == 1:
        return changes[0]['old_val']
    logging.warning("DB returned invalid data")
    raise RuntimeError("RethinkDB checked out too many items.")

async def request_item(conn):
    if item := await _get_item(conn, "todo"):
        return item
    if item := await _get_item(conn, "secondary"):
        return item
    if item := await _get_item(conn, "retry"):
        return item
    return None

CURRENT_ID = 1
async def connectionHandler(cid: int, websocket: websockets.WebSocketServerProtocol):
    logging.info(f"New client {cid}")
    CURRENT_TASK = None
    AFTERNOONED = False
    UNTRUSTED   = False
    AUTHED      = False
    WEB         = False
    CONN        = None
    async for message in websocket:
        # Check length of message.
        # Limited to 4 MiB because JSON memory usage is crazy.
        if len(message) > 4*1024*1024:
            logging.warning(f"Client sent message of length {len(message)}, closing connection")
            await websocket.close(1009, "Message too large")
            break
        # Load message. If that doesn't work, close the connection.
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logging.warning("Client sent unparseable message, closing connection")
            await websocket.close(1008, "JSON decode error")
            break
        if "type" not in data:
            logging.warning("Client sent invalid message, closing connection")
            await websocket.close(1008, "Invalid message structure")
            break
        logging.debug(f"Client {cid} sent a message of type {data['type']} and length {len(message)}")
        # Base message types
        if auth := data.get("auth"):
            logging.debug(f"Authenticating client {cid}")
            if UNTRUSTED:
                logging.warning(f"Untrusted client {cid} attempted to authenticate")
                continue
            result = await r.db("twitch").table("secrets").get(auth).run(CONN)
            if result:
                if result.get("kick"):
                    await websocket.close(1008, result['Kreason'])
                    logging.info(f"Kicking kickable client {cid} (secret used: {auth})")
                    break
                if result.get("web"):
                    logging.info("New web client {cid} just dropped using authentication {auth}.")
                    UNTRUSTED = True
                    AUTHED = False
                    WEB = True
                    continue
                logging.info(f"Client {cid} authed with {auth}.")
        if not AUTHED:
            logging.info(f"Client {cid} : message without proper auth")
            continue
        if data['type'] == "afternoon":
            if not data.get("version"):
                await websocket.close(1008, "No version provided. Please ensure your container is up to date.")
                break
            logging.info(f"Welcome client {cid} with version {data['version']}!")
            CONN = await r.connect()
            AFTERNOONED = True
            continue

        if WEB or not AFTERNOONED:
            logging.warning("Unauthorized client {cid} attempted a message")
            break

        msgType = data['type']
        if msgType == "get":
            if CURRENT_TASK:
                response = {"type": "item", "item": "", "started_by": None, "suppl": "UNFINISHED_ITEM"}
                await websocket.send(json.dumps(response))
                continue
            if STOP_FLAG:
                response = {"type": "item", "item": "", "started_by": None, "suppl": "NO_NEW_SERVES"}
                await websocket.send(json.dumps(response))
                continue
            item = await request_item(CONN)
            if not item: item = {"type": "item", "item": "", "started_by": None}
            CURRENT_TASK = item['id']
            logging.debug(f"Sending {item} to client {cid}")
            await websocket.send(json.dumps(item))
            continue

    # We or the client disconnected
    if CURRENT_TASK:
        await taskDisconnected(cid, CURRENT_TASK)
    logging.info(f"Lost connection to client {cid}")

async def connectionHandlerWrapper(cid, websocket):
    global CURRENT_ID
    cid = CURRENT_ID
    CURRENT_ID += 1
    logging.debug(f"Handling connection (cid: {cid})")
    try:
        await connectionHandler(cid, websocket)
    except Exception as ename:
        logging.error(f"Error occured during connection handler for client {cid}:")
        logging.error(repr(traceback.format_exc()))
        raise
    else:
        logging.debug("Connection {cid} finished")

async def main():
    async with websockets.serve(connectionHandlerWrapper, "", 9876):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
