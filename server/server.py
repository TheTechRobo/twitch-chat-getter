import asyncio, logging, json, traceback, os
import typing

logging.basicConfig(level=logging.DEBUG, force=True)

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
    logging.warning("DB returned invalid data")
    raise RuntimeError("RethinkDB checked out too many items.")

async def _request_item(conn):
    if item := await _get_item(conn, "priority"):
        return item
    if item := await _get_item(conn, "todo"):
        return item
    if item := await _get_item(conn, "backfeed"):
        return item
    if item := await _get_item(conn, "retry"):
        return item
    if item := await _get_item(conn, "retry2"):
        return item
    return None

async def request_item():
    conn = await r.connect()
    try:
        return await _request_item(conn)
    finally:
        try:
            await conn.close()
        except Exception:
            pass

CURRENT_ID = 1

def int_or_none(s):
    try:
        return int(s)
    except ValueError:
        return None

class ConnectionState:
    CLOSED = -100
    IGNORE = -1
    START = 0
    AUTHED = 1
    READY = 2
    TASK = 5
    UPLOAD = 10

NOPE = {"type": "item", "item": "", "started_by": None}

class Connection:
    def __init__(self, id: int, sock: websockets.WebSocketServerProtocol):
        logging.info(f"New client {id}")

        self.state = ConnectionState.START
        self.web = False
        self.conn = None
        self.sock = sock
        self.id = id
        self.ctask = None

        logging.info(f"Client {self.id} ready!")

    def debug(self, msg):
        logging.debug(f"Handler({self.id}): {msg}")

    def info(self, msg):
        logging.info(f"Handler({self.id}): {msg}")

    def warning(self, msg):
        logging.warning(f"Handler({self.id}): {msg}")

    def error(self, msg):
        logging.error(f"Handler({self.id}): {msg}")

    async def run(self, expr, tries=3):
        try:
            conn = await r.connect()
            return await expr.run(conn)
        except Exception as e:
            if tries > 0:
                self.error(f"Error occured while querying DB ({repr(e)}), retrying")
                return await self.run(expr, tries-1)
            self.error(f"Error occured while querying DB ({repr(e)}), giving up")
            raise

    async def _start(self):
        async for sm in self.sock:
            # Ensure one client can't starve other coroutines of resources.
            await asyncio.sleep(0)
            # Load message. If that doesn't work, close the connection.
            ml = len(sm)
            try:
                data = json.loads(sm)
            except json.JSONDecodeError:
                self.warning("Unparseable message, closing connection")
                await self.sock.close(1008, "JSON decode error")
                self.state = ConnectionState.CLOSED
                break
            if "type" not in data:
                self.warning("Invalid message, closing connection")
                await self.sock.close(1008, "Invalid message structure")
                self.state = ConnectionState.CLOSED
                break
            mtype = data['type']
            self.info(f"Message {mtype}({ml})")

            if auth := data.get("auth"):
                if self.state < ConnectionState.START: # untrusted, disconnected, etc
                    self.warning(f"Untrusted client (state {self.state}) attempted to authenticate")
                    continue
                if self.state >= ConnectionState.AUTHED:
                    self.warning(f"Already authed, reauthenticating")
                self.info(f"Authenticating with {auth}")
                result = await self.run(r.db("twitch").table("secrets").get(auth))
                if result:
                    if result.get("kick"):
                        await self.sock.close(1008, result.get("Kreason", ""))
                        self.warning(f"Kicking due to policy")
                        self.state = ConnectionState.CLOSED
                        break
                    if result.get("web"):
                        self.info("New web client just dropped")
                        self.state = ConnectionState.IGNORE
                        self.web = True
                        WEB_CLIENTS.append(self)
                    self.info("Authentication accepted")
                    self.state = ConnectionState.AUTHED
                else:
                    self.warning("Access denied")
                    self.state = ConnectionState.IGNORE
                    continue

            if self.state < ConnectionState.AUTHED:
                self.warning("Message without authentication")

            if mtype == "afternoon":
                version = data.get("version")
                if not version:
                    self.warning("No version provided")
                    await self.sock.close(1008, "Container is out of date.")
                    break
                self.info("Version: {version}")
                self.state = ConnectionState.READY
                continue

            if self.state < ConnectionState.READY:
                self.warning("Client is too eager")
                await self.sock.close(1008, "Container is out of date.")

            # start READY block
            if self.state == ConnectionState.READY:
                if mtype == "get":
                    if self.ctask:
                        self.error("State contradiction (READY vs ctask); bailing out")
                        # There should not be an item running in the READY state
                        raise RuntimeError("State contradiction (READY vs ctask)")
                    if STOP_FLAG.is_set():
                        self.info("Stop flag is set")
                        response = NOPE | {"suppl": "NO_NEW_SERVES"}
                        await self.sock.send(json.dumps(response))
                        continue
                    try:
                        item = await request_item()
                    except Exception as ename:
                        self.error(f"Error when requesting item: {repr(ename)}")
                        response = NOPE | {"suppl": "ERROR"}
                        await self.sock.send(json.dumps(response))
                        continue
                    if not item:
                        self.info("No items found")
                        item = NOPE
                    self.ctask = item['id']
                    self.info(f"Sending {item} to client")
                    await self.sock.send(json.dumps(item))
                    self.state = ConnectionState.TASK
                    continue
            # end READY block
        # end loop
        if task := self.ctask:
            await taskDisconnected(self.id, task)
        self.ctask = None
        self.info("Connection lost")

    async def start(self):
        await self._start()

async def connectionHandlerWrapper(websocket: websockets.WebSocketServerProtocol):
    global CURRENT_ID
    cid = CURRENT_ID
    CURRENT_ID += 1
    logging.info(f"Handling connection (cid: {cid})")
    try:
        conn = Connection(cid, websocket)
    except Exception:
        logging.error(f"Can't make connection handler for client {cid}")
        await websocket.close(1011, "Internal Server Error")
        raise
    try:
        await conn.start()
    except Exception:
        logging.error(f"Error occured during connection handler for client {cid}:")
        logging.error(repr(traceback.format_exc()))
        if task := conn.ctask:
            logging.info(f"Failing item {task} because {cid} disconnected")
            await taskDisconnected(cid, task)
        await websocket.close(1011, "Internal Server Error")
        raise
    logging.info("Connection {cid} finished")

STOP_SERVER = asyncio.Future()

async def main():
    port = int(os.environ['WSPORT'])
    async with websockets.serve(connectionHandlerWrapper, "", port, max_size=4*1024*1024, max_queue=16):
        try:
            await STOP_SERVER
        except KeyboardInterrupt:
            STOP_FLAG.set()
            await STOP_SERVER


if __name__ == "__main__":
    asyncio.run(main())
