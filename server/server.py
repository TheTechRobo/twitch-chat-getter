import asyncio, logging, json, traceback, os
import typing, random, signal

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format="# [%(asctime)s] %(levelname)s %(message)s (%(lineno)d/%(funcName)s/%(filename)s)", encoding="utf-8", errors="backslashreplace")

logger.info("> Begin new log session")

import websockets
from rethinkdb import r
r.set_loop_type("asyncio")

async def failItem(task, reason):
    # Allows up to 3 retries before moving item to the `error` table and sending details to IRC
    raise NotImplementedError()

async def taskDisconnected(cid, task):
    # Fail the item
    id = task['id']
    logger.info(f"Client {cid} disconnected while working on item {id}")
    await failItem(task, "Client disconnected")

WEB_CLIENTS = []

# Stop flag: Stops item serves if set. Used when the server is shutting down
STOP_FLAG: asyncio.Event = asyncio.Event()
# Pause flag: Stops item serves if set. Set manually by the IRC bot
PAUSE_FLAG = asyncio.Event()

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

# Queues earlier in this list will be drained before queues later in the list.
# When an item fails and is retried, it is placed in the next queue.
# (If there is no queue to move it to, the item is not moved.)
QUEUES = ["priority", "todo", "backfeed", "aux", "aux2"]

async def _request_item(conn):
    for queue in QUEUES:
        if item := await _get_item(conn, queue):
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
        logger.info(f"New client {id}")

        self.state = ConnectionState.START
        self.web = False
        self.conn = None
        self.sock = sock
        self.id = id
        self.ctask = None

        # Try to prevent log injection
        self.logcount = random.randint(0, 9)
        logger.info(f"Handler({self.id}): Starting with log number {self.logcount}")

        logger.info(f"Client {self.id} ready!")

    def debug(self, msg):
        if logger.isEnabledFor(logging.DEBUG):
            self.logcount += 1
            logger.debug(f"Handler({self.id})[{self.logcount}]: {msg}")

    def info(self, msg):
        self.logcount += 1
        logger.info(f"Handler({self.id})[{self.logcount}]: {msg}")

    def warning(self, msg):
        self.logcount += 1
        logger.warning(f"Handler({self.id})[{self.logcount}]: {msg}")

    def error(self, msg):
        self.logcount += 1
        logger.error(f"Handler({self.id})[{self.logcount}]: {msg}")

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
                else:
                    self.warning(f"Message type {repr(mtype)} is not recognised in this context (READY)")
                    response = {"type": "response", "response": "error", "reason": "unrecognised_command"}
                    await self.sock.send(json.dumps(response))
                    continue
            # end READY block

            # begin TASK block
            elif self.state == ConnectionState.TASK:
                if False:
                    pass
                else:
                    self.warning(f"Message type {repr(mtype)} is not recognised in this context")
                    response = {"type": "response", "response": "error", "reason": "unrecognised_command"}
                    await self.sock.send(json.dumps(response))
                    continue
            # end TASK block
            else:
                self.warning(f"Unknown state {self.state}")
                response = {"type": "response", "response": "error", "reason": "internal_error"}
                await self.sock.send(json.dumps(response))
                continue
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
    logger.info(f"Handling connection (cid: {cid})")
    try:
        conn = Connection(cid, websocket)
    except Exception:
        logger.error(f"Can't make connection handler for client {cid}")
        await websocket.close(1011, "Internal Server Error")
        raise
    try:
        await conn.start()
    except Exception:
        logger.error(f"Error occured during connection handler for client {cid}:")
        logger.error(repr(traceback.format_exc()))
        if task := conn.ctask:
            logger.info(f"Failing item {task} because {cid} disconnected")
            await taskDisconnected(cid, task)
        await websocket.close(1011, "Internal Server Error")
        raise
    logger.info("Connection {cid} finished")

STOP_SERVER = asyncio.Event()

# I would use Task.cancel, but then we can't have the "press ctrl-c twice to exit" feature
def signal_handler():
    if STOP_SERVER.is_set():
        print("Pressing Ctrl-C again won't do anything. What a waste of time.")
        return
    if STOP_FLAG.is_set():
        print("Stopping immediately.")
        STOP_SERVER.set()
        return
    print("> Press Ctrl-C again to stop immediately")
    print("! Stopping when current tasks are complete...")
    STOP_FLAG.set()

async def main():
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, signal_handler)
    port = int(os.environ['WSPORT'])
    async with websockets.serve(connectionHandlerWrapper, "", port, max_size=4*1024*1024, max_queue=16):
        await STOP_SERVER.wait()
    print("! The server has shut down!")

if __name__ == "__main__":
    asyncio.run(main())
