import json
import websockets, logging, asyncio, random

from rethinkdb import r
from websockets.frames import CloseCode

logger = logging.getLogger(__name__)

from .messages import *
from .shared import *
from .db import *
from . import handlers

__all__ = ["Connection"]

def parse_version(version):
    try:
        date, seq = version.split(".")
    except ValueError:
        return None
    # todo: min version check
    return version

class Connection:
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self):
        raise TypeError("The `id` property is read-only")

    @id.deleter
    def id(self):
        raise TypeError("The `id` property cannot be deleted")

    def __hash__(self) -> int:
        return self.id

    def __init__(self, id: int, sock: websockets.WebSocketServerProtocol):
        logger.info(f"New client {id}")

        self.disconnected = False
        self.state = ConnectionState.START
        self.web = False
        self.sock = sock
        # self._id is not mutable; *do not* change it.
        # You will break everything if you change it outside of __init__.
        # This is the direct return value of __hash__.
        self._id = id
        self.ctask = None
        self.busy = asyncio.Lock()

        # Set up handlers
        self.commands = {}
        print(HANDLER_FUNCTIONS)
        for state, items in HANDLER_FUNCTIONS.items():
            self.commands[state] = {}
            for name, i in items.items():
                self.commands[state][name] = i

        assert self not in CONNECTIONS

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

    async def send_response(self, response, data={}):
        """
        Sends a response to the client
        """
        await self.sock.send(json.dumps(data | {"type": "response", "response": response, "seq": self.seq}))

    async def _start(self):
        if DISCONNECT_CLIENTS.is_set():
            await self.sock.close(CloseCode.GOING_AWAY, "Not accepting new connections")
            return

        async for sm in self.sock:
            async with self.busy:
                if self.disconnected:
                    break

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
                self.seq = data.get("seq")

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
                        self.info("Authentication accepted")
                        self.state = ConnectionState.AUTHED
                    else:
                        self.warning("Access denied")
                        self.state = ConnectionState.IGNORE
                        continue

                if mtype == "ping":
                    await self.sock.pong()
                    continue

                if self.state < ConnectionState.AUTHED:
                    self.warning("Message without authentication")

                if mtype == "afternoon":
                    version = parse_version(data.get("version"))
                    if not version:
                        self.warning(f"Bad or missing version: {data.get('version')}")
                        await self.sock.close(1008, "Container is out of date.")
                        break
                    self.info(f"Version: {version}")
                    self.version = version
                    await self.send_response("welcome")
                    self.state = ConnectionState.READY
                    continue

                if self.state < ConnectionState.READY:
                    self.warning("Client is too eager")
                    await self.sock.close(1008, "Container is out of date.")

                commands = HANDLER_FUNCTIONS[self.state]
                if command := commands[mtype]:
                    print("Running handler", command.__name__)
                    await command(self, data)
                else:
                    self.warning(f"Message type {repr(mtype)} is not recognised in this context ({self.state})")
                    response = {"type": "response", "response": "error", "reason": "unrecognised_command", "seq": self.seq}
                    await self.sock.send(json.dumps(response))
                    continue

        # end loop
        self.info("Connection lost")

    async def start(self):
        assert self not in CONNECTIONS
        CONNECTIONS.add(self)
        try:
            await self._start()
        except websockets.exceptions.ConnectionClosedError:
            self.info("Connection closed uncleanly")
        finally:
            CONNECTIONS.remove(self)
            if task := self.ctask:
                await taskDisconnected(self.id, task)
            self.ctask = None

