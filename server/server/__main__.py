import asyncio, json, traceback, os
import typing, random, signal
import warnings as _ # allows tracemalloc to show unclosed sockets; export PYTHONTRACEMALLOC=<a value> to enable

from .shared import *
from .connection import *
from .db import *
from .irc import irc

logger.info("> Begin new log session")

import websockets

CURRENT_ID = 1

async def connectionHandlerWrapper(websocket: websockets.WebSocketServerProtocol):
    global CURRENT_ID
    cid = CURRENT_ID
    CURRENT_ID += 1
    logger.info(f"Handling connection (cid: {cid})")
    if DISCONNECT_CLIENTS.is_set():
        logger.info(f"Kicking {cid} as we are shutting down")
        await websocket.close(1001, "Not accepting connections")
        return
    try:
        conn = Connection(cid, websocket)
    except Exception:
        logger.error(f"Can't make connection handler for client {cid}: {repr(traceback.format_exc())}")
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
        print("The server is already stopping as fast as possible.")
        return
    if DISCONNECT_CLIENTS.is_set():
        print("Stopping immediately.")
        STOP_SERVER.set()
        return
    print("> Press Ctrl-C again to stop immediately")
    print("! Stopping when current tasks are complete...")
    DISCONNECT_CLIENTS.set()

get_non_web_clients = lambda : [i for i in CONNECTIONS if not i.web]
get_web_clients = lambda : [i for i in CONNECTIONS if i.web]

async def check():
    await DISCONNECT_CLIENTS.wait()
    while clients := get_non_web_clients():
        print("Shuting down", len(clients), "clients")
        for client in clients:
            async with client.busy:
                if client.state < ConnectionState.TASK:
                    try:
                        await client.sock.close(1001, "Shutting down")
                    except Exception:
                        logger.warning(f"Could not close handler {client.id}")
                    client.disconnected = True
        if get_non_web_clients():
            await asyncio.sleep(5)
    for client in get_web_clients():
        async with client.busy:
            await client.sock.close(1001, "Shutting down")
            client.disconnected = True
    STOP_SERVER.set()

async def main():
    # Clear out claims
    async for job in get_all_claimed_jobs():
        await irc.fail_item(job, "Tracker died while item was claimed")

    task = asyncio.create_task(check())
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, signal_handler)
    port = int(os.environ['WSPORT'])
    async with websockets.serve(connectionHandlerWrapper, "", port, max_size=4*1024*1024, max_queue=16, ping_timeout=None) as server:
        await STOP_SERVER.wait()
        server.close(False) # check() would have already handled this
    print("! The server has shut down.")
    task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
