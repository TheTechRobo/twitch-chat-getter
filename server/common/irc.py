import asyncio
from .db import *

import aiohttp, logging

class MessageSendError(Exception): pass
class StatusCodeError(Exception): pass

class IrcBot:
    def __init__(self, stream_url: str, post_url: str):
        self.stream_url = stream_url
        self.post_url = post_url
        self.session = aiohttp.ClientSession()

    async def send_message(self, message: str):
        async with self.session.post(self.post_url, data=message) as response:
            if response.status != 200:
                raise MessageSendError(response.status)

    async def reply(self, author: str, message: str):
        if author:
            await self.send_message(f"{author}: {message}")
        else:
            await self.send_message(message)

    async def __aiter__(self):
        async with self.session.get(self.stream_url) as response:
            if response.status != 200:
                raise StatusCodeError(response.status)
            async for line in response.content:
                yield line.decode()

AIOHTTP_SESSION = None

async def try_upload_file(url: str, data: str):
    global AIOHTTP_SESSION
    if not AIOHTTP_SESSION:
        AIOHTTP_SESSION = aiohttp.ClientSession(timeout=10)
    tries = 4
    attempts = 0
    while attempts < tries:
        async with AIOHTTP_SESSION.put(url, data=data) as resp:
            if resp.status == 200:
                url = await resp.text()
                return url
            await asyncio.sleep(2**attempts)
            attempts += 1
    raise RuntimeError("Couldn't upload file to transfer")
