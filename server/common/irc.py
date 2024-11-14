import asyncio
from .db import *

import aiohttp, logging

class MessageSendError(Exception): pass
class StatusCodeError(Exception): pass

class IrcBot:
    def __init__(self, stream_url: str, post_url: str):
        self.stream_url = stream_url
        self.post_url = post_url
        self.session = None

    async def send_message(self, message: str):
        if not self.session:
            self.session = aiohttp.ClientSession()
        async with self.session.post(self.post_url, data=message) as response:
            if response.status != 200:
                raise MessageSendError(response.status)

    async def reply(self, author: str, message: str):
        if not self.session:
            self.session = aiohttp.ClientSession()
        if author:
            await self.send_message(f"{author}: {message}")
        else:
            await self.send_message(message)

    async def __aiter__(self):
        if not self.session:
            self.session = aiohttp.ClientSession()
        tries = 1
        while True:
            try:
                async with self.session.get(self.stream_url) as response:
                    if response.status != 200:
                        raise StatusCodeError(response.status)
                    async for line in response.content:
                        yield line.decode()
            except Exception:
                logging.exception("Exception when streaming:")
            delay = min(4*tries, 60)
            logging.info(f"Try {tries}. Waiting {delay}s before reconnecting")
            await asyncio.sleep(delay)
            tries += 1

    @staticmethod
    async def prettify_item(item: str) -> str:
        return f"https://twitch.tv/{item}" if item.startswith("c") else f"https://twitch.tv/videos/{item}"

    async def fail_item(self, item, reason):
        verdict, job = await fail_item(item, reason)
        parent = None
        if verdict == Decision.FAILED_FINISHED:
            job, parent = job
        assert isinstance(job, dict)
        if verdict == Decision.FAILED:
            job['item'] = await self.prettify_item(job['item'])
            await self.send_message(f"{job['started_by']}: Your job for {job['item']} failed. Use !status {job['id']} for details.")
        if parent:
            parent['item'] = await self.prettify_item(parent['item'])
            await self.send_message(f"{parent['started_by']}: Your job for {parent['item']} failed. Use !status {parent['id']} for details.")

    async def finish_item(self, ident: str):
        item, errors = await finish_item(ident)
        if item:
            it = await get_item(item)
            with_errors = " with errors" if errors else ""
            await self.send_message(f"{it['started_by']}: Your job {it['id']} for {await self.prettify_item(it['item'])} has finished{with_errors}.")

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
