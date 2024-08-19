import aiohttp, asyncio

__all__ = ["CLIENT_SESSION"]

async def make_client_session():
    return aiohttp.ClientSession()

CLIENT_SESSION = asyncio.run(make_client_session())
