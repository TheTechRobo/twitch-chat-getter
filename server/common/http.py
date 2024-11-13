import aiohttp, asyncio

__all__ = ["CLIENT_SESSION"]

async def _make_client_session():
    return aiohttp.ClientSession()

CLIENT_SESSION = asyncio.run(_make_client_session())
