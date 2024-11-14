from common.db import *
from rethinkdb import r

async def get_all_claimed_jobs():
    conn = await r.connect()
    async for job in await r.db("twitch").table("todo").get_all("claims", index="status").run(conn):
        yield job['id']
    await conn.close()

