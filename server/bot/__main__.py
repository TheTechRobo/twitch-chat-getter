import asyncio
import os
import re
import time
import typing
from common.irc import try_upload_file
import common.db as db
from .bot import Bot, Prefix
from common.http import CLIENT_SESSION as http
from common.log import logger

import arrow

POST_URL = os.environ['H2IBOT_POST_URL']
STREAM_URL = os.environ['H2IBOT_STREAM_URL']

bot = Bot(STREAM_URL, POST_URL)

async def main():
    try:
        await bot.run_forever()
    except KeyboardInterrupt:
        # TODO: Wait until commands are finished running
        print("stop")

IS_TRANSFER_URL = re.compile(r"^https?://transfer.archivete\.am/(?:inline/)?[^/]")

async def get_item_details(job: str) -> list[dict[str, typing.Any]]:
    results = []
    if IS_TRANSFER_URL.search(job):
        async with http.get(job) as resp:
            async for line in resp.content:
                results.append(await get_item_details(line.decode()))
    else:
        if res := await db.get_item(job):
            results.append(res)
            child_items, child_errors = await db.get_item_children(job)
            child_unfinished_items, child_finished_items = [], []
            for i in child_items:
                if i['status'] == "done":
                    child_finished_items.append(i)
                else:
                    child_unfinished_items.append(i)
            res['children'] = child_unfinished_items, child_finished_items, child_errors
        else:
            results.append({"id": job})
    return results

async def generate_status_message(job: str) -> list[str]:
    messages = []
    details = await get_item_details(job)
    for detail in details:
        if "status" not in detail:
            messages.append(f"Job {detail['id']} doesn't seem to exist.")
            continue
        if detail['item'].startswith("c"):
            item = f"https://twitch.tv/{detail['item'][1:]}"
        else:
            item = f"https://twitch.tv/videos/{detail['item']}"
        message = f"Job {detail['id']} is in {detail['status']}. It scraped {item}."
        message += f"Queued {arrow.get(detail['queued_at'])}"
        if finished_ts := detail.get("finished_at"):
            message += f"; finished at {arrow.get(finished_ts)}"
        message += ". "
        child_unfinished_items, child_finished_items, child_errors = detail['children']
        if any((child_unfinished_items, child_finished_items, child_errors)):
            ul = len(child_unfinished_items)
            fl = len(child_finished_items)
            el = len(child_errors)
            message += f"Item has {ul+fl+el} child items, {fl} of which have finished, and {el} of which finished with errors. "
        if expires := detail.get("expires"):
            tense = "Expires" if expires > time.time() else "Expired"
            ts = arrow.get(expires).humanize(granularity=["hour", "minute"])
            message += "{tense} {ts}."
        messages.append(message)
    return messages

@bot.command("!status")
async def status(self, user, ran, *jobs):
    """
    Gets the number of jobs in each queue, or the current status of a job.
    Examples:
    - !status
    > 0 jobs in todo, 0 jobs in claims.
    (todo is the queue; claims are in-progress jobs.)
    - !status 1319f607-38e6-4210-a3ed-4a540424a6fb
    > Shows the information about that job.
    - !status 1319f607-38e6-4210-a3ed-4a540424a6fb 8b1d2d80-7a8e-43e6-8f6f-1cb171f3bf69
    > Shows the information about those two jobs.
    """
    if jobs:
        for job in jobs:
            if not job:
                continue
            msg = await generate_status_message(job)
            if len(msg) > 1:
                u = await try_upload_file("https://transfer.archivete.am/btt-bulk-job-status", "\n".join(msg)+"\n")
                msg = f"There are multiple messages for {job}, so go here: {u}"
            yield msg
        return
    data = await db.get_queue_status()
    yield f"{data['todo']} jobs in todo, {data['claims']} jobs in claims."

asyncio.run(main())
