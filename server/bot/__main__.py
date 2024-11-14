import asyncio
import os
import re
import time
import typing
import random

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
        message = f"Job {detail['id']} (for {item}) is in {detail['status']}. "
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
            message += f"{tense} {ts}. "
        if detail.get("try", 0) != 0:
            if detail['status'] == "error":
                message += f"Tried {detail['try']} times before failing. "
                message += f"Last error: {repr(detail['errorReasons'][-1])}"
            elif detail['status'] == "done":
                message += "Finished in {detail['try']} attempts. "
            else:
                message += f"Attempt {detail['try'] + 1}. "
                message += f"Last error: {repr(detail['errorReasons'][-1])}"
        messages.append(message)
    return messages

@bot.command("!status")
async def status(self: Bot, user, ran, *jobs):
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
            else:
                msg = msg[0]
            yield msg
    else:
        data = await db.get_queue_status()
        yield f"{data['todo']} jobs in todo, {data['claims']} jobs in claims."

@bot.command("!sutats")
async def sutats(self: Bot, user, ran, *args):
    async for message in status(self, user, ran, *args):
        yield message[::-1] # Don't ask. I don't know either.

@bot.command(Prefix("!s"))
async def stauts(self: Bot, user, ran, *args):
    async for message in status(self, user, ran, *args):
        yield "".join(random.sample(list(message), len(message)))

@bot.command("!help")
async def help(self: Bot, user, ran, command=None):
    if command:
        if not command.startswith("!"):
            command = "!" + command
        runner = self.lookup_command(command)
        if not runner or not runner.help:
            yield f"{command} does not exist or is undocumented."
            return
        for line in runner.help.split("\n"):
            line = line.strip()
            if line:
                yield line
        if random.randint(0, 50) == 42:
            yield "fireonlive is awesome"
        return
    text = ("List of commands:",
            "!status <IDENTIFIER> [IDENTIFIERS...]: Returns the status of the given job(s) (e.g. !status 1319f607-38e6-4210-a3ed-4a540424a6fb). Does not currently work with URLs.",
            "!status: Returns the list of jobs in each queue.",
            "!a <URL> [EXPLANATION]: Archives the metadata of a twitch VOD or channel by its URL, saving the explanation into the database.",
            "Be sure to provide explanations for your jobs, and remember that everything queued here takes up space on IA.",
            "Please note that when a channel is queued here, only the metadata of the VODs will be saved, excluding clips and other channel content. To test what will be discovered, use yt-dlp (relevant code: https://github.com/TheTechRobo/twitch-chat-getter/blob/4f11b65e394e2d2f94e7e8f6cb1ed451eeb99ca1/client.py#L138-L151 )",
            "Also, archiving in bulk with transfer.archivete.am URLs works. This also applies to !status.",
            "You can find the data on IA here: https://archive.org/details/archiveteam_twitch_metadata")
    for line in text:
        line = line.strip()
        if line:
            yield line

asyncio.run(main())
