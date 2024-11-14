from common.irc import IrcBot, try_upload_file

import os

__all__ = ['send_message']

irc = IrcBot(None, os.environ['H2IBOT_POST_URL'])

async def send_message(message: str):
    await irc.send_message(message)

async def reply(author: str, message: str):
    await irc.reply(author, message)

async def fail_item(item: str, reason: str):
    await irc.fail_item(item, reason)

async def finish_item(ident: str):
    await irc.finish_item(ident)

async def warn(ident: str, message: str):
    await irc.warn_item(ident, message)

