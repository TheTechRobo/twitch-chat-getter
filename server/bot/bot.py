import functools, inspect, json, traceback, enum

import aiohttp
from common.irc import IrcBot

import arrow

class Prefix(str): pass

class Command:
    def __init__(self: "Command", match: str | Prefix, r, required_modes):
        self.match = match
        self.runner = r
        self.required_modes = required_modes

    async def __call__(self: "Command", bot, user, ran, *args):
        if modes := self.required_modes:
            success = False
            for mode in modes:
                if mode in user['modes']:
                    success = True
            if not success:
                return

        argspec = inspect.getfullargspec(self.runner)
        # Take the number of arguments, subtract the number of arguments with default values, then subtract
        # the number of arguments that are not from the message.
        minArgs = len(argspec.args) - len(argspec.defaults or ()) - 3
        if argspec.varargs:
            maxArgs = 5000
        else:
            maxArgs = len(argspec.args) - 3
        if len(args) < minArgs:
            bot.reply(user['nick'], f"Not enough arguments for command {ran}.")
            return
        if len(args) > maxArgs:
            bot.reply(user['nick'], f"Too many arguments for command {ran}.")
            return
        async for msg in self.runner(bot, user, ran, *args):
            yield msg

class Bot:
    """
    IRC bot that can connect to http2irc servers.
    """
    def __init__(self: "Bot", stream_url: str, post_url: str):
        """
        Constructs the IRC bot.
        Arguments:
            streamUrl(str): The http2irc stream URL.
            postUrl(str):   The http2irc message sending URL.
        """
        self.commands = []
        self.irc = IrcBot(stream_url, post_url)

    def command(self, f=None, *, match=None, requiredModes=None):
        if f and isinstance(f, (str, set, Prefix)):
            return functools.partial(self.command, match=f)
        elif f:
            if (not match):
                raise ValueError("match arg is required")
            cmd = Command(match, f, requiredModes)
            cmd.__name__ = match
            self.commands.append(cmd)
            return cmd
        raise ValueError("first arg must be function or match")

    async def parse_irc_line(self, line: dict):
        command = line['command']
        if command == "PRIVMSG":
            user = line['user']
            author = user['nick']
            if author == "h2ibot":
                return # don't process our own messages
            message = line['message']
            args = message.split(" ")
            mtime = arrow.Arrow.fromtimestamp(line['time']).format()
            print(f"[{mtime}] <{author}> {message}")
            for runner in self.commands:
                if isinstance(runner.match, Prefix):
                    if not args[0].startswith(runner.match):
                        continue
                elif isinstance(runner.match, str):
                    if args[0] != runner.match:
                        continue
                elif type(runner.match) == set:
                    for match in runner.match:
                        if args[0] != match:
                            continue
                else:
                    await self.irc.reply(user['nick'], "Task failed spectacularly.")
                if args:
                    args_ = args[1:]
                else:
                    args_ = []
                try:
                    async for message in runner(self, user, args[0], *args_):
                        await self.irc.reply(user['nick'], message)
                except Exception:
                    await self.irc.reply(author, "An error occured while processing the command")
                    traceback.print_exc()

    async def run_forever(self):
        await self.irc.reply("", "Bot loaded.")
        async for line in self.irc:
            await self.parse_irc_line(json.loads(line))

