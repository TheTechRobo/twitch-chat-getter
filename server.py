"""
   Copyright 2022-2023 TheTechRobo

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

clients = {}

import json
import os
import traceback
import hashlib
import threading
import re
import functools
import time
import shutil
import base64
import tempfile
import concurrent.futures as futures
import subprocess
import random

from itertools import permutations

from glob import glob
from pathlib import Path

import arrow
import requests

from websocket_server import WebsocketServer
from rethinkdb import r

# Called for every client connecting (after handshake)
def new_client(client, server):
    CLIENTS_DISCONNECTED.clear()
    print("New client connected and was given id %d" % client['id'])
    server.send_message(client, """
            {"type":"godot", "method":"ping"}""")
    client["auth"] = False
    client['tasks'] = {}
    clients[client['id']] = client


# Called for every client disconnecting
def client_left(client, _server):
    for (item, author) in clients[client['id']]['tasks'].values():
        reply(author, f"Your item {item['id']} for {item['item']} failed. (Reason: Client disconnected)")
        client['reason'] = "disconnect"
        client['moved_at'] = time.time()
        e = r.db("twitch").table("error").insert(r.db("twitch").table("todo").get(item['id']).run(conn)).run(conn)
        if e['errors']:
            reply("TheTechRobo", "D1F.")
            raise ValueError(e)
        e = r.db("twitch").table("todo").get(item['id']).delete(return_changes=True).run(conn)
        if e['errors']:
            reply(f"{author}, TheTechRobo", "Could not remove item from todo. Check logs.")
            raise ValueError(e)
    del clients[client['id']]
    if not clients:
        CLIENTS_DISCONNECTED.set()
    print(f"Client({client['id']}) disconnected")

pool = futures.ThreadPoolExecutor(max_workers=2)

def run_uploader_pipeline(uuid, dirname, chan):
    dirname_ = dirname
    conn = r.connect()
    try:
        things_to_do = (
                ("./untarrer.zsh", "UNTARRING_UPLOAD"),
                ("./verify_only_one.zsh", "VERIFYING_STRUCTURE"),
                ("SET_DIRNAME_TO_SUBDIRECTORY", "UPDATING_VARS"),
                ("./extract_urls.zsh", "EXTRACTING_URLS"),
                ("./tarrer.zsh", "TARRING_DIRECTORY"),
                (["./upload_to_ia.zsh", "{dirname}", chan], "UPLOADING_TO_IA")
        )
        for thing, status in things_to_do:
            print(thing, status)
            assert r.db("twitch").table("uploads").get(uuid).update({
                "status": status
            }).run(conn)['errors'] == 0
            print("updated")
            if type(thing) == str:
                thing = [thing, dirname]
            if thing[0] == "SET_DIRNAME_TO_SUBDIRECTORY":
                d = list(glob(dirname + "/*"))
                assert len(d) == 1
                dirname = d[0]
                continue
            for idx, param in enumerate(thing):
                thing[idx] = param.replace("{dirname}", dirname)
            subprocess.run(thing, check=True)
    except Exception as ename:
        r.db("twitch").table("uploads").get(uuid).update({
            "status": "FAILED"
        }).run(conn)
        traceback.print_exc()
        raise
    else:
        assert r.db("twitch").table("uploads").get(uuid).update({
            "status": "FINISHED",
            "completed": True
        }).run(conn)['errors'] == 0
        shutil.rmtree(dirname_)
    finally:
        conn.close()

# Called when a client sends a message
def message_received(client, server, message):
    if not clients[client['id']]['tasks'] and DISCONNECT_CLIENTS.is_set():
        client['handler'].send_close(1001, b"Server going down")
    if len(message) > 4*1024*1024: # 2 MiB
        client['handler'].send_close(1009, b"Max msg size is 1MiB")
    try:
        msg = json.loads(message)
    except Exception:
        return "Fail"
    if auth := msg.get("auth"):
        if clients[client['id']].get("untrusted"):
            return
        conn = r.connect()
        result = r.db("twitch").table("secrets").get(auth).run(conn)
        if result:
            print("Client", client, clients.get(client['id']), "auth'd.")
            clients[client['id']]['auth'] = True
        else:
            reply("TheTechRobo", "Received an unrecognised password.")
            print(f"Wrong password - received {auth}")
            print("Client:", client, clients.get(client['id']))
            clients[client['id']]['untrusted'] = True
        del conn, result, auth
    if msg["type"] ==   "ping":
        msg["type"] =  "godot"
        msg["method"] = "ping"
        server.send_message(client, json.dumps(msg))
        return
    if not clients[client['id']]['auth']:
        # Ignore their messages if they are not authenticated
        # maybe they'll be delayed by thinking it's a bad connection
        return
    if msg['type'] == "negotiate":
        result = None
        if msg['method'] == "chunk_size":
            result = 1*1024*1024 # 1 MiB
        server.send_message(client, json.dumps({"type": "negotiate",
            "result": result}))
        return
    if msg['type'] == "upload" and msg['method'] == "preflight":
        if PAUSE_UPLOADS.is_set():
            message = {"type": "nak", "reason": "Uploads manually paused"}
            server.send_message(client, json.dumps(message))
            return
        # Load disk space
        DATA_DIR = "data"
        free_space = shutil.disk_usage(DATA_DIR).free
        if (msg['approxSize'] * 4) > free_space:
            message = {"type": "nak", "reason": "Free space check failed"}
            server.send_message(client, json.dumps(message))
            return
        dirname = tempfile.mkdtemp(prefix="UPLOAD_TMP_", dir=DATA_DIR)
        print(dirname)
        conn = r.connect()
        query_result = r.db("twitch").table("uploads").insert({
            "status": "PREFLIGHT",
            "completed": False,
            "dir": dirname
        }).run(conn)
        if query_result['errors']:
            message = {"type": "nak", "reason": "Database query failed"}
            server.send_message(client, json.dumps(message))
            return
        clients[client['id']]['upload_uuid'] = query_result['generated_keys'][0]
        clients[client['id']]['dirname'] = dirname
        Path(f"{dirname}/data.tgz").touch()
        message = {"type": "mes", "action": "start"}
        server.send_message(client, json.dumps(message))
        return
    if msg['type'] == "chunk":
        uuid = clients[client['id']]['upload_uuid']
        conn = r.connect()
        assert r.db("twitch").table("uploads").get(uuid).update({
            "status": "UPLOADING"
        }).run(conn)['errors'] == 0
        conn.close()
        dirname = clients[client['id']]['dirname']
        file = f"{dirname}/data.tgz"
        with open(file, "ab") as f:
            f.write(base64.b85decode(msg['data']))
        message = {"type": "upload_ack", "num": msg['num']}
        server.send_message(client, json.dumps(message))
        return
    if msg['type'] == "fin":
        uuid = clients[client['id']]['upload_uuid']
        conn = r.connect()
        assert r.db("twitch").table("uploads").get(uuid).update({
            "status": "WAITING"
        }).run(conn)
        conn.close()
        # Start upload pipeline
        dirname = clients[client['id']]['dirname']
        chan = msg['chan']
        pool.submit(run_uploader_pipeline, uuid, dirname, chan)
        # Confirm to client that item has started
        message = {"type": "fin_ack"}
        server.send_message(client, json.dumps(message))
    if msg['type'] == "verify":
        dirname = clients[client['id']]['dirname']
        file = f"{dirname}/data.tgz"
        # read up to 8MiB at a time
        buffer_size = 16*1024*1024
        has = msg['hash']
        if has['type'] != "sha256":
            message = {"type": "verify_result", "res": "nosupportedhash"}
            server.send_message(client, json.dumps(message))
            return
        payload = has['payload']
        sha = hashlib.sha256()
        with open(file, "rb") as f:
            while data := f.read(buffer_size):
                sha.update(data)
        if sha.hexdigest() == payload:
            message = {"type": "verify_result", "res": "match"}
        else:
            message = {"type": "verify_result", "res": "mismatch"}
        server.send_message(client, json.dumps(message))
        return
    if msg['type'] == "upload_satuts":
        uuid = clients[client['id']]['upload_uuid']
        conn = r.connect()
        response = r.db("twitch").table("uploads").get(uuid).run(conn)
        message = {"type": "upload_status", "status": response['status']}
        server.send_message(client, json.dumps(message))
        return
    if msg["type"] == "get":
        if STOP_FLAG.is_set():
            message = {"type": "item", "item": "", "started_by": "", "suppl": "NO_NEW_SERVES"}
            server.send_message(client, json.dumps(message))
            return
        try:
            item = request_item(client)
            author, itemName = item['started_by'], item['item']
            if itemName and author:
                clients[client['id']]['tasks'][item['item']] = ((item, author))
            server.send_message(client, json.dumps(item))
            if itemName:
                print("Sent", itemName, "to client")
        except SyntaxError:
            client["handler"].send_close(1008, 'No Auth'.encode())
    elif msg["type"] == "warn":
        if not client['auth']:
            client['handler'].send_close(1008, "NO AUTH".encode())
            return
        item = msg['item']
        person = msg['person']
        message = msg['msg']
        reply(person, f"A warning was emitted on item {item}: {message}")
    elif msg["type"] == "done":
        if not client['auth']:
            client['handler'].send_close(1008, "No Auth".encode())
            return
        item = msg['item']
        ident = msg['id']
        data = finish_item(ident, client)
        user = data['started_by']
        otheritem = data.get("queued_for_item") or data['id']
        if True:
            otheritemname, items, errors = any_items_left(otheritem)
            if not items and not errors:
                extra = "(with errors)" if errors else ""
                reply(user, f"Your job {ident} for {otheritemname} has finished {extra}.")
        else:
            reply(user, f"Your job {ident} for {item} has finished.")
        del clients[client['id']]['tasks'][item]
    elif msg["type"] == "feed":
        item = msg['item']
        item_for = msg['item_for']
        user = msg['person']
        reason = msg['reason']
        if " " in item or not item or not reason:
            server.send_message(client, '{"type": "failure", "method": "backfeed", "reason": "space_in_item_name"}')
            print("Bad Backfeed", repr(item), repr(reason))
            return
        reply(".", f"!a {item} {reason}") # we ignore our own messages, so its fine
        start_pipeline_2w(item, user, reason, item_for=item_for)
    elif msg["type"] == "error":
        item = msg["item"]
        id = msg["id"]
        del clients[client['id']]['tasks'][item]
        try:
            d = error_item(item, id, client, msg['reason'])
            if d:
                user, id, e, ename = d
            else:
                user, id, e = "(?)", "(?)", None
            reply(user, f"Your job {id} for {item} on client {client['id']} failed. ({msg['reason']})")
            if e:
                reply(user, f"Your job {e} for {ename} finished with errors.")
        except SyntaxError:
            client['handler'].send_close(1008, "No Auth".encode())
    print("Client(%d) said: %s" % (client['id'], message))

def int_or_none(string):
    try:
        return int(string)
    except (ValueError, TypeError):
        pass

def any_items_left(id, onlydone=True):
    conn = r.connect()
    ritems = list(r.db("twitch").table("todo").get_all(id, index="queued_for_item").run(conn))
    items = []
    for item in ritems:
        if item['status'] == "done" and onlydone:
            continue
        items.append(item)
    error = list(r.db("twitch").table("error").get_all(id, index="queued_for_item").run(conn))
    item_data = r.db("twitch").table("todo").get(id).run(conn) \
            or r.db("twitch").table("error").get(id).run(conn)
    return item_data['item'], items, error

PORT=int_or_none(os.getenv("WSPORT")) or 9001
server = WebsocketServer(port=PORT, host="0.0.0.0")
server.set_fn_new_client(new_client)
server.set_fn_client_left(client_left)
server.set_fn_message_received(message_received)
server.run_forever(True)

print(f"Listening on port {PORT}.")
print("Server is up and running.")
print("Connecting to h2ibot...")

STREAM_URL = os.environ["H2IBOT_STREAM_URL"]
POST_URL   = os.environ["H2IBOT_POST_URL"]

stream = requests.get(STREAM_URL, stream=True)

def request_item(client):
    conn = r.connect()
    if not client['auth']:
        raise SyntaxError("Bad-Auth")
    d = list(r.db("twitch").table("todo").get_all("todo", index="status").sample(1).run(conn))
    if len(d) == 0:
        print("No items found")
        return {"id": "", "item": "", "started_by": "", "type": "item"}
    r.db("twitch").table("todo").get(d[0]['id']).update(
            {"status": "claims", "claimed_at": time.time()}
    ).run(conn)
    print("Sending", d[0], "to client")
    d[0]['type'] = "item"
    return d[0]

def error_item(item, id, client, reason):
    if not client['auth']:
        raise SyntaxError("Bad-Auth")
    conn = r.connect()
    errored = False
    data = r.db("twitch").table("todo").get(id).run(conn)
    if not data:
        data = r.db("twitch").table("todo").get(id).run(conn)
        errored = True
    if not data:
        reply(":WARNING", "This item no longer appears to exist! In order to prevent data loss, here is the data the client sent.")
        url = requests.put("https://transfer.archivete.am/pebbles-errlog", json={"item": item, "client": (client['id'], client['address']), "reason": reason, "time":time.time()}).text
        reply("", url)
        return
    data['moved_at'] = time.time()
    data['reason'] = reason
    data['client_failed'] = (client['id'], client['address'])
    if errored:
        r.db("twitch").table("error").get(id).update({
            "reason": reason,
            "client_failed": (client['id'], client['address'])
        })
    else:
        r.db("twitch").table("error").insert(data).run(conn)
    r.db("twitch").table("todo").get(data['id']).delete().run(conn)
    ename = None
    a = None
    if b := data.get("queued_for_item"):
        _, i, e = any_items_left(data['id'])
        if (not i) and (not e):
            ename = r.db("twitch").table("todo").get("queued_for_item").run(conn)
            a = b
    return data['started_by'], data['id'], data.get("queued_for_item"), ename

def finish_item(ident, client):
    if not client['auth']:
        raise SyntaxError("Bad-Auth")
    conn = r.connect()
    print("finish", r.db("twitch").table("todo").get(ident).run(conn))
    r.db("twitch").table("todo").get(ident).update(
        {"status": "done", "finished_at": time.time()}
    ).run(conn)
    print("Finished item")
    data = r.db("twitch").table("todo").get(ident).run(conn)
    print("print", data)
    return data

def add_to_db_2(item, author, explain, expires=None, item_for=None):
    conn = r.connect()
    if a := list(r.db("twitch").table("todo").get_all(item, index="item").run(conn)):
        # VODs should only have ever been run once.
        # If otherwise, fail.
        if item[0] != 'c':
            assert len(a) == 1
        a = a[0]
        ts = a.get("expires")
        if ts and a['status'] == "done" and ts < int(time.time()):
            pass
        else:
            raise Exception("Item has already been run, please try !status " + a['id'] + " ")
    a = {
        "item": item,
        "started_by": author,
        "status": "todo",
        "queued_at": time.time(),
        "explain": explain,
        "expires": expires,
        "queued_for_item": item_for
    }
    id = r.db("twitch").table("todo").insert(a).run(conn)['generated_keys'][0]
    return id

def generate_status_message(ident) -> str:
    results = get_item_details(ident)
    messages = []
    for result in results:
        try:
            if result.get("failedMiserably"):
                messages.append(f"Job {result['id']} never started: {result['full']}")
                continue
            if not result.get("ok"):
                messages.append(f"Job {result['id']} doesn't seem to exist.")
                raise Exception("do not")
        except Exception as ename:
            n = '\n'
            if str(ename) != "do not":
                messages.append(f"Failed to get job details for item {id}: {str(type(ename))} {str(ename).split(n)[0]}")
        else:
            item = result['item']
            ts = result.get("expires")
            if ts:
                tense = "will expire" if ts > time.time() else "expired"
                ts = arrow.get(ts).humanize(granularity=["hour", "minute"])

            queued_ts = "Queued %s" % arrow.get(result['queued_at'])
            finished_ts = ""
            if fints := result.get("finished_at"):
                finished_ts = "; finished %s" % arrow.get(fints)
            when = f"{queued_ts}{finished_ts}."

            tstext = f"Job {tense} {ts}." if ts else ""
            item_type = "VOD"
            if result['item'].startswith('c'):
                item_type = "channel"
                item = item[1:]
            children = ""
            if result.get("hasChildren") == "yes":
                children = " Has at least one child process. "
            elif result.get("hasChildren") == "yes:alldone":
                children = " Has at least one child process (all done). "
            elif result.get("hasChildren") == "yes:error":
                children = " Has at least one child process, at least one of which has failed. "
            messages.append(f"Job {result['id']} is in {result['status']}. It scraped {item_type} {item}. {when} {tstext}{children} This command will provide more details later.")
    if len(messages) > 1:
        class DummyResponse:
            status_code = 0
        resp = DummyResponse()
        while resp.status_code != 200:
            resp = requests.put("https://transfer.archivete.am/pebbles-job-status", data="\n\n".join(messages))
        url = resp.text.replace(".am/", ".am/inline/")
        return [f"There are multiple messages, so go here: {url}"]
    assert len(messages) == 1
    return messages


def get_item_details(ident) -> list[dict[str, str]]:
    conn = r.connect()
    idents = [ident]
    if re.search(r"^https?://transfer.archivete\.am/(?:inline/)?[^/]", ident):
        idents = requests.get(ident).text.split("\n")
    results = []
    for identifier in idents:
        if "could not be queued:" in identifier:
            url = re.search("^Item (.*) could not be queued: ", identifier).group(1)
            results.append({"id": url, "failedMiserably": True, "full": identifier})
            continue
        if data := r.db("twitch").table("todo").get(identifier) \
                .run(conn):
            _, items, errors = any_items_left(identifier, onlydone=False)
            for i in items:
                if i['status'] == "done":
                    if not data.get("hasChildren"): # don't if there are already unfinished ones
                        data['hasChildren'] = "yes:alldone"
                else:
                    data['hasChildren'] = "yes"
                break
            for i in errors:
                data['hasChildren'] = "yes:error"
                break
            data['ok'] = True
            results.append(data)
            continue
        if data1 := r.db("twitch").table("error").get(identifier) \
                .run(conn):
            data1['status'] = "error"
            data1['ok'] = True
            results.append(data1)
            continue
        results.append({"id": identifier})
    return results

def start_pipeline_2(item, author, explain, item_for=None):
    if re.search(r"^https?://transfer.archivete\.am/(?:inline/)?[^/]", item):
        ids = []
        try:
            fail = False
            for newitem in requests.get(item).text.strip().split("\n"):
                #print("Queue", newitem)
                d = start_pipeline_2(newitem, author, explain, item_for=item_for)
                if d['status']:
                    ids.append(d['id'])
                else:
                    ids.append(f"Item {newitem} could not be queued: {d['msg']}.")
                    fail = True
            url = requests.put("https://transfer.archivete.am/pebbles-bulk-ids", data="\n".join(ids)).text
            if fail:
                reply(author, f"At least one item could not be queued; check {url} for more details.")
            return {"status": True, "id": url}
        except Exception as ename:
            print(type(ename), repr(ename))
            raise
            #return {"status": False, "msg": str(ename).split("\n")[0]}
    id = re.search(r"^https?://w?w?w?.?twitch.tv/videos/(\d+)", item)
    expires = None
    is_channel = False
    if not id:
        id = re.search(r"^https?://w?w?w?\.?twitch\.tv/([\w]+)", item)
        is_channel = True
        expires = int(time.time()) + 48 * 3600 # expires in 48 hours
        if not id:
            return {"status":False,"msg":"That doesn't look like a valid VOD or channel URL (if this is a bug, file an issue or contact T.heTechRobo)"}
    id = id.group(1).lower()
    if is_channel:
        id = f"c{id}"
    try:
        return {"status": True, "id": add_to_db_2(id, author, explain, expires=expires, item_for=item_for)}
    except Exception as ename:
        n = "\n"
        traceback.print_exc()
        return {"status": False, "msg": f"{str(type(ename))} {str(ename).split(n)[0]}"}

def start_pipeline_2w(item, author, explain, item_for=None):
    res = start_pipeline_2(item, author, explain, item_for=item_for)
    if res['status']:
        reply(author, f"Queued {item} for chat archival. I will ping you when finished. Use !status {res['id']} for details.")
    else:
        n = "\n"
        reply(author, f"Couldn't queue your job for {item}. ({res['msg'].split(n)[0]})")


def get_status() -> dict[str, str]:
    conn = r.connect()
    todo_count = r.db("twitch").table("todo").get_all("todo", index="status").count().run(conn)
    claims_count = r.db("twitch").table("todo").get_all("claims", index="status").count().run(conn)
    return {"todo": todo_count, "claims": claims_count}

class Command:
    def __init__(self: "Command", match: str, r, requiredModes, preflight):
        self.match = match
        self.runner = r
        self.requiredModes = requiredModes
        self.preflight = preflight

    def __call__(self: "Command", bot, user, ran, *args) -> bool:
        """
        Returns:
            success(bool): False if the command did not match this function and the caller should continue searching for a working command.
        """
        if type(self.match) == set:
            none = True
            for match in self.match:
                if ran == match:
                    none = False
                    break
            if none:
                return False
        else:
            if ran != self.match:
                return False
        if not self.preflight(user, ran, args):
            return False
        if modes := self.requiredModes:
            success = False
            for mode in modes:
                if mode in user['modes']:
                    success = True
            if not success:
                return False
        self.runner(bot, user, ran, *args)
        return True

class IrcBot:
    """
    IRC bot that can connect to http2irc servers.
    """
    def __init__(self: "IrcBot", streamUrl: str, postUrl: str):
        """
        Constructs the IRC bot.
        Arguments:
            streamUrl(str): The http2irc stream URL.
            postUrl(str):   The http2irc message sending URL.
        """
        self.commands = []
        self.streamUrl = streamUrl
        self.postUrl = postUrl

    def command(self, f=None, *, match=None, requiredModes=None, preflight=lambda _user, _ran, _args : True):
        if f and (type(f) == str or type(f) == set):
            return functools.partial(self.command, match=f, preflight=preflight)
        elif f:
            if (not match):
                raise ValueError("match arg is required")
            cmd = Command(match, f, requiredModes, preflight)
            cmd.__name__ = match
            self.commands.append(cmd)
            return cmd
        raise ValueError("first arg must be function or match")

    def parse_irc_line(self, line: dict):
        user = line['user']
        author = user['nick']
        if author == "h2ibot":
            return # don't process our own messages
        command = line['command']
        if command == "PRIVMSG":
            message = line['message']
            args = message.split(" ")
            print(f"[{arrow.Arrow.fromtimestamp(line['time']).format()}] <{author}> {message}")
            for runner in self.commands:
                if type(runner.match) == str:
                    if args[0] != runner.match:
                        continue
                elif type(runner.match) == set:
                    for match in runner.match:
                        if args[0] != match:
                            continue
                else:
                    reply(user['nick'], "Task failed spectacularly.")
                if args:
                    args_ = args[1:]
                else:
                    args_ = []
                try:
                    status = runner(self, user, args[0], *args_)
                    if status:
                        return
                except Exception as ename:
                    self.reply(author, "An error occured while processing the command")
                    traceback.print_exc()

    def run_forever(self):
        self.reply("", "Server loaded.")
        for linee in stream.iter_lines():
            self.parse_irc_line(json.loads(linee.decode("utf-8")))

    def reply(self, user: str, message: str):
        startof = f"{user}: " if user else ""
        r = requests.post(self.postUrl, data=f"{startof}{message}".encode("utf-8"))
        assert r.status_code == 200, f"FAILED {user} {message} {r}"

def reply(user, message):
    startof = f"{user}: " if user else ""
    assert requests.post(POST_URL, data=f"{startof}{message}".encode("utf-8")).status_code == 200, f"FAILED {user} {message}"

print("Moving items around...")

# TODO: Put this in a function so it's not in the main scope.

# Moving claimed items to error
conn = r.connect()
cursor = r.db("twitch").table("todo").get_all("claims", index="status")
for entry in cursor.run(conn):
    prettifiedItem = f"https://twitch.tv/{entry['item'][1:]}" if entry['item'].startswith('c') else f"https://twitch.tv/videos/{entry['item']}"
    reply(entry["started_by"], f"Your job {entry['id']} for {prettifiedItem} failed. (Tracker died while item was claimed)")
    entry["moved_at"] = time.time()
    r.db("twitch").table("error").insert(entry).run(conn)
    r.db("twitch").table("todo").get(entry['id']).delete().run(conn)

cursor = r.db("twitch").table("uploads").get_all(False, index="completed")

for item in cursor.run(conn):
    reply(None, f"Cleaning up unfinished upload {item['id']}.")
    try:
        shutil.rmtree(item['dir'])
    except FileNotFoundError:
        reply(None, f"job upload {item['dir']} no longer exists..")
    assert r.db("twitch").table("uploads").get(item['id']).update({
        "PRE_MOVE_status": item['status'],
        "status": "FAILED",
        "completed": None,
        "reason": "Tracker died while completed was false"
    }).run(conn)['errors'] == 0

print("\n\n\n\n=======\nI'm in.\n=======")

STOP_FLAG: threading.Event = threading.Event()
DISCONNECT_CLIENTS: threading.Event = threading.Event()
CLIENTS_DISCONNECTED: threading.Event = threading.Event()
CLIENTS_DISCONNECTED.set()
PAUSE_UPLOADS: threading.Event = threading.Event()

bot = IrcBot(STREAM_URL, POST_URL)

@bot.command("!help")
def help(self, user, _ran, *args):
    nick = user['nick']
    text = ("List of commands:\n"
            "!status <IDENTIFIER>: Returns the status of a given job (e.g. !status 1319f607-38e6-4210-a3ed-4a540424a6fb). Does not currently work with URLs.\n"
            "!status: Returns the list of jobs in each queue.\n"
            "!a <URL> [EXPLANATION]: Archives the metadata of a twitch VOD or channel by its URL, saving the explanation into the database.\n"
            "Be sure to provide explanations for your jobs, and remember that everything queued here takes up space on IA.\n"
            "Please note that when a channel is queued here, only the metadata of the VODs will be saved, excluding clips and other channel content. To test what will be archived, use yt-dlp (relevant code: https://github.com/TheTechRobo/twitch-chat-getter/blob/4f11b65e394e2d2f94e7e8f6cb1ed451eeb99ca1/client.py#L138-L151 )\n"
            "Also, archiving in bulk with transfer.archivete.am URLs works. This also applies to !status.\n"
            "You can find the data on IA here: https://archive.org/details/archiveteam_twitch_metadata")
    for line in text.split("\n"):
        self.reply(nick, line.strip())

@bot.command("!status")
def status(self, user, _ran, job=None, callback=None):
    author = user['nick']
    if job:
        try:
            msg = generate_status_message(job)
            assert len(msg) == 1
        except AssertionError:
            self.reply(author, "An internal continuity error occured!")
        else:
            if callback:
                msg[0] = callback(msg[0])
            self.reply(author, msg[0])
        return
    data = get_status()
    msg = f"{data['todo']} jobs in todo, {data['claims']} jobs in claims."
    if callback:
        msg = callback(msg)
    self.reply(author, msg)

@bot.command("!sutats")
def sutats(self, user, _ran, job=None):
    return status(self, user, "!status", job, lambda a : a[::-1])

WATEROFFDEAD_PERMUTATIONS = set(['!' + ''.join(p) for p in permutations("status")])
WATEROFFDEAD_PERMUTATIONS.discard('status')
WATEROFFDEAD_PERMUTATIONS.discard('sutats')
@bot.command(WATEROFFDEAD_PERMUTATIONS)
def stdusiwyfw(self, user, ran, job=None):
    d = lambda a : "".join(random.sample(list(a), len(a)))
    return status(self, user, "!status", job, d)

@bot.command("!stoptasks")
def stoptasks(self, user, _ran):
    STOP_FLAG.set()
    self.reply(user['nick'], "STOP_FLAG has been set. No items will be served.")

@bot.command("!starttasks")
def starttasks(self, user, _ran):
    STOP_FLAG.clear()
    self.reply(user['nick'], "STOP_FLAG has been cleared. Items can now be served.")

@bot.command("!a")
def archive(_self, user, _ran, item, *explain):
    explain = " ".join(explain)
    start_pipeline_2w(item, user['nick'], explain)

@bot.command("!stopuploads")
def stopuploads(self, _user, _ran):
    PAUSE_UPLOADS.set()
    self.reply(_user['nick'], "Paused uploads")

@bot.command("!startuploads")
def startuploads(self, _user, _ran):
    PAUSE_UPLOADS.clear()
    self.reply(_user['nick'], "Resumed uploads")

try:
    bot.run_forever()
finally:
    reply("TheTechRobo", "The server is shutting down! Signaling for clients to disconnect.")
    STOP_FLAG.set()
    DISCONNECT_CLIENTS.set()
    server.deny_new_connections(status=1001, reason=b"Server is Going down")
    CLIENTS_DISCONNECTED.wait()
    try:
        server.shutdown_gracefully()
    except Exception as ename:
        print("Exception raised.", ename)
        server.shutdown_abruptly()
    reply("TheTechRobo", "Server is going down NOW!")
