clients = {}

import json
import os
import re
import time

import arrow
import requests

from websocket_server import WebsocketServer
from rethinkdb import r

WHOIS = {}
SECRET = os.getenv("SECRET")

# Called for every client connecting (after handshake)
def new_client(client, server):
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
        r.db("twitch").table("error").insert(r.db("twitch").table("todo").get(item['id']).run(conn)).run(conn)
        e = r.db("twitch").table("todo").get(item['id']).delete(return_changes=True).run(conn)
        if e['errors']:
            reply(f"{author}, TheTechRobo", "Could not remove item from todo. Check logs.")
            raise ValueError(e)
    del clients[client['id']]
    print(f"Client({client['id']}) disconnected")


# Called when a client sends a message
def message_received(client, server, message):
    try:
        msg = json.loads(message)
    except Exception:
        return "Fail"
    if msg.get("auth") == SECRET:
        print("Secret-Auth")
        clients[client['id']]['auth'] = True
    if msg["type"] ==   "ping":
        msg["type"] =  "godot"
        msg["method"] = "ping"
        server.send_message(client, json.dumps(msg))
        print(f"Client({client['id']}) sent a keep-alive")
        return
    elif msg["type"] == "get":
        try:
            item = request_item(client)
            author, itemName = item['started_by'], item['item']
            if itemName and author:
                clients[client['id']]['tasks'][item['item']] = ((item, author))
            server.send_message(client, json.dumps(item))
            if itemName:
                print("Sent", itemName, "to client")
        except SyntaxError:
            client["handler"].send_close(1000, 'No Auth'.encode())
    elif msg["type"] == "done":
        if not client['auth']:
            client['handler'].send_close(1000, "No Auth".encode())
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
        # The following line has a zero-width space; do not remove it.
        # That way if we ever have IRCv3 message ack enabled,
        # this won't accidentally run the pipeline twice (which'll just
        # add spam to the channel).
        reply(".", f"​!a {item} {reason}")
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
            client['handler'].send_close(1000, "No Auth".encode())
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
server = WebsocketServer(port=PORT)
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
            tstext = f"Job {tense} {ts}." if ts else ""
            item_type = "VOD"
            if result['item'].startswith('c'):
                item_type = "channel"
                item = item[1:]
            children = ""
            if result.get("hasChildren") == "yes:alldone":
                # it doesn't work now, so let's just return less
                # information just to be safe (in case otherwise
                # it'd say this when it shouldn't)
                result['hasChildren'] = "yes"
            if result.get("hasChildren") == "yes":
                children = " Has at least one child process. "
            elif result.get("hasChildren") == "yes:alldone":
                children = " Has at least one child process (all done). "
            elif result.get("hasChildren") == "yes:error":
                children = " Has at least one child process, at least one of which has failed. "
            messages.append(f"Job {result['id']} is in {result['status']}. It scraped {item_type} {item}. {tstext}{children} This command will provide more details later.")
    if len(messages) > 1:
        url = requests.put("https://transfer.archivete.am/pebbles-job-status",
            data="\n\n".join(messages)).text.replace(".am/", ".am/inline/")
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
                print("Queue", newitem)
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
        id = re.search(r"^https?://w?w?w?\.?twitch\.tv/([^/?&]+)", item)
        is_channel = True
        expires = int(time.time()) + 48 * 3600 # expires in 48 hours
        if not id:
            return {"status":False,"msg":"That doesn't look like a valid VOD URL"}
    id = id.group(1)
    if is_channel:
        id = f"c{id}"
    try:
        return {"status": True, "id": add_to_db_2(id, author, explain, expires=expires, item_for=item_for)}
    except Exception as ename:
        n = "\n"
        return {"status": False, "msg": f"{str(type(ename))} {str(ename).split(n)[0]}"}

def start_pipeline_2w(item, author, explain, item_for=None):
    res = start_pipeline_2(item, author, explain, item_for=item_for)
    if res['status']:
        reply(author, f"Queued {item} for chat archival. I will ping you when finished. Use !status {res['id']} for details.")
    else:
        n = "\n"
        reply(author, f"Couldn't queue your job for {item}. ({res['msg'].split(n)[0]})")

def reply(user, message):
    assert requests.post(POST_URL, data=f"{user}: {message}").status_code == 200
    time.sleep(1)

def get_status() -> dict[str, str]:
    conn = r.connect()
    todo_count = r.db("twitch").table("todo").get_all("todo", index="status").count().run(conn)
    claims_count = r.db("twitch").table("todo").get_all("claims", index="status").count().run(conn)
    return {"todo": todo_count, "claims": claims_count}

def parse_irc_line(line: dict):
    author = line.get("user")["nick"]
    command = line["command"]
    if command == "PRIVMSG":
        message = line["message"]
        if message == "!status":
            data = get_status()
            reply(author, f"{data['todo']} jobs in todo, {data['claims']} jobs in claims.")
            return
        if message.startswith("!help"):
            reply(author, "List of commands:")
            reply(author, "!status <IDENTIFIER>: Gets job status by job ID (e.g. !status 1319f607-38e6-4210-a3ed-4a540424a6fb)")
            reply(author, "!status: Gets list of jobs in each queue.")
            reply(author, "!a <URL> <EXPLANATION>: Archives a twitch vod or channel by its URL, saving the explanation into the database.")
            reply(author, "Be sure to provide explanations for your jobs, and try not to overload my servers!")
            reply(author, "Please note that when you archive a channel, you are only archiving the VODs - not the clips or anything like that.")
            reply(author, "Also, archiving in bulk with transfer.archivete.am URLs works.Again, though, PLEASE do not overload my servers!")
            return
        if not message.startswith("!a ") and not message.startswith("!status "):
            return
        if message.startswith("!status "):
            id = message.split(" ")[1]
            try:
                msg = generate_status_message(id)
                assert len(msg) == 1
            except AssertionError:
                reply(author, "An internal continuity error occured!")
            else:
                reply(author, msg[0])
            return
        item = message.split(" ")[1]
        try:
            explain = " ".join(message.split(" ")[2:])
        except IndexError:
            explain = ""
        channel = line["channel"]
        time.sleep(1)
        start_pipeline_2w(item, author, explain)

print("Moving items around...")

# Moving claimed items to error
conn = r.connect()
cursor = r.db("twitch").table("todo").get_all("claims", index="status")
for entry in cursor.run(conn):
    prettifiedItem = f"https://twitch.tv/{entry['item'][1:]}" if entry['item'].startswith('c') else f"https://twitch.tv/videos/{entry['item']}"
    reply(entry["started_by"], f"Your job {entry['id']} for {prettifiedItem} failed. (Tracker died while item was claimed)")
    entry["moved_at"] = time.time()
    r.db("twitch").table("error").insert(entry).run(conn)
    r.db("twitch").table("todo").get(entry['id']).delete().run(conn)

print("\n\n\n\n=======\nI'm in.\n=======")

for linee in stream.iter_lines():
    print(linee)
    parse_irc_line(json.loads(linee.decode("utf-8")))
