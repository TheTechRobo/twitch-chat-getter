clients = {}

import socket, ssl, re, requests, os
from websocket_server import WebsocketServer
import json, time
from rethinkdb import r

MESSAGES_TO_SEND = []
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
def client_left(client, server):
    for (item, author) in clients[client['id']]['tasks'].values():
        MESSAGES_TO_SEND.append(f"PRIVMSG {CHAN} :{author}: Your item {item['id']} for {item['item']} failed. (Reason: Client disconnected)")
        client['reason'] = "disconnect"
        client['moved_at'] = time.time()
        r.db("twitch").table("error").insert(r.db("twitch").table("todo").get(item['id']).run(conn)).run(conn)
        e = r.db("twitch").table("todo").get(item['id']).delete(return_changes=True).run(conn)
        if e['errors']:
            MESSAGES_TO_SEND.append(f"PRIVMSG {CHAN} :{author}, TheTechRobo: Could not remove item from todo. Check logs.")
            raise ValueError(e)
    del clients[client['id']]
    print("Client(%d) disconnected" % client['id'])


# Called when a client sends a message
def message_received(client, server, message):
    msg = json.loads(message)
    if msg.get("auth") == SECRET:
        print("Secret-Auth")
        clients[client['id']]['auth'] = True
    if msg["type"] ==   "ping":
        msg["type"] =  "godot"
        msg["method"] = "ping"
        print("Sent keep-alive")
        server.send_message(client, json.dumps(msg))
        print(f"Client({client['id']}) sent a keep-alive")
    elif msg["type"] == "get":
        try:
            item = request_item(client)
            author, itemName = item['started_by'], item['item']
            if itemName and author:
                clients[client['id']]['tasks'][item['item']] = ((item, author))
            server.send_message(client, itemName)
            if itemName:
                print("Sent", itemName, "to client")
        except SyntaxError:
            client["handler"].send_close(1000, 'No Auth'.encode())
    elif msg["type"] == "done":
        if not client['auth']:
            client['handler'].send_close(1000, "No Auth".encode())
            return
        item = msg["item"]
        user = finish_item(item, client)
        MESSAGES_TO_SEND.append(f"PRIVMSG {CHAN} :{user}: Your job for {item} has finished.")
        del clients[client['id']]['tasks'][item]
    elif msg["type"] == "feed":
        item = msg["item"]
        reason = msg["reason"]
        if " " in item or not item or not reason:
            server.send_message(client, '{"type": "failure", "method": "backfeed", "reason": "space_in_item_name"}')
            print("Bad Backfeed", repr(item), repr(reason))
        MESSAGES_TO_SEND.append(f"PRIVMSG {CHAN} :!a {item} {reason}")
        start_pipeline_2w(item, NICK, reason)
    elif msg["type"] == "error":
        item = msg["item"]
        try:
            d = error_item(item, client, msg['reason'])
            if d:
                user, id = error_item(item, client, msg['reason'])
            else:
                user, id = "(?)", "(?)" # will hopefully be fixed in the future
            MESSAGES_TO_SEND.append(f"PRIVMSG {CHAN} :{user}: Your job {id} for {item} on client {client['id']} failed. ({msg['reason']})")
        except SyntaxError:
            client['handler'].send_close(1000, "No Auth".encode())
    print("Client(%d) said: %s" % (client['id'], message))

def int_or_none(string):
    try:
        return int(string)
    except ValueError:
        pass

PORT=int_or_none(os.getenv("WSPORT")) or 9001
server = WebsocketServer(port=PORT)
server.set_fn_new_client(new_client)
server.set_fn_client_left(client_left)
server.set_fn_message_received(message_received)
server.run_forever(True)

print("Server is up and running.")

context = ssl.create_default_context()
HOST = os.environ['SERVER']
PORT = int(os.environ['PORT']) #port
NICK = os.environ['NICK']
CHAN = os.environ['CHANNEL']
PASSWORD = os.environ['PASSWORD']

# Moving claimed items to error
conn = r.connect()
cursor = r.db("twitch").table("todo").get_all("claims", index="status")
for entry in cursor.run(conn):
    prettifiedItem = f"https://twitch.tv/{entry['item'][1:]}" if entry['item'].startswith('c') else f"https://twitch.tv/videos/{entry['item']}"
    MESSAGES_TO_SEND.append(f"PRIVMSG {CHAN} :{entry['started_by']}: Your job {entry['id']} for {prettifiedItem} failed. (Tracker died while item was claimed)")
    entry["moved_at"] = time.time()
    r.db("twitch").table("error").insert(entry).run(conn)
    r.db("twitch").table("todo").get(entry['id']).delete().run(conn)

def send_command(command, sock):
    sock.send((command + "\r\n").encode())

def request_item(client):
    conn = r.connect()
    if not client['auth']:
        raise SyntaxError("Bad-Auth")
    d = list(r.db("twitch").table("todo").get_all("todo", index="status").sample(1).run(conn))
    if len(d) == 0:
        print("No items found")
        return {"id": "", "item": "", "started_by": ""}
    r.db("twitch").table("todo").get(d[0]['id']).update(
            {"status": "claims", "claimed_at": time.time()}
    ).run(conn)
    print("Sending", d[0], "to client")
    return d[0]

def error_item(item, client, reason):
    if not client['auth']:
        raise SyntaxEror("Bad-Auth")
    conn = r.connect()
    data = list(r.db("twitch").table("todo").get_all(item, index="item").run(conn))
    try:
        assert len(data) == 1, (item, client, reason, data)
    except AssertionError:
        # the disconnect function has probably already removed this!
        MESSAGES_TO_SEND.append(f"PRIVMSG {CHAN} :WARNING: This item is no longer in todo! The disconnect function has probably already removed this. This may result in incomplete data being added to the database, so that data will be printed here:")
        url = requests.put("https://transfer.archivete.am/pebbles-errlog", json={"item": item, "client": (client['id'], client['address']), "reason": reason, "time":time.time()}).text
        MESSAGES_TO_SEND.append(f"PRIVMSG {CHAN} {url}")
        return
    data = data[0]
    data['moved_at'] = time.time()
    data['reason'] = reason
    r.db("twitch").table("error").insert(data).run(conn)
    r.db("twitch").table("todo").get(data['id']).delete().run(conn)
    return data['started_by'], data['id']

def finish_item(item, client):
    if not client['auth']:
        raise SyntaxError("Bad-Auth")
    conn = r.connect()
    r.db("twitch").table("todo").get_all(item, index="item").update(
        {"status": "done", "finished_at": time.time()}
    ).run(conn)
    print("Finished item", item)
    return list(r.db("twitch").table("todo").get_all(item, index="item").run(conn))[0]["started_by"]


def add_to_db_2(item, author, explain):
    conn = r.connect()
    if a := list(r.db("twitch").table("todo").get_all(item, index="item").run(conn)):
        raise Exception("Item has already been run, try !status " + a[0]['id'])
    id = r.db("twitch").table("todo").insert(
        {"item": item, "started_by": author, "status": "todo", "queued_at": time.time(), "explain": explain}
    ).run(conn)['generated_keys'][0]
    return id

def get_item_details(ident):
    conn = r.connect()
    if data := r.db("twitch").table("todo").get(ident) \
            .run(conn):
        return data
    if data1 := r.db("twitch").table("error").get(ident) \
            .run(conn):
        data1["status"] = "error"
        return data1

def start_pipeline_2(item, author, explain, use_sock=None):
    if re.search(r"^https?://transfer.archivete\.am/(?:inline/)?[^/]", item):
        ids = []
        try:
            fail = False
            for newitem in requests.get(item).text.strip().split("\n"):
                print("Queue", newitem)
                d = start_pipeline_2(newitem, author, explain)
                if d['status']:
                    ids.append(d['id'])
                else:
                    ids.append(f"Item {newitem} could not be queued: {d['msg']}.")
                    fail = True
            url = requests.put("https://transfer.archivete.am/pebbles-bulk-ids", data="\n".join(ids)).text
            if fail:
                send_command(f"PRIVMSG {CHAN} :{author}: At least one item could not be queued; check {url} for more details.", ssock)
            return {"status": True, "id": url}
        except Exception as ename:
            print(type(ename), repr(ename))
            raise
            return {"status": False, "msg": str(ename).split("\n")[0]}
    id = re.search(r"^https?://w?w?w?.?twitch.tv/videos/(\d+)", item)
    is_channel = False
    if not id:
        id = re.search(r"^https?://w?w?w?\.?twitch\.tv/([^/?&]+)", item)
        is_channel = True
        if not id:
            return {"status":False,"msg":"That doesn't look like a valid VOD URL"}
    id = id.group(1)
    if is_channel:
        id = f"c{id}"
    try:
        return {"status": True, "id": add_to_db_2(id, author, explain)}
    except Exception as ename:
        return {"status": False, "msg": str(ename).split("\n")[0]}

SEND_QUEUED = False

def start_pipeline_2w(*args, **kwargs):
    res = start_pipeline_2(*args, **kwargs)
    if res['status']:
        send_command(f"PRIVMSG {channel} :{author}: Queued {item} for chat archival. I will ping you when finished. Use !status {res['id']} for details.", ssock)
    else:
        n = "\n"
        send_command(f"PRIVMSG {channel} :{author}: Couldn't queue your job for {item}. ({res['msg'].split(n)[0]})", ssock)

def reply(channel, user, message, sock):
    send_command(f"PRIVMSG {channel} :{user}: {message}", sock)
    time.sleep(1)

def get_status() -> dict[str, str]:
    conn = r.connect()
    todo_count = r.db("twitch").table("todo").get_all("todo", index="status").count().run(conn)
    claims_count = r.db("twitch").table("todo").get_all("claims", index="status").count().run(conn)
    return {"todo": todo_count, "claims": claims_count}

with socket.create_connection((HOST, PORT)) as sock:
    with context.wrap_socket(sock, server_hostname=HOST) as ssock:
        send_command(f"NICK {NICK}", ssock)
        send_command(f"USER {NICK} {NICK} {NICK} IRC-Bot", ssock)
        send_command(f"JOIN {CHAN}", ssock)
        MESSAGES_TO_SEND.append(f"PRIVMSG NICKSERV :IDENTIFY {NICK} {PASSWORD}")
        for line in ssock.makefile():
            if SEND_QUEUED:
                for message in MESSAGES_TO_SEND:
                    send_command(message, ssock)
                    time.sleep(1)
                MESSAGES_TO_SEND = []
            print(line)
            if "JOIN" in line:
                send_command(f"WHOIS {CHAN}", ssock)
                SEND_QUEUED = True # do not send privmsgs until we're connected
            data = line.split(" ")
            command = data[0]
            if command == "PING":
                send_command(f"WHOIS {CHAN}", ssock)
                send_command(f"PONG {data[1]}", ssock)
                print("\t Pong!")
            if line.startswith(":"):
                if line.startswith(f":{NICK} MODE "):
                    send_command(f"JOIN {CHAN}", ssock)
            else:
                continue
            data = line.split(" ", 3)
            author = data[0].lstrip(':').split("!")[0]
            command = data[1]
            if command == "353":
                data = line.split(" ")
                channel = data[4]
                whois = data[5:]
                whois[0] = whois[0].lstrip(":")
                for user in whois:
                    mode = "normal"
                    if user.startswith("@"):
                        mode = "op"
                    if user.startswith("+"):
                        mode = "voice"
                    user = user.lstrip(":+@").strip()
                    WHOIS[user] = mode
            if command == "PRIVMSG":
                message = data[3].lstrip(":").strip()
                if message == "!status":
                    data = get_status()
                    reply(CHAN, author, f"{data['todo']} jobs in todo, {data['claims']} jobs in claims.", ssock)
                    continue
                if message.startswith("!help"):
                    reply(CHAN, author, "List of commands:", ssock)
                    reply(CHAN, author, "!status <IDENTIFIER>: Gets job status by job ID (e.g. !status 1319f607-38e6-4210-a3ed-4a540424a6fb)", ssock)
                    reply(CHAN, author, "!status: Gets list of jobs in each queue.", ssock)
                    reply(CHAN, author, "!a <URL> <EXPLANATION>: Archives a twitch vod by its URL, saving the explanation into the database.", ssock)
                    continue
                if not message.startswith("!a ") and not message.startswith("!status "):
                    continue

                if message.startswith("!status "):
                    id = message.split(" ")[1]
                    try:
                        data = get_item_details(id)
                        if not data:
                            send_command(f"PRIVMSG {channel} :{author}: That job doesn't appear to exist.", ssock)
                            raise Exception("Job doesn't appear to exist")
                    except Exception as ename:
                        n = '\n'
                        send_command(f"PRIVMSG {channel} :{author}: Failed to get job details for item {id}: {str(type(ename))} {str(ename).split(n)[0]}", ssock)
                        continue
                    send_command(f"PRIVMSG {channel} :{author}: Job {id} is in {data['status']}. It scraped VOD {data['item']}. This command will provide more details later.", ssock)
                    continue

                item = message.split(" ")[1]
                try:
                    explain = " ".join(message.split(" ")[2:])
                except IndexError:
                    explain = ""
                channel  = data[2]
                time.sleep(1)
                start_pipeline_2w(item, author, explain)
