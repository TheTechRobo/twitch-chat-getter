clients = {}

import socket, ssl, re, os
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
        except SyntaxError:
            client["handler"].send_close(1000, 'No Auth'.encode())
    elif msg["type"] == "done":
        item = msg["item"]
        user = finish_item(item, client)
        MESSAGES_TO_SEND.append(f"PRIVMSG {CHAN} :{user}: Your job for {item} has finished.")
        del clients[client['id']]['tasks'][item]
    print("Client(%d) said: %s" % (client['id'], message))


PORT=9001
server = WebsocketServer(port = PORT)
server.set_fn_new_client(new_client)
server.set_fn_client_left(client_left)
server.set_fn_message_received(message_received)
server.run_forever(True)

print("Server is up and running.")

context = ssl.create_default_context()
HOST = 'irc.hackint.org' #irc server
PORT = 6697 #port
NICK = 'Pebbles'
CHAN = '#twitchchat'


# Moving claimed items to error
conn = r.connect()
cursor = r.db("twitch").table("todo").get_all("claims", index="status")
for entry in cursor.run(conn):
    MESSAGES_TO_SEND.append(f"PRIVMSG {CHAN} :{entry['started_by']}: Your job {entry['id']} for {entry['item']} failed. (Tracker died while item was claimed)")
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
        return {"id": "", "item": "", "started_by": ""}
    r.db("twitch").table("todo").get(d[0]['id']).update(
            {"status": "claims", "claimed_at": time.time()}
    ).run(conn)
    return d[0]
def finish_item(item, client):
    conn = r.connect()
    if not client['auth']:
        raise SyntaxError("Bad-Auth")
    r.db("twitch").table("todo").get_all(item, index="item").update(
        {"status": "done", "finished_at": time.time()}
    ).run(conn)
    return list(r.db("twitch").table("todo").get_all(item, index="item").run(conn))[0]["started_by"]


def add_to_db_2(item, author):
    conn = r.connect()
    if a := list(r.db("twitch").table("todo").get_all(item, index="item").run(conn)):
        raise Exception("Item has already been run, try !status " + a[0]['id'])
    id = r.db("twitch").table("todo").insert(
        {"item": item, "started_by": author, "status": "todo", "queued_at": time.time()}
    ).run(conn)['generated_keys'][0]
    return id

def get_item_details(ident):
    conn = r.connect()
    if data := r.db("twitch").table("todo").get(ident) \
            .run(conn):
        return data
    if data1 := r.db("twitch").table("error").get(ident) \
            .run(conn):
        data1[0]["status"] = "error"
        return data1

def start_pipeline(item):
    print("'", item, "'", sep="")
    if not re.search(r"^\d{9}\d?$", item):
        return {"status":False,"msg": "That doesn't look like a valid VOD ID"}
    return {"status": True}

def start_pipeline_2(item, author):
    id = re.search(r"^https?://w?w?w?.?twitch.tv/videos/(\d+)", item)
    if not id:
        return {"status":False,"msg":"That doesn't look like a valid VOD URL"}
    id = id.group(1)
    try:
        return {"status": True, "id": add_to_db_2(id, author)}
    except Exception as ename:
        return {"status": False, "msg": str(ename).split("\n")[0]}

SEND_QUEUED = False

with socket.create_connection((HOST, PORT)) as sock:
    with context.wrap_socket(sock, server_hostname=HOST) as ssock:
        send_command(f"NICK {NICK}", ssock)
        send_command(f"USER {NICK} {NICK} {NICK} {NICK}", ssock)
        send_command(f"JOIN {CHAN}", ssock)
        for line in ssock.makefile():
            if SEND_QUEUED:
                for message in MESSAGES_TO_SEND:
                    send_command(message, ssock)
                    time.sleep(0.9)
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
                if not message.startswith("!a ") and not message.startswith("!status "):
                    continue

                if message.startswith("!status "):
                    id = message.split(" ")[1]
                    data = get_item_details(id)
                    if not data:
                        send_command(f"PRIVMSG {channel} :{author}:That job doesn't appear to exist.", ssock)
                        continue
                    send_command(f"PRIVMSG {channel} :{author}: Job {id} is in {data['status']}. It scraped VOD {data['item']}. This command will provide more details later.", ssock)
                    continue

                send_command(f"NAMES {channel}", ssock)
                time.sleep(2)

                item = message.split(" ")[1]
                #if WHOIS.get(author) != "voice" and WHOIS.get(author) != "op":
                #    send_command(f"PRIVMSG {channel} :{author}: Bad priveleges (need voice or higher auth)", ssock)
                #    continue
                channel  = data[2]
                res = start_pipeline_2(item, author)
                if res['status']:
                    send_command(f"PRIVMSG {channel} :{author}: Queued {item} for chat archival. I will ping you when finished. Use !status {res['id']} for details.", ssock)
                else:
                    n = "\n"
                    send_command(f"PRIVMSG {channel} :{author}: Your job for {item} failed. ({res['msg'].split(n)[0]})", ssock)

