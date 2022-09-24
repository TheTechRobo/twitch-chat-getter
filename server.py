clients = []

import socket, ssl, re
from websocket_server import WebsocketServer
import json, time

WHOIS = {}

# Called for every client connecting (after handshake)
def new_client(client, server):
    print("New client connected and was given id %d" % client['id'])
    server.send_message(client, """
            {"type":"godot", "method":"ping"}""")
    client["auth"] = False
    clients.append(client)


# Called for every client disconnecting
def client_left(client, server):
    for index, client in enumerate(clients):
        if client['id'] == client:
            del clients[index]
    print("Client(%d) disconnected" % client['id'])


# Called when a client sends a message
def message_received(client, server, message):
    msg = json.loads(message)
    if msg["type"] ==   "ping":
        msg["type"] =  "godot"
        msg["method"] = "ping"
        server.send_message(client, json.dumps(msg))
        print(f"Client({client['id']}) sent a keep-alive")
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

import time

def send_command(command, sock):
    sock.send((command + "\r\n").encode())

def start_pipeline(item):
    return {"status": int(time.time()) % 2, "msg": "Could not add item to the queue."}

with socket.create_connection((HOST, PORT)) as sock:
    with context.wrap_socket(sock, server_hostname=HOST) as ssock:
        send_command(f"NICK {NICK}", ssock)
        send_command(f"USER {NICK} {NICK} {NICK} {NICK}", ssock)
        send_command(f"JOIN {CHAN}", ssock)
        for line in ssock.makefile():
            print(line)
            data = line.split(" ")
            command = data[0]
            if command == "PING":
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
                if not message.startswith("!a "):
                    continue

                send_command(f"NAMES {channel}", ssock)
                time.sleep(2)

                item = message.split(" ")[1]
                if WHOIS.get(author) != "voice" and WHOIS.get(author) != "op":
                    send_command(f"PRIVMSG {channel} :{author}: Bad priveleges (need voice or higher auth)", ssock)
                    continue
                channel  = data[2]
                send_command(f"PRIVMSG {channel} :{author}: Queued {item} for archival. I will ping you when finished. Use !status {item} for details.", ssock)
                result = start_pipeline(item)
                if not result["status"]:
                    send_command(f"PRIVMSG {channel} :{author}: Your job for {item} failed. ({result['msg']})", ssock)
                else:
                    send_command(f"PRIVMSG {channel} :{author}: Your job for {item} has finished.", ssock)

