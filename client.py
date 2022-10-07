
import websocket, json, os, time, sys, subprocess, shutil, os, os.path
import requests
secret = os.getenv("SECRET")

assert os.getenv("DATA_DIR") and os.getenv("DATA_DIR").startswith("/")
assert secret
assert os.getenv("CURL_CA_BUNDLE") == ""

ws = websocket.WebSocket()
ws.connect(os.environ["CONNECT"])
ws.send(json.dumps({"type": "auth", "method": "secret", "auth": secret}))
ws.send(json.dumps({"type": "ping"}))
assert json.loads(ws.recv())["type"] == "godot", "Incorrect server!"
ws.recv()

def open_and_wait(args, ws):
    process = subprocess.Popen(args, shell=False)
    while True:
        status = process.poll()
        if status is None:
            # Process hasn't finished yet
            ws.send('{"type": "ping"}')
            time.sleep(1)
            continue
        # Process has finished
        assert status == 0, "Bad status code %s" % status
        break


while True:
    ws.send(json.dumps({"type": "get"}))
    item = ws.recv()
    try:
        _ = json.loads(item)
        if type(_) == dict and _.get("type") == "godot":
            continue
    except json.JSONDecodeError:
        pass
    if not item:
        print("No items received. Trying again in 15 seconds.")
        time.sleep(15)
        continue
    print(f"Got item {item}")
    try:
        ws.send('{"type": "ping"}')
        print("Preparing directories for Item")
        subprocess.run(["mkdir", "-p", os.path.join(os.environ["DATA_DIR"], item)]).check_returncode()
        ws.send('{"type": "ping"}')
        os.chdir(os.path.join(os.environ["DATA_DIR"], item))
        ws.send('{"type": "ping"}')
        print("Starting warcprox for Item")
        warcprox = subprocess.Popen(
            [
                "warcprox", "-zp", "4551",
                "--crawl-log-dir", "."
            ]
        )
        ws.send('{"type": "ping"}')
        time.sleep(5)
        assert requests.get("http://localhost:4551").status_code == 500
        ws.send('{"type": "ping"}')
        print("Downloading metadata")
        open_and_wait([
            "yt-dlp", "--ignore-config", "--skip-download",
            "--write-info-json", "--write-description", "--write-thumbnail",
            "--write-all-thumbnails", "--no-check-certificate",
            "--retries", "4", "--embed-subs", "--all-subs",
            "--limit-rate", "150k",
            "--proxy", "http://localhost:4551",
            "https://twitch.tv/videos/" + item
        ], ws)
        print("Downloading chat")
        open_and_wait([
            shutil.which("chat_downloader"),
            "--message_groups", 'messages bans deleted_messages hosts room_states user_states notices chants other bits subscriptions upgrades raids rituals mods colours commercials vips charity', "-o", "chat.json",
            "--proxy", "http://localhost:4551",
            "https://twitch.tv/videos/" + item
        ], ws)
        ws.send('{"type": "ping"}')
        print("Terminating warcprox")
        subprocess.run([
            shutil.which("kill"), "-INT", str(warcprox.pid)]
        ).check_returncode()
        ws.send('{"type": "ping"}')
        warcprox.wait()
        ws.send('{"type": "ping"}')
        print("Sending finish")
        ws.send(json.dumps({"type": "done", "item": item}))
    except Exception as ename:
        print(type(ename), ename)
        try:
            warcprox.pid
        except Exception:
            pass
        else:
            print("Terminating warcprox")
            subprocess.run([
                shutil.which("kill"), "-INT", str(warcprox.pid)]
            ).check_returncode()
        finally:
            try:
                process.pid
            except Exception:
                pass
            else:
                print("Terminating scraper")
                subprocess.run([
                    shutil.which("kill"), "-INT", str(process.pid)]
                ).check_returncode()
            ws.close()
        sys.exit(8)
