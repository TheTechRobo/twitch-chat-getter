import atexit, websocket, json, os, time, sys, subprocess, shutil, os, os.path
import requests

from typing import NoReturn as Neverl

# TODO: Cleanup this section
secret = os.getenv("SECRET")
DATA_DIR = os.environ['DATA_DIR']
GQL_HEADER = os.environ['GQL_HEADER'] # go to twitch on a browser, open the network tools, find the Client-Id header in a GQL request, then profit :-)
assert DATA_DIR.startswith("/")
assert secret
assert os.getenv("CURL_CA_BUNDLE") == ""

ws = websocket.WebSocket()
ws.connect(os.environ["CONNECT"])
ws.send(json.dumps({"type": "auth", "method": "secret", "auth": secret}))
ws.send(json.dumps({"type": "ping"}))
assert json.loads(ws.recv())["type"] == "godot", "Incorrect server!"

def open_and_wait(args, ws):
    process = subprocess.Popen(args, shell=False)
    @atexit.register
    def kill_process():
        process.terminate()
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
    atexit.unregister(kill_process)

class Task:
    def __init__(self, logger, ws):
        self.logger = logger
        self.name = self.__class__.__name__
        self.ws = ws

    def run(self, item, itemType, author, id, queued_for):
        raise NotImplementedError("Implement the `run' method")

class TaskWithWebsocket(Task):
    pass

class PrepareDirectories(Task):
    def prepare_directories(self):
        folder = os.path.join(DATA_DIR, f"{self.itemType}{self.item}.tmp")
        subprocess.run([
            "mkdir", "-p", folder
        ]).check_returncode()

        os.chdir(folder)

    def run(self, item, itemType, author, id, full, queued_for):
        self.item, self.itemType = item, itemType
        self.prepare_directories()

class DownloadData(Task):
    warcprox = None

    def _start_warcprox(self):
        self.WARCPROX_PORT = "4553"
        print(f"Starting warcprox for Item {self.item}")
        warcprox = subprocess.Popen([
            "warcprox", "-zp", self.WARCPROX_PORT,
            "--crawl-log-dir", "."
        ])
        time.sleep(5)
        assert requests.get("http://localhost:" + self.WARCPROX_PORT).status_code == 500 # Warcprox will respond to / with a 500
        print(ws)
        self.ws.send(json.dumps({"type": "ping"}))

        return warcprox

    def _kill_warcprox(self, warcprox, signal="9"):
        print("Terminating warcprox")
        if not warcprox:
            print("Nothing to kill")
            return
        subprocess.run([
            shutil.which("kill"), f"-{signal}", str(warcprox.pid)]
        ).check_returncode()
        try:
            self.ws.send('{"type": "ping"}')
        except Exception:
            pass
        warcprox.wait()

    def _run_vod(self, item):
        ws = self.ws

        self.warcprox = self._start_warcprox()
        warcprox = self.warcprox
        print("Downloading metadata")
        open_and_wait([
            "yt-dlp", "--ignore-config", "--skip-download",
            "--write-info-json", "--write-description", "--write-thumbnail",
            "--write-all-thumbnails", "--no-check-certificate",
            "--retries", "4", "--embed-subs", "--all-subs",
            "--limit-rate", "150k", "-o", "infojson:%(id)s",
            "--proxy", "http://localhost:" + self.WARCPROX_PORT,
            "https://twitch.tv/videos/" + item
        ], ws)
        print("Downloading chat")
        open_and_wait([
            shutil.which("chat_downloader"),
            "--message_groups", 'messages bans deleted_messages hosts room_states user_states notices chants other bits subscriptions upgrades raids rituals mods colours commercials vips charity', "-o", "chat.json",
            "--proxy", "http://localhost:" + self.WARCPROX_PORT,
            "https://twitch.tv/videos/" + item
        ], ws)
        ws.send('{"type": "ping"}')

    def _run_channel(self, item):
        self.warcprox = self._start_warcprox()
        proxies = {
                "http": "http://localhost:" + self.WARCPROX_PORT,
                "https": "http://localhost:" + self.WARCPROX_PORT
        }
        # https://stackoverflow.com/a/58054717/9654083
        # Only retrieves the first 100 videos! Feel free to
        # send a PR to add pagination. There needs to be a configurable
        # limit though.
        post_data = json.loads('[{"operationName":"FilterableVideoTower_Videos","variables":{"limit":100,"channelOwnerLogin":"%s","broadcastType":null,"videoSort":"TIME","cursor":"MTQ1"},"extensions":{"persistedQuery":{"version":1,"sha256Hash":"2023a089fca2860c46dcdeb37b2ab2b60899b52cca1bfa4e720b260216ec2dc6"}}}]' % item)
        headers = {"Client-Id": GQL_HEADER}
        videos = []
        data = requests.post("https://gql.twitch.tv/gql", json=post_data, headers=headers, proxies=proxies).json()[0]['data']
        for video in data['user']['videos']['edges']:
            videos.append(f"https://twitch.tv/videos/{video['node']['id']}")

        # we do this after getting the list so if an exception is raised
        # we aren't sending incomplete data
        bulk_list = requests.put(f"https://transfer.archivete.am/{item}-items", data="\n".join(videos)).text
        amsg = f"(for {self.author})" if self.author else ""
        a = {
            "type": "feed",
            "item": bulk_list,
            "person": self.author,
            "reason": f"Automatically queued for channel {item} {amsg}",
            "item_for": self.id
        }
        print(a)
        print(self.id)
        print("Done Dump...")
        self.ws.send(json.dumps(a))
        return videos

    def _run(self, item, itemType, author, id, full, queued_for):
        self.full = full
        self.author = author
        self.id = id
        self.queued_for = queued_for
        self.item = item
        self.itemType = itemType
        with open("jobdata.json", "w+") as file:
            file.write(json.dumps(self.full))
        if itemType == 'v':
            self._run_vod(item)
        elif itemType == 'c':
            self._run_channel(item)
        else:
            raise ValueError("Unknown itemType!")

    def run(self, *args, **kwargs):
        try:
            self._run(*args, **kwargs)
        finally:
            try:
                self._kill_warcprox(self.warcprox)
            except Exception:
                print("Couldnt kill warcprox lol")

class MoveFiles(Task):
    def _move_vod(self):
        with open(os.path.join(DATA_DIR, self.itemType + self.item + ".tmp", f"v{item}.info.json")) as file:
            data = json.load(file)
            channel = data['uploader_id']
        os.chdir(DATA_DIR)
        subprocess.run([
            "mkdir", "-p", os.path.join(DATA_DIR, channel, self.item)
        ])
        os.rename(os.path.join(DATA_DIR, self.itemType + item + ".tmp"), os.path.join(DATA_DIR, channel, self.item, str(time.time())))

    def _move_channel(self):
        channel = self.item
        os.chdir(DATA_DIR)
        subprocess.run([
            "mkdir", "-p", os.path.join(DATA_DIR, channel)
        ])
        os.rename(os.path.join(DATA_DIR, f"{self.itemType}{self.item}.tmp"), os.path.join(DATA_DIR, channel, str(time.time())))

    def run(self, item, itemType, author, id, full, queued_for):
        self.item = item
        self.itemType = itemType

        if itemType == 'c':
            self._move_channel()
        elif itemType == 'v':
            self._move_vod()
        else:
            raise ValueError("unsupported item type")
class Pipeline:
    tasks: list[Task] = []

    def __init__(self, ws, *args):
        for task in args:
            self.tasks.append(task(print, ws))
        print(self.tasks)

    def start(self, item, ws, author, ident, full, queuedFor):
        ws.send(json.dumps({"type": "ping"}))
        fullItem = item
        try:
            itemType = 'v'
            if item.startswith('c'):
                itemType = 'c'
                item = item[1:]
            for task in self.tasks:
                cls = task.__class__
                print(f"Starting {cls.__name__} for item {itemType}{item}")
                task.run(item, itemType, author, ident, full, queuedFor)
                ws.send(json.dumps({"type": "ping"}))
                print(f"Finished {cls.__name__} for item {itemType}{item}")
        except Exception:
            print("Caught exception!")
            print("Sending to server and aborting.")
            data = sys.exc_info()
            type = data[0].__class__.__name__
            value = repr(data[1])
            line = data[2].tb_lineno

            ws.send(json.dumps({
                "type": "error",
                "item": fullItem,
                "reason": f"Caught exception: {type} {value} on {line}",
                "id": ident,
                "author": author
            }))
            ws.close()
            raise
        print("Sending finish")
        ws.send(json.dumps({"type": "done", "item": fullItem, "id": ident, "itemFor": queuedFor}))


doNotRequestItem = False

pipeline = Pipeline(
        ws,
        PrepareDirectories,
        DownloadData,
        MoveFiles
) # later we need to do things like put warcprox in its own

while True:
    if not doNotRequestItem:
        print("Requesting item")
        ws.send(json.dumps({"type": "get"}))
    full = ws.recv()
    item = full
    print(item)
    _ = json.loads(item)
    if type(_) == dict:
        if _['type'] != "godot" and _['type'] != "item":
            print("Skip", _)
        if _['type'] != "item":
            doNotRequestItem = True # we already did - this is not the item
            continue
        if _['type'] == "item":

            item = _['item']
            if not item:
                print("No items received. Trying again in 15 seconds.")
                time.sleep(15)
                continue
            author = _['started_by']
            id = _['id']
            queuedFor = _.get("queued_for_item")
            doNotRequestItem = False
    else:
        print()
        print("Item:", item)
        raise ValueError("Bad server version?")
    print(f"Got item {item} for author {author}")
    pipeline.start(item, ws, author, id, full, queuedFor)
