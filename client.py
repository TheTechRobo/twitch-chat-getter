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

import atexit, websocket, json, os, time, sys, subprocess, shutil, os, os.path
import hashlib, traceback, signal, collections, struct
import requests, yt_dlp, prevent_sigint

secret = os.environ['SECRET']
DATA_DIR = os.environ['DATA_DIR']
assert DATA_DIR.startswith("/")
assert os.getenv("CURL_CA_BUNDLE") == "", "Set CURL_CA_BUNDLE to an empty string"

def open_and_wait(args, ws):
    process = subprocess.Popen(args, shell=False, preexec_fn=os.setpgrp)
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
        assert status == 0, f"Bad exit code {status}"
        break
    atexit.unregister(kill_process)

class Task:
    def __init__(self, logger, ws):
        self.logger = logger
        self.name = self.__class__.__name__
        self.ws = ws

    def run(self, item, itemType, author, id, full, queued_for, ctx):
        raise NotImplementedError("Implement the `run' method")

class PrepareDirectories(Task):
    def prepare_directories(self, ctx):
        folder = os.path.join(DATA_DIR, f"{self.itemType}{self.item}{time.time()}.tmp")
        subprocess.run([
            "mkdir", "-p", folder
        ]).check_returncode()
        ctx['folder'] = folder

        os.chdir(folder)

    def run(self, item, itemType, author, id, full, queued_for, ctx):
        self.item, self.itemType = item, itemType
        self.prepare_directories(ctx)

class DownloadData(Task):
    warcprox = None

    def _start_warcprox(self):
        self.WARCPROX_PORT = "4553"
        print(f"Starting warcprox for Item {self.item}")
        self.warcprox = subprocess.Popen([
            "warcprox", "-zp", self.WARCPROX_PORT,
            "--crawl-log-dir", "."
        ], preexec_fn=os.setpgrp)
        time.sleep(4)
        file_hash = ""
        with open(__file__, "rb") as file:
            file_hash = hashlib.sha256(file.read()).hexdigest()
        assert requests.request("WARCPROX_WRITE_RECORD", f"http://localhost:{self.WARCPROX_PORT}/burnthetwitch_client_version", headers={"Content-Type": "text=plain;charset=utf-8", "WARC-Type": "resource"}, data="burnthetwitch client.py sha256:%s" % file_hash).status_code == 204
        self.ws.send(json.dumps({"type": "ping"}))

        return self.warcprox

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
        print("Downloading metadata")
        open_and_wait([
            "yt-dlp", "--ignore-config", "--skip-download",
            # Some VODs return a 403 on the m3u8, so format data
            # will be missing on those ones since we're stifling the errors
            # better than getting nothing, though
            "--ignore-no-formats-error",
            "--write-info-json", "--write-description", "--write-thumbnail",
            "--write-all-thumbnails", "--no-check-certificate",
            "--retries", "4",
            # yt-dlp chat extraction is currently broken, so let's not make it crash
            #"--embed-subs", "--all-subs",
            "--limit-rate", "150k", "-o", "infojson:%(id)s",
            "--proxy", "http://localhost:" + self.WARCPROX_PORT,
            "https://twitch.tv/videos/" + item
        ], ws)
        print("Pre-emptively touching file...")
        with open("chat.json", "w+") as file:
            file.write("[]") # workaround for chat_downloader only writing the file when there are messages
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
        proxy = "http://localhost:" + self.WARCPROX_PORT
        options = {
                "proxy": proxy,
                "nocheckcertificate": True
        }
        videos = set()
        with yt_dlp.YoutubeDL(options) as ydl:
            ie = ydl.get_info_extractor("TwitchVideos")
            q = collections.deque()
            q.append(ie.extract("https://twitch.tv/%s/videos" % self.item))
            while q:
                e = q.popleft()
                if e['_type'] == "playlist":
                    entries = list(e['entries'])
                    q.extend(entries)
                elif e['_type'] in ("url", "url_transparent"):
                    videos.add(e['url'])
                else:
                    raise ValueError(f"Bad data returned by yt-dlp: {e}")
        print(f"Discovered {len(videos)} items.")
        if not videos:
            with open("url-list", "x") as file:
                file.write("\n")
            print("WARNING: Submitting zero videos to backfeed.")
            self.ws.send(json.dumps({"type":"warn","msg":"Zero videos found. ctx=%s" % json.dumps(self.ctx),"item":self.id,"person":self.author}))
            return

        with open("url-list", "x") as file:
            file.write("\n".join(videos) + "\n")

        # we do this after getting the list so if an exception is raised
        # we aren't sending incomplete data
        class DummyResponse:
            status_code = 0
        req = DummyResponse()
        while req.status_code != 200:
            req = requests.put(f"https://transfer.archivete.am/{item}-items", data="\n".join(videos))
            print(f"Status code {req.status_code}")
        bulk_list = req.text
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
        print("Submitted videos to backfeed.")
        return videos

    def _run(self, item, itemType, author, id, full, queued_for, ctx):
        self.ctx = ctx
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
    def _move_vod(self, ctx):
        with open(os.path.join(ctx['folder'], f"v{self.item}.info.json")) as f:
            data = json.load(f)
            channel = data['uploader_id']
        os.chdir(DATA_DIR)
        subprocess.run([
            "mkdir", "-p", os.path.join(DATA_DIR, channel, self.item)
        ], check=True)
        os.rename(ctx['folder'], os.path.join(DATA_DIR, channel, self.item, str(time.time())))

    def _move_channel(self, ctx):
        channel = self.item
        os.chdir(DATA_DIR)
        subprocess.run([
            "mkdir", "-p", os.path.join(DATA_DIR, channel)
        ], check=True)
        os.rename(ctx['folder'], os.path.join(DATA_DIR, channel, str(time.time())))

    def run(self, item, itemType, author, id, full, queued_for, ctx):
        self.item = item
        self.itemType = itemType

        if itemType == 'c':
            self._move_channel(ctx)
        elif itemType == 'v':
            self._move_vod(ctx)
        else:
            raise ValueError("unsupported item type")

class Pipeline:
    tasks: list[Task] = []

    def __init__(self, ws, *args):
        for task in args:
            self.tasks.append(task(print, ws))
        print(self.tasks)

    def _start(self, item, ws, author, ident, full, queuedFor):
        ws.send(json.dumps({"type": "ping"}))
        fullItem = item
        ctx = {}
        try:
            itemType = 'v'
            if item.startswith('c'):
                itemType = 'c'
                item = item[1:]
            for task in self.tasks:
                cls = task.__class__
                print(f"Starting {cls.__name__} for item {itemType}{item}")
                task.run(item, itemType, author, ident, full, queuedFor, ctx)
                ws.send(json.dumps({"type": "ping"}))
                print(f"Finished {cls.__name__} for item {itemType}:{item}")
        except Exception:
            print("Caught exception!")
            print("Sending to server and aborting.")
            data = "".join(traceback.format_exception(*sys.exc_info()))
            class Dummy:
                status_code = 0
            resp = Dummy()
            while resp.status_code != 200:
                resp = requests.put("https://transfer.archivete.am/traceback", data=f"{data}\n{os.getcwd()}\nctx={json.dumps(ctx)}")
            url = resp.text.replace(".am/", ".am/inline/")

            ws.send(json.dumps({
                "type": "error",
                "item": fullItem,
                "reason": f"Caught exception: {url}",
                "id": ident,
                "author": author
            }))
            ws.close()
            print("Socket closed.")
            raise
        print("Sending finish")
        ws.send(json.dumps({"type": "done", "item": fullItem, "id": ident, "itemFor": queuedFor}))
        print("Sent finish to the server.")

    def start(self, *args, **kwargs):
        """
        Wrapper to _start that defers SIGINT.
        """
        with prevent_sigint.signal_fence(signal.SIGINT, on_deferred_signal=self._stuff):
            return self._start(*args, **kwargs)

    @staticmethod
    def _stuff(*args, **kwargs):
        print("Stopping when current tasks are finished...")

doNotRequestItem = False

def updateWS(ws):
    global doNotRequestItem # pylint: disable=global-statement
    if not doNotRequestItem:
        print("Requesting item")
        ws.send(json.dumps({"type": "get"}))
    opcode, data = ws.recv_data()
    print(data)
    if opcode == 1:
        # Continue
        pass
    elif opcode == 8:
        # unpack status code as network-endian (big endian) unsigned short
        code = struct.unpack("!H", data[:2])[0]
        print("Server responded with CLOSE frame.\n"
                "Status Code: %d\tReason: %s" % (code, data[2:].decode()))
        print("Connection closed by remote host.")
        sys.exit(0)
    else:
        raise ValueError("Unsupported opcode sent from server: %d." % opcode)
    item = data
    _ = json.loads(item)
    if type(_) == dict:
        if _['type'] != "godot" and _['type'] != "item":
            print("Skip", _)
        if _['type'] != "item":
            doNotRequestItem = True # we already did - this is not the item
            return
        if _['type'] == "item":
            item = _['item']
            if not item:
                print("No items received. Trying again in 15 seconds.")
                time.sleep(15)
                doNotRequestItem = False
                return
            author = _['started_by']
            id = _['id']
            queuedFor = _.get("queued_for_item")
            doNotRequestItem = False
    else:
        print()
        print("Item:", item)
        raise ValueError("Bad server version?")
    print(f"Got item {item} for author {author}")
    pipeline.start(item, ws, author, id, data.decode("utf-8"), queuedFor)

pipeline = None

def mainloop():
    global pipeline # pylint: disable=global-statement

    # init
    ws = websocket.WebSocket()
    ws.connect(os.environ["CONNECT"])
    ws.send(json.dumps({"type": "auth", "method": "secret", "auth": secret}))
    ws.send(json.dumps({"type": "ping"}))
    assert json.loads(ws.recv())["type"] == "godot", "Incorrect server!"
    pipeline = Pipeline(
        ws,
        PrepareDirectories,
        DownloadData,
        MoveFiles
    ) # later we need to do things like put warcprox in its own task
    # tini
    while True:
        updateWS(ws)

mainloop()
