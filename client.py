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
import hashlib, traceback, signal, collections, struct, hashlib, base64
import requests, yt_dlp, prevent_sigint

from subprocess import PIPE

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
            "-c", "./file.pem",
            "--crawl-log-dir", "."
        ], preexec_fn=os.setpgrp)
        time.sleep(4)
        file_hash = ""
        with open(__file__, "rb") as file:
            file_hash = hashlib.sha256(file.read()).hexdigest()
        assert requests.request("WARCPROX_WRITE_RECORD", f"http://localhost:{self.WARCPROX_PORT}/burnthetwitch_client_version", headers={"Content-Type": "text=plain;charset=utf-8", "WARC-Type": "resource"}, data="burnthetwitch client.py sha256:%s" % file_hash).status_code == 204
        self.ws.send(json.dumps({"type": "ping"}))

        return self.warcprox

    def _kill_warcprox(self, warcprox, signal="INT"):
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
            # will be missing on those ones since we're stifling the errors.
            # Better than getting nothing, though
            "--ignore-no-formats-error",
            "--write-info-json", "--write-description", "--write-thumbnail",
            "--write-all-thumbnails", "--no-check-certificate",
            "--retries", "4",
            # yt-dlp chat extraction is currently broken, so let's not make it crash
            #"--embed-subs", "--all-subs",
            "--limit-rate", "300k", "-o", "infojson:%(id)s",
            "--proxy", "http://localhost:" + self.WARCPROX_PORT,
            "https://twitch.tv/videos/" + item
        ], ws)
        print("Pre-emptively touching file...")
        with open("chat.json", "w+") as file:
            file.write("[]") # workaround for chat_downloader only writing the file when there are messages
        print("Downloading chat")
        os.environ['CURL_CA_BUNDLE'] = "./file.pem"
        open_and_wait([
            shutil.which("chat_downloader"),
            "--message_groups", 'messages bans deleted_messages hosts room_states user_states notices chants other bits subscriptions upgrades raids rituals mods colours commercials vips charity', "-o", "chat.json",
            "--interruptible_retry", "False",
            "--proxy", "http://localhost:" + self.WARCPROX_PORT,
            "https://twitch.tv/videos/" + item
        ], ws)
        del os.environ['CURL_CA_BUNDLE']
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
            self.ws.send(json.dumps({"type":"warn","msg":"Zero videos found.","item":self.id,"person":self.author}))
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
        newrpath = os.path.join(channel, self.item, str(time.time()))
        newpath = os.path.join(DATA_DIR, newrpath)
        os.rename(ctx['folder'], newpath)
        ctx['final_relative_path'] = newrpath
        ctx['final_path'] = newpath
        ctx['channel'] = channel

    def _move_channel(self, ctx):
        channel = self.item
        os.chdir(DATA_DIR)
        subprocess.run([
            "mkdir", "-p", os.path.join(DATA_DIR, channel)
        ], check=True)
        new_relative_path = os.path.join(channel, str(time.time()))
        new_path = os.path.join(DATA_DIR, new_relative_path)
        os.rename(ctx['folder'], new_path)
        ctx['final_relative_path'] = new_relative_path
        ctx['final_path'] = new_path
        ctx['channel'] = channel

    def run(self, item, itemType, author, id, full, queued_for, ctx):
        self.item = item
        self.itemType = itemType

        if itemType == 'c':
            self._move_channel(ctx)
        elif itemType == 'v':
            self._move_vod(ctx)
        else:
            raise ValueError("unsupported item type")

def get_next_message(webSocket, wanted_type=None):
    data = {"type": "godot"}
    while data['type'] == "godot":
        opcode, data = webSocket.recv_data()
        if opcode == 1:
            data = json.loads(data)
        elif opcode == 8:
            print("Server unexpectedly closed the conection.\nResponse: %s" % data)
            sys.exit(4)
        else:
            print("Unknown opcode.\nData:" % [opcode, data])
            sys.exit(5)
    if wanted_type:
        assert data['type'] == wanted_type
    return data


# TODO: Move this into another file
# Source: https://stackoverflow.com/a/55648984/9654083
def du(path):
    if os.path.islink(path):
        return (os.lstat(path).st_size, 0)
    if os.path.isfile(path):
        st = os.lstat(path)
        return (st.st_size, st.st_blocks * 512)
    apparent_total_bytes = 0
    total_bytes = 0
    have = []
    for dirpath, dirnames, filenames in os.walk(path):
        apparent_total_bytes += os.lstat(dirpath).st_size
        total_bytes += os.lstat(dirpath).st_blocks * 512
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.islink(fp):
                apparent_total_bytes += os.lstat(fp).st_size
                continue
            st = os.lstat(fp)
            if st.st_ino in have:
                continue  # skip hardlinks which were already counted
            have.append(st.st_ino)
            apparent_total_bytes += st.st_size
            total_bytes += st.st_blocks * 512
        for d in dirnames:
            dp = os.path.join(dirpath, d)
            if os.path.islink(dp):
                apparent_total_bytes += os.lstat(dp).st_size
    return (apparent_total_bytes, total_bytes)

class UploadData(Task):
    def run(self, item, itemType, author, id, full, queued_for, ctx):
        ws = self.ws
        path = ctx['final_path']

        sha = hashlib.sha256()

        ws.send(json.dumps({"type": "negotiate", "method": "chunk_size"}))
        chunk_response = get_next_message(ws, "negotiate")
        # Maximum 2 MiB
        chunk_size = max(chunk_response['result'], 2*1024*1024)
        # Start takeoff
        preflight_response = {"type":None}
        while preflight_response['type'] != "mes":
            ws.send(json.dumps({"type": "upload", "method": "preflight",
                "approxSize": du(path)[1]}))
            preflight_response = get_next_message(ws)
            assert preflight_response['type'] in ("mes", "nak")
            if preflight_response['type'] == "mes":
                break
            print("Tracker is not ready to upload content.\n%s"
                  % preflight_response)
            print("Sleeping 30 seconds.")
            time.sleep(30)

        data = subprocess.Popen(
            ["tar", "-C", DATA_DIR, "-czv", ctx['final_relative_path']],
            shell=False,
            stdout=subprocess.PIPE
        )
        chunk_num = 0
        while chunk := data.stdout.read(chunk_size):
            sha.update(chunk)
            status = None
            encoded = base64.b85encode(chunk).decode("ascii")
            while status != "successful":
                ws.send(json.dumps({"type": "chunk", "data": encoded,
                                    "size": chunk_size, "num": chunk_num}))
                msg = get_next_message(ws)
                assert msg['type'] in ("upload_ack", "upload_nak")
                if msg['type'] == "upload_nak":
                    status = "nak'd"
                    print("NAK received. The server cannot handle this chunk."
                         " Retrying in 30 seconds...")
                    time.sleep(30)
                elif msg['type'] == "upload_ack":
                    assert msg['num'] == chunk_num
                    status = "successful"
                else:
                    print("Unrecognised server response.\n%s" % msg)
            chunk_num += 1
        print("Submitted ALL data.")

        sha = sha.hexdigest()
        print("Hash:", sha)
        ws.send(json.dumps({"type": "verify", "hash": {
            "type": "sha256",
            "payload": sha
        }}))
        assert get_next_message(ws, "verify_result")['res'] == "match"
        print("Hash verified.")

        ws.send(json.dumps({"type": "fin", "chan": ctx['channel']}))
        get_next_message(ws, "fin_ack")

        # TODO: Properly retry upload if it fails
        current_status = None
        while current_status != "FINISHED":
            if current_status == "FAILED":
                raise Exception("Upload FAILED according to server..")
            ws.send(json.dumps({
                "type": "upload_satuts"
            }))
            d = get_next_message(ws, "upload_status")
            if d['status'] != current_status:
                current_status = d['status']
                print(f"Item entered {current_status.upper()} status.")
            ws.send(json.dumps({"type": "ping"}))
            time.sleep(2)

        print("Upload confirmed on IA!")

class DeleteDirectories(Task):
    def run(self, item, itemType, author, id, full, queued_for, ctx):
        shutil.rmtree(ctx['final_path'])
        shutil.rmtree(os.path.join(DATA_DIR, ctx['channel']))

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
                ws.send(json.dumps({"type": "status", "task": cls.__name__}))
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
                "Reason: %d %s" % (code, data[2:].decode()))
        print("Connection closed by remote host.")
        sys.exit(0)
    else:
        raise ValueError("Unsupported opcode sent from server: %d." % opcode)
    item = data
    _ = json.loads(item)
    if type(_) == dict:
        if _['type'] != "godot" and _['type'] != "item":
            raise ValueError(f"Unexpected type {_}")
        if _['type'] != "item":
            doNotRequestItem = True # we already did - this is not the item
            return
        if _['type'] == "item":
            item = _['item']
            if not item:
                message = "No items received."
                if suppl := _.get("suppl"):
                    if suppl == "NO_NEW_SERVES":
                        message = "Items are not currently being served."
                    elif suppl == "RATE_LIMITING":
                        message = "Tracker ratelimiting is active. In order not to overload Twitch, we've limited the speed of item serves."
                    elif suppl == "ERROR":
                        message = "Tracker experienced an internal error."
                    else:
                        message = f"Server returned status {suppl}."
                print(f"{message} Trying again in 15 seconds.")
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
    ws.send(json.dumps({"type": "afternoon"}))
    ws.send(json.dumps({"type": "auth", "method": "secret", "auth": secret}))
    ws.send(json.dumps({"type": "ping"}))
    assert json.loads(ws.recv())["type"] == "godot", "Incorrect server!"
    pipeline = Pipeline(
        ws,
        PrepareDirectories,
        DownloadData,
        MoveFiles,
        UploadData,
        DeleteDirectories
    ) # later we need to do things like put warcprox in its own task
    # tini
    while True:
        updateWS(ws)

mainloop()
