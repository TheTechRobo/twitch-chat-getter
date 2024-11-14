######### NOTE ######
# REEnABLE PREVENT SIGINT ###
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

# FIXME: Update this whenever you make a non-cosmetic change.
# FIXME: It will be stored in the WARC file and sent to the tracker.
VERSION = "20240728.01"

import atexit, time, sys

sys.stdout.flush()

print("x=================x\n"
      "|Container Started|\n"
      "x=================x\n",
      end="", flush=True)

print("It is now", time.time(), "o'clock.", flush=True)

@atexit.register
def wait_on_exit():
    print("Waiting 15 seconds before exiting.\n\tFeel free to interrupt this, this is just so that broken machines dont drain the queue.")
    time.sleep(15)

import websocket, json, os, subprocess, shutil, os, os.path, base64
import hashlib, traceback, signal, collections, struct, hashlib, logging, fcntl
import requests, yt_dlp, prevent_sigint, subprocess_with_logging, threading, typing
import random, tempfile, tarfile

secret = os.environ['SECRET']
DATA_DIR = os.environ['DATA_DIR']
assert DATA_DIR.startswith("/"), "DATA_DIR path must be absolute"
MACHINE_NAME = os.environ['MACHINE_NAME']

def open_and_wait(args, ws, *, also_write_to=None):
    subprocess_with_logging.run_with_log(args, shell=False, start_new_session=True, check=True, also_write_to=also_write_to)

class ConnectionClosedCleanly(Exception):
    """
    Raised by _get_next_message when a close frame is sent.
    """
    def __init__(self, code, reason):
        self.code = code
        self.reason = reason

class Websocket:
    """
    A wrapper for a websocket.
    """
    def __init__(self, ws):
        self.ws = ws
        self.seq = 0
        self.seq_lock = threading.Lock()
        atexit.register(self.close_then_shutdown)

    def send(self, msg):
        """
        Sends a message, and returns the server's response.
        """
        with self.seq_lock:
            self.seq += 1
            seq = self.seq
        msg = json.loads(msg)
        msg['seq'] = seq
        self.ws.send(json.dumps(msg))
        return self._get_response(seq)

    def send_unchecked(self, msg):
        """
        Sends a message that the server will not provide a response for.
        """
        self.ws.send(msg)

    def _get_next_message(self, wanted_type=None, ok_close_connection=False):
        data = {"type": "godot"}
        while data['type'] == "godot":
            opcode, data = self.ws.recv_data()
            if opcode == 1:
                data = json.loads(data)
            elif opcode == 8:
                code = struct.unpack("!H", data[:2])[0]
                self.close(code)
                raise ConnectionClosedCleanly(code, f"{data[2:].decode()}")
            else:
                print(f"Unknown opcode.\nData: {[opcode, data]}")
                sys.exit(5)
        if wanted_type:
            assert data['type'] == wanted_type, f"Bad type for {data}."
        return data

    def get_next_message(self, wanted_type=None, ok_close_connection=False):
        """
        Gets a message. If wanted_type is set, an exception will be thrown if
        the message does not match that type.
        """
        data = self._get_next_message(wanted_type, ok_close_connection)
        if data['type'] == "response":
            print(f"Received response: {data}")
            raise RuntimeError("Unchecked response!")

    def _get_response(self, seq):
        data = self._get_next_message("response")
        if seq != data['seq']:
            print(f"Received mismatched response: {data}")
            raise RuntimeError("Mismatched response!")
        if data['response'] in {"error", "err"}:
            print(f"Received error response: {data}")
            raise RuntimeError("Received negative response!")
        return data

    def recv(self):
        """
        Receives one message.
        """
        return self.get_next_message()

    def ping(self):
        return self.ws.ping()

    def close(self, status=1000, reason=b""):
        return self.ws.close(status, reason)

    def close_then_shutdown(self, status=1000, reason=b""):
        self.close(status, reason)
        self.ws.shutdown()

class Task:
    def __init__(self, logger, ws):
        self.logger = logger
        self.name = self.__class__.__name__
        self.ws = ws

    def run(self, item, itemType, author, id, full, queued_for, ctx):
        raise NotImplementedError("Implement the `run' method")

class PrepareDirectories(Task):
    def prepare_directories(self, ctx):
        ctime = time.time()
        ctx['start_time'] = ctime
        assert "/" not in self.item
        temp_folder = tempfile.mkdtemp(suffix=f"{self.itemType}-{self.item}-{ctime}.tmp", dir=DATA_DIR)
        crawl_folder = os.path.join(temp_folder, "crawl")
        os.mkdir(crawl_folder)
        ctx['root_folder'] = temp_folder
        ctx['crawl_folder'] = crawl_folder
        os.chdir(crawl_folder)

    def run(self, item, itemType, author, id, full, queued_for, ctx):
        self.item, self.itemType = item, itemType
        self.prepare_directories(ctx)

class DownloadData(Task):
    warcprox_pid: typing.Optional[int] = None
    warcprox_log = None

    def _run_warcprox(self):
        print(f"Starting warcprox for Item {self.item}")
        self.warcprox_log = open("warcprox__log.log", "w+")
        self.process = subprocess.Popen([
            "warcprox", "-zp", self.WARCPROX_PORT,
            "-c", "./file.pem",
            "--crawl-log-dir", "."
        ], start_new_session=True, stdout=self.warcprox_log, stderr=self.warcprox_log)
        self.warcprox_pid = self.process.pid

    def _run_warcprox_tail(self):
        print("Starting tail...")
        self.tailStop = threading.Event()
        self.tail: threading.Thread = threading.Thread(target=run_warcprox_tail, args=(self.tailStop, self.ws, self.id), daemon=True)
        self.tail.start()
        print("Thread!!!", self.tail)

    def _start_warcprox(self):
        print("Starting warcprox...")
        self.WARCPROX_PORT = "4553"
        self._run_warcprox()
        self._run_warcprox_tail()
        time.sleep(6)
        file_hash = ""
        with open(__file__, "rb") as file:
            file_hash = hashlib.sha256(file.read()).hexdigest()
        assert requests.request("WARCPROX_WRITE_RECORD", f"http://localhost:{self.WARCPROX_PORT}/burnthetwitch_client_version", headers={"Content-Type": "text=plain;charset=utf-8", "WARC-Type": "resource"}, data="burnthetwitch client.py sha256:%s v:%s" % (file_hash, VERSION)).status_code == 204
        self.ws.ping()

    def _kill_warcprox(self, pid, sig="INT"):
        print("Terminating warcprox")
        if not pid:
            print("Nothing to kill")
            return
        subprocess.run([
            shutil.which("kill"), f"-{sig}", str(pid)
        ]).check_returncode()
        try:
            self.ws.ping()
        except Exception:
            pass

    def _run_vod(self, item):
        ws = self.ws
        print("Downloading metadata")
        open_and_wait([
            "yt-dlp", "--ignore-config", "--skip-download",
            # Keep the filenames to a sane length
            # All of the metadata used by the filename is in the infojson/WARC,
            # so we're not losing any data by doing this.
            # Some items were failing due to a too-long filename.
            # ext4 limits it to 255 chars, but let's play it safe.
            "--trim-filenames", "200",
            # Some VODs return a 403 on the m3u8, so format data
            # will be missing on those ones since we're stifling the errors.
            # Better than getting nothing, though
            "--ignore-no-formats-error",
            # Write metadata and thumbnail
            "--write-info-json", "--write-description", "--write-thumbnail", "--write-all-thumbnails",
            # TODO: Check the certificate
            "--no-check-certificate",
            # Multiple retries
            "--retries", "4",
            # yt-dlp chat extraction is currently broken, so let's not make it crash
            #"--embed-subs", "--all-subs",
            # Limit speed and put the infojson in a specific file
            "--limit-rate", "300k", "-o", "%(id)s.%(ext)s",
            "--proxy", "http://localhost:" + self.WARCPROX_PORT,
            "https://twitch.tv/videos/" + item
        ], ws)
        print("Pre-emptively touching file...")
        with open("chat.json", "w+") as file:
            file.write("[]") # workaround for chat_downloader only writing the file when there are messages
        print("Downloading chat")
        os.environ['CURL_CA_BUNDLE'] = "./file.pem"
        try:
            open_and_wait([
                shutil.which("chat_downloader"),
                "--message_groups", 'messages bans deleted_messages hosts room_states user_states notices chants other bits subscriptions upgrades raids rituals mods colours commercials vips charity', "-o", "chat.json",
                "--logging", "warning",
                "--interruptible_retry", "False",
                "--proxy", "http://localhost:" + self.WARCPROX_PORT,
                "https://twitch.tv/videos/" + item
            ], ws)
        finally:
            del os.environ['CURL_CA_BUNDLE']
        ws.ping()

    def _run_channel(self, item):
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
        self._start_warcprox()
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
                print("Stopping Tail Thread")
                self.tailStop.set()
                print("Joining Tail Thread")
                self.tail.join()
                print("Joined Tail Thread")
            except Exception as ename:
                print("...Failed.", json.dumps(traceback.format_exc()))
            try:
                self.warcprox_log.close()
            except Exception:
                print("Could not close file")
                print(traceback.format_exc())
                print("---------------")
            try:
                self._kill_warcprox(self.process.pid)
                self.process.wait()
            except Exception:
                print("Couldnt kill warcprox lol")
                print(traceback.format_exc())
                raise

class MoveFiles(Task):
    def _move(self, ctx, channel):
        ctx['logfile'] = "Unavailable."
        root = ctx['root_folder']
        os.chdir(root)
        new_path = str(time.time())
        new_absolute_path = os.path.join(root, new_path)
        os.rename(ctx['crawl_folder'], new_absolute_path)
        ctx['final_relative_path'] = new_path
        ctx['final_path'] = new_absolute_path
        ctx['channel'] = channel

    def run(self, item, itemType, author, id, full, queued_for, ctx):
        self.item = item
        self.itemType = itemType

        if itemType == 'c':
            channel = self.item
            self._move(ctx, channel)
        elif itemType == 'v':
            with open(os.path.join(ctx['crawl_folder'], f"v{self.item}.info.json")) as f:
                data = json.load(f)
                channel = data['uploader_id']
            self._move(ctx, channel)
        else:
            raise ValueError("unsupported item type")

def nonblocking_readlines(file):
    print("START NONBL")
    fd = file.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fl |= os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, fl)
    buffer = bytearray()
    while True:
        try:
            block = os.read(fd, 8192)
        except BlockingIOError:
            yield ""
            continue

        if not block:
            yield ""
            continue

        buffer.extend(block)

        while True:
            n = buffer.find(b"\n")
            if n == -1:
                # No newlines yet
                break
            # Get the data
            yield buffer[:n+1].decode(errors="backslashreplace")
            buffer = buffer[n+1:]

def run_warcprox_tail(stop, ws, id):
    with open("warcprox__log.log") as f:
        for i in nonblocking_readlines(f):
            if not i:
                if stop.is_set():
                    return
                time.sleep(0.4)
                continue
            try:
                ws.send_unchecked(json.dumps({"type": "WLOG", "data": i, "item": id}))
            except Exception as ename:
                print("Could not submit Warcprox Log, raising.", file=sys.stdout.old, flush=True)
                raise
            print(i, file=sys.stdout.old, end="")

class UploadData(Task):
    def run(self, item, itemType, author, id, full, queued_for, ctx):
        ws = self.ws
        path = ctx['final_relative_path']
        root = ctx['root_folder']

        while True:
            target_response = ws.send(json.dumps({"type": "upload"}))
            if target_response['status'] == "ok":
                url = target_response['url']
                print(f"Received target URL {url}.")
                break
            print(f"No targets available. Sleeping 30 seconds. {target_response}")
            time.sleep(30)

        fn = os.path.join(root, str(ctx['start_time']))
        try:
            with tarfile.open(fn, "x:") as tar:
                tar.add(path)
            open_and_wait([
                shutil.which("bullseye-client"),
                "--project", "burnthetwitch",
                "--pipeline", "tar",
                "--uploader", MACHINE_NAME,
                "--base-url", url,
                fn,
                f"{itemType}:{item}"
            ], ws, also_write_to=sys.stdout.old) # todo: make this unnecessary
        finally:
            os.remove(fn)

class DeleteDirectories(Task):
    def run(self, item, itemType, author, id, full, queued_for, ctx):
        os.chdir(DATA_DIR)
        shutil.rmtree(ctx['root_folder'])

class Logger:
    def __init__(self, prefix, old):
        self.prefix = prefix
        self.old = old
        self.ws = None
        self.item = None
        self.LOG_STUFF = False

    def write(self, message):
        if message.strip():
            # print likes to do a separate call for its `end` parameter
            if self.LOG_STUFF:
                logging.info(f"{self.prefix} {message.rstrip()}")
            if self.ws:
                try:
                    self.ws.send_unchecked(json.dumps({"type": "WLOG", "data": f"{self.prefix} {message.rstrip()}", "item": self.item}))
                except Exception:
                    print("Failed to post message to server.", file=self.old)
        print(message, file=self.old, end="")

    def flush(self):
        self.old.flush()

class RedirectStdout(Task):
    def run(self, item, itemType, author, id, full, queued_for, ctx):
        # This is a really shitty solution, but I'd rather not have to change all the print statements.
        sys.stdout.ws = self.ws
        sys.stdout.item = id
        logging.basicConfig(filename=os.path.join(ctx['crawl_folder'], "btt.log"), level=logging.DEBUG,
                format="[%(asctime)s] %(levelname)s %(message)s (%(lineno)d/%(funcName)s/%(filename)s)",
                force=True
        )
        ctx['logfile'] = os.path.join(ctx['crawl_folder'], "btt.log")
        sys.stdout.LOG_STUFF = True

sys.stdbak = sys.stdout
sys.stdout = Logger("PRINT", sys.stdbak)
# TODO: Redirect stderr?

class RecoverStdout(Task):
    def run(self, _item, _itemType, _author, _id, _full, _queued_for, _ctx):
        sys.stdout.LOG_STUFF = False

class FinalCleanup(Task):
    def run(self, item, itemType, author, id, full, queued_for, ctx):
        sys.stdout.ws = None
        sys.stdout.item = None

class Pipeline:
    tasks: list[Task] = []

    def __init__(self, ws, *args):
        for task in args:
            self.tasks.append(task(print, ws))

    def _start(self, item, ws, author, ident, full, queuedFor):
        ws.ping()
        fullItem = item
        ctx = {}
        try:
            itemType = 'v'
            if item.startswith('c'):
                itemType = 'c'
                item = item[1:]
            for task in self.tasks:
                cls = task.__class__
                ws.send(json.dumps({"type": "status", "task": cls.__name__, "id": ident}))
                print(f"Starting {cls.__name__} for item {itemType}{item}")
                task.run(item, itemType, author, ident, full, queuedFor, ctx)
                print(f"Finished {cls.__name__} for item {itemType}:{item}")
        except Exception:
            print("Caught exception!")
            print("Sending to server and aborting.")
            data = "".join(traceback.format_exception(*sys.exc_info()))

            try:
                with open(ctx['logfile']) as file:
                    logfile = file.read()
            except FileNotFoundError:
                logfile = "Logfile not found. The file may have already been moved."
            try:
                with open(os.path.join(ctx['crawl_folder'], "warcprox__log.log")) as file:
                    logfile += "\n\nWarcprox log:\n"
                    logfile += file.read()
            except FileNotFoundError:
                logfile += "Warcprox logfile not found. The file may have already been moved."

            resp = None
            while not resp or resp.status_code != 200:
                resp = requests.put("https://transfer.archivete.am/traceback", data=f"{data}\n{os.getcwd()}\nLogs:\n{logfile}", timeout=120)
            url = resp.text.replace(".am/", ".am/inline/")

            ws.send_unchecked(json.dumps({
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
        #with prevent_sigint.signal_fence(signal.SIGINT, on_deferred_signal=self._stuff):
        return self._start(*args, **kwargs)

    @staticmethod
    def _stuff(*_args, **_kwargs):
        print("Stopping when current tasks are finished...")

def updateWS(ws: Websocket):
    print("Requesting item")
    try:
        response = ws.send(json.dumps({"type": "get"}))
    except ConnectionClosedCleanly as info:
        print(f"Server closed connection.\nReason: {info.code} {info.reason}")
        sys.exit(4)
    print(response)
    assert response['response'] == "item"
    item = response['item']
    if not item:
        message = "No items received."
        if suppl := response.get("suppl"):
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
        return
    author = response['started_by']
    id = response['id']
    queuedFor = response.get("queued_for_item")
    print(f"Got item {item} for author {author}")
    pipeline.start(item, ws, author, id, response, queuedFor)

pipeline = None

def mainloop():
    global pipeline # pylint: disable=global-statement

    # init
    rws = websocket.WebSocket()
    rws.connect(os.environ["CONNECT"])
    ws = Websocket(rws)
    try:
        welcome = ws.send(json.dumps({"type": "afternoon", "version": VERSION, "auth": secret, "name": MACHINE_NAME}))
    except ConnectionClosedCleanly as info:
        print(f"Connection didn't open ({info.code} {info.reason})")
        sys.exit(4)
    if welcome['response'] != "welcome":
        print(welcome)
        raise RuntimeError("Server did not grant us a warm welcome")
    ws.ping()
    pipeline = Pipeline(
        ws,
        PrepareDirectories,
        RedirectStdout,
        DownloadData,
        RecoverStdout,
        MoveFiles,
        UploadData,
        DeleteDirectories,
        FinalCleanup
    ) # later we need to do things like put warcprox in its own task
    # tini
    while True:
        updateWS(ws)

mainloop()
