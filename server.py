#!/usr/bin/env python3
"""几猫国产3A大作 — 局域网联机服务器（零依赖，Python 标准库）

用法:
  python3 server.py                # 默认 8080 端口
  python3 server.py --port 9000
  python3 server.py --selftest     # 协议全链路自测
"""
import base64
import hashlib
import json
import os
import socket
import struct
import sys
import threading
import time

RESPAWN_S = 5.0
TICK_HZ = 20
MAX_PLAYERS = 16
HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "几猫国产3A大作_V1.html")
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


# ---------- WebSocket 帧编解码 ----------

def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed")
        buf += chunk
    return buf


def read_frame(sock):
    """返回 (opcode, data)。opcode: 1=text 8=close 9=ping 10=pong"""
    h = recv_exact(sock, 2)
    op = h[0] & 0x0F
    masked = h[1] & 0x80
    ln = h[1] & 0x7F
    if ln == 126:
        ln = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif ln == 127:
        ln = struct.unpack("!Q", recv_exact(sock, 8))[0]
    mask = recv_exact(sock, 4) if masked else None
    data = recv_exact(sock, ln) if ln else b""
    if mask:
        data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    return op, data


def make_frame(data, op=1):
    if isinstance(data, str):
        data = data.encode("utf-8")
    fin_op = 0x80 | op
    ln = len(data)
    if ln < 126:
        hdr = bytes([fin_op, ln])
    elif ln < 65536:
        hdr = bytes([fin_op, 126]) + struct.pack("!H", ln)
    else:
        hdr = bytes([fin_op, 127]) + struct.pack("!Q", ln)
    return hdr + data


def ws_accept_key(key):
    digest = hashlib.sha1((key + WS_GUID).encode()).digest()
    return base64.b64encode(digest).decode()


# ---------- 房间 ----------

class Client:
    def __init__(self, sock, cid, name="玩家"):
        self.sock = sock
        self.id = cid
        self.name = name


class Room:
    def __init__(self):
        self.lock = threading.Lock()
        self.players = {}          # id -> Client（连接即占位，id 单调递增不复用）
        self.names = {}            # id -> 昵称
        self.last_state = {}       # id -> {p, ya, pi, hp}
        self.score = {}            # id -> {k, d}（断线保留，显示最终比分）
        self.respawn_at = {}       # id -> 时间戳
        self.relays = set()        # 已切断中继（晚加入者补进度）
        self.next_id = 0

    def send(self, cid, obj):
        c = self.players.get(cid)
        if not c:
            return
        try:
            c.sock.sendall(make_frame(json.dumps(obj, ensure_ascii=False)))
        except OSError:
            pass

    def broadcast(self, obj, except_id=None):
        for i in list(self.players):
            if i != except_id:
                self.send(i, obj)

    def names_map(self):
        return dict(self.names)


# ---------- 消息处理 ----------

def handle_message(room, client, msg):
    t = msg.get("t")
    cid = client.id
    with room.lock:
        if t == "hello":
            room.names[cid] = str(msg.get("name") or f"玩家{cid}")[:12]
            room.score.setdefault(cid, {"k": 0, "d": 0})
            room.send(cid, {"t": "welcome", "you": cid,
                            "players": room.names_map(),
                            "relays": sorted(room.relays)})
            room.send(cid, {"t": "begin", "players": room.names_map()})
        elif t == "state":
            room.last_state[cid] = {
                "p": msg.get("p"), "ya": msg.get("ya"),
                "pi": msg.get("pi"), "hp": msg.get("hp"),
            }
        elif t == "hit":
            try:
                to = int(msg.get("to"))
            except (TypeError, ValueError):
                to = None
            if to in room.players:
                room.send(to, {"t": "hit", "from": cid, "to": to,
                               "dmg": msg.get("dmg", 0)})
        elif t == "dead":
            by = msg.get("by", "ai")
            if by != "ai" and by in room.score:
                room.score[by]["k"] += 1
            room.score.setdefault(cid, {"k": 0, "d": 0})["d"] += 1
            st = room.last_state.get(cid)
            pos = st["p"] if st else [0, 1, 0]
            room.broadcast({"t": "dead", "who": cid, "by": by,
                            "score": room.score, "pos": pos})
            room.respawn_at[cid] = time.time() + RESPAWN_S
        elif t == "relay":
            rid = msg.get("id")
            if rid:
                room.relays.add(str(rid))
            room.broadcast({"t": "relay", "id": rid, "from": cid}, except_id=cid)
        elif t == "extract":
            room.broadcast({"t": "extract", "from": cid}, except_id=cid)
        elif t == "fire":  # 预留：客户端目前不发，收到就转发
            room.broadcast({"t": "fire", "from": cid,
                            "o": msg.get("o"), "d": msg.get("d")}, except_id=cid)


def client_loop(room, client, sock):
    cid = client.id
    try:
        while True:
            op, data = read_frame(sock)
            if op == 8:
                break
            if op == 9:
                with room.lock:
                    try:
                        sock.sendall(make_frame(data, op=10))
                    except OSError:
                        pass
                continue
            if op != 1:
                continue
            try:
                msg = json.loads(data.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
            if not isinstance(msg, dict):
                continue
            handle_message(room, client, msg)
    except (ConnectionError, OSError):
        pass
    finally:
        with room.lock:
            if room.players.get(cid) is client:
                del room.players[cid]
                room.names.pop(cid, None)
                room.last_state.pop(cid, None)
                room.respawn_at.pop(cid, None)
                # score 保留：断线者仍显示最终比分
                room.broadcast({"t": "leave", "who": cid})
            try:
                sock.close()
            except OSError:
                pass


# ---------- HTTP / 握手 ----------

def read_http_request(sock):
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError
        buf += chunk
        if len(buf) > 16384:
            raise ConnectionError
    head, _, rest = buf.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    req = lines[0].split(" ")
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    return (req[0] if req else "GET"), (req[1] if len(req) > 1 else "/"), headers, rest


def serve_http(sock, method, path, html_bytes):
    if method != "GET":
        sock.sendall(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 4\r\n\r\n405")
        return
    status = "HTTP/1.1 200 OK\r\n"
    if path in ("/", "/index.html"):
        body = html_bytes
        ctype = "text/html; charset=utf-8"
    elif path == "/favicon.ico":
        body = b""
        ctype = "image/x-icon"
    else:
        status = "HTTP/1.1 404 Not Found\r\n"
        body = b"404"
        ctype = "text/plain; charset=utf-8"
    head = (
        f"{status}"
        f"Content-Type: {ctype}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    try:
        sock.sendall(head + body)
    except OSError:
        pass


def handle_conn(room, sock, html_bytes):
    try:
        method, path, headers, _ = read_http_request(sock)
        if headers.get("upgrade", "").lower() == "websocket":
            key = headers.get("sec-websocket-key", "")
            if not key:
                sock.close()
                return
            resp = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {ws_accept_key(key)}\r\n\r\n"
            )
            sock.sendall(resp.encode())
            with room.lock:
                if len(room.players) >= MAX_PLAYERS:
                    sock.sendall(make_frame(json.dumps({"t": "full"})))
                    sock.close()
                    return
                cid = room.next_id
                room.next_id += 1
                client = Client(sock, cid)
                room.players[cid] = client
            client_loop(room, client, sock)
        else:
            # 每次请求重读 HTML：改游戏文件后无需重启服务器
            try:
                with open(HTML_PATH, "rb") as f:
                    html_bytes = f.read()
            except OSError:
                pass
            serve_http(sock, method, path, html_bytes)
            sock.close()
    except (ConnectionError, OSError):
        try:
            sock.close()
        except OSError:
            pass


def tick_loop(room):
    while True:
        time.sleep(1.0 / TICK_HZ)
        with room.lock:
            now = time.time()
            for cid in [c for c, at in room.respawn_at.items() if now >= at]:
                del room.respawn_at[cid]
                st = room.last_state.get(cid)
                pos = st["p"] if st else [0, 1, 0]
                room.broadcast({"t": "respawn", "who": cid, "pos": pos})
            if not room.players:
                continue
            players = {}
            for pid in room.players:
                st = room.last_state.get(pid)
                if st:
                    players[pid] = {"name": room.names.get(pid, f"玩家{pid}"),
                                    "p": st["p"], "ya": st["ya"],
                                    "pi": st["pi"], "hp": st["hp"]}
            for cid in room.players:
                room.send(cid, {
                    "t": "states", "you": cid,
                    "players": players,
                    "score": room.score,
                })


def lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def start_server(port=8080):
    """绑定端口并返回 (listen_sock, actual_port, room)。不进入 accept 循环。"""
    with open(HTML_PATH, "rb") as f:
        html_bytes = f.read()
    room = Room()
    ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ls.bind(("0.0.0.0", port))
    ls.listen(8)
    actual = ls.getsockname()[1]
    threading.Thread(target=tick_loop, args=(room,), daemon=True).start()

    def accept_loop():
        while True:
            try:
                csock, _ = ls.accept()
            except OSError:
                break
            threading.Thread(target=handle_conn, args=(room, csock, html_bytes),
                             daemon=True).start()

    threading.Thread(target=accept_loop, daemon=True).start()
    return ls, actual, room


# ---------- 自测（零依赖协议客户端） ----------

class _BufSock:
    """把握手响应里捎带的帧字节保留下来给 read_frame 用（TCP 合流）。"""

    def __init__(self, sock, buf):
        self.sock = sock
        self.buf = buf

    def recv(self, n):
        if self.buf:
            out, self.buf = self.buf[:n], self.buf[n:]
            return out
        return self.sock.recv(n)


class TestClient:
    """最小 WebSocket 客户端（握手 + 掩码帧），仅自测用。"""

    def __init__(self, port):
        self.s = socket.create_connection(("0.0.0.0", port), timeout=3)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
        )
        self.s.sendall(req.encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            buf += self.s.recv(4096)
        assert b"101" in buf.split(b"\r\n")[0], f"handshake failed: {buf[:80]!r}"
        _, _, rest = buf.partition(b"\r\n\r\n")
        self._rd = _BufSock(self.s, rest)

    def send(self, obj):
        data = json.dumps(obj).encode()
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        ln = len(data)
        if ln < 126:
            hdr = bytes([0x81, 0x80 | ln])
        elif ln < 65536:
            hdr = bytes([0x81, 0x80 | 126]) + struct.pack("!H", ln)
        else:
            hdr = bytes([0x81, 0x80 | 127]) + struct.pack("!Q", ln)
        self.s.sendall(hdr + mask + masked)

    def recv(self, timeout=3):
        self.s.settimeout(timeout)
        op, data = read_frame(self._rd)
        if op == 8:
            raise ConnectionError("closed")
        if op == 9:
            self.s.sendall(make_frame(data, op=10))
            return self.recv(timeout)
        return json.loads(data.decode("utf-8"))

    def wait_for(self, types, timeout=3):
        """收到 types 中任一消息为止（跳过其他消息）。types 可为 str 或 set。"""
        if isinstance(types, str):
            types = {types}
        deadline = time.time() + timeout
        while time.time() < deadline:
            m = self.recv(timeout=max(0.1, deadline - time.time()))
            if m.get("t") in types:
                return m
        raise AssertionError(f"timeout waiting for {types}")

    def close(self):
        try:
            self.s.sendall(make_frame(b"", op=8))
            self.s.close()
        except OSError:
            pass


def selftest():
    global RESPAWN_S
    RESPAWN_S = 0.3
    ls, port, room = start_server(port=0)

    def wait_msgs(cl, typ, whos, timeout=3):
        """收集 typ 消息直到见到全部 whos（消息可能乱序到达）。"""
        seen = {}
        deadline = time.time() + timeout
        while time.time() < deadline and len(seen) < len(whos):
            m = cl.wait_for(typ, timeout=max(0.1, deadline - time.time()))
            if m.get("who") in whos and m["who"] not in seen:
                seen[m["who"]] = m
        assert len(seen) == len(whos), f"missing {typ} for {whos}: got {list(seen)}"
        return seen

    # 三人加入：各自 welcome{you} + begin{players}
    a = TestClient(port)
    a.send({"t": "hello", "name": "Alice"})
    wa = a.wait_for("welcome")
    assert wa["you"] == 0, wa
    assert wa["players"] == {"0": "Alice"}, wa
    assert wa["relays"] == [], wa
    ba = a.wait_for("begin")
    assert ba["players"] == {"0": "Alice"}, ba

    b = TestClient(port)
    b.send({"t": "hello", "name": "Bob"})
    wb = b.wait_for("welcome")
    assert wb["you"] == 1 and wb["players"] == {"0": "Alice", "1": "Bob"}, wb
    b.wait_for("begin")

    c = TestClient(port)
    c.send({"t": "hello", "name": "Cara"})
    wc = c.wait_for("welcome")
    assert wc["you"] == 2 and len(wc["players"]) == 3, wc
    c.wait_for("begin")

    # 状态中继：A 的状态 B、C 都收到，you 各自正确
    a.send({"t": "state", "p": [1, 2, 3], "ya": 0.5, "pi": 0.1, "hp": 100})
    stb = b.wait_for("states")
    stc = c.wait_for("states")
    assert stb["you"] == 1 and stc["you"] == 2, (stb, stc)
    assert stb["players"]["0"]["p"] == [1, 2, 3], stb
    assert stc["players"]["0"]["p"] == [1, 2, 3], stc

    # 定向伤害：只有目标 C 收到
    a.send({"t": "hit", "to": 2, "dmg": 30})
    hit = c.wait_for("hit")
    assert hit == {"t": "hit", "from": 0, "to": 2, "dmg": 30}, hit
    # B 侧排空到下一个 states，期间不得出现 hit
    while True:
        m = b.recv(timeout=1.5)
        assert m.get("t") != "hit", f"B leaked hit: {m}"
        if m.get("t") == "states":
            break

    # 计分：by="ai" 不加分；by=玩家id 加给该玩家（两条 dead 可能乱序到达，一次收集）
    b.send({"t": "dead", "by": "ai"})
    a.send({"t": "dead", "by": 1})

    sc = wait_msgs(c, "dead", {0, 1})
    assert sc[1]["score"]["1"]["d"] == 1, sc[1]          # B 死了一次
    assert sc[0]["score"]["1"]["k"] == 1, sc[0]          # A 被 B 杀 → B 得 1 分
    wait_msgs(a, "dead", {0, 1})                         # A 侧同样收到两条

    # 重生（respawn 广播给所有人）
    wait_msgs(a, "respawn", {0, 1})
    wait_msgs(b, "respawn", {0, 1})
    wait_msgs(c, "respawn", {0, 1})

    # 任务事件广播给其他所有人
    a.send({"t": "relay", "id": "RELAY_A"})
    assert b.wait_for("relay")["id"] == "RELAY_A"
    assert c.wait_for("relay")["id"] == "RELAY_A"
    c.send({"t": "relay", "id": "RELAY_C"})
    assert a.wait_for("relay")["id"] == "RELAY_C"
    assert b.wait_for("relay")["id"] == "RELAY_C"

    # 晚加入者从 welcome.relays 补任务进度
    d = TestClient(port)
    d.send({"t": "hello", "name": "Dan"})
    wd = d.wait_for("welcome")
    assert set(wd["relays"]) == {"RELAY_A", "RELAY_C"}, wd
    d.wait_for("begin")

    # 撤离广播
    c.send({"t": "extract"})
    assert a.wait_for("extract")["from"] == 2
    assert b.wait_for("extract")["from"] == 2

    # 断线广播
    b.close()
    assert a.wait_for("leave")["who"] == 1
    assert c.wait_for("leave")["who"] == 1
    assert d.wait_for("leave")["who"] == 1

    # 满员：当前 A/C/D=3，补13个到16，第17个收到 full
    extra = []
    for i in range(13):
        x = TestClient(port)
        x.send({"t": "hello", "name": f"X{i}"})
        x.wait_for("welcome")
        x.wait_for("begin")
        extra.append(x)
    full_c = TestClient(port)
    fm = full_c.recv(timeout=3)
    assert fm.get("t") == "full", fm
    full_c.close()

    for cl in [a, c, d] + extra:
        cl.close()
    ls.close()
    print("SELFTEST PASS")


def main():
    if "--selftest" in sys.argv:
        selftest()
        return
    port = 18080
    if "--port" in sys.argv:
        port = int(sys.argv[sys.argv.index("--port") + 1])
    ls, actual, _ = start_server(port)
    ip = lan_ip()
    print("几猫联机服务器已启动")
    print(f"  游戏地址:  http://{ip}:{actual}   （发给对手，浏览器打开即可）")
    print(f"  WebSocket: ws://{ip}:{actual}")
    print("  按 Ctrl+C 停止")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\n已停止")
        ls.close()


if __name__ == "__main__":
    main()
