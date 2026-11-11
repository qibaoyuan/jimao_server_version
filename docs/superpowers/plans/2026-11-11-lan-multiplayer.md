# 几猫局域网联机对战版 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 单机 HTML FPS 变成局域网 1v1 联机（保留 PvE 任务 + AI），输入服务器地址进入，双方可互相攻击。

**Architecture:** 零依赖 Python 标准库单文件 `server.py`（同端口 HTTP 游戏页 + WebSocket 中继，20Hz 状态广播）；客户端在原 HTML 末尾追加一个独立 `<script>` 联机模块，通过 `window.__G` 与事件系统挂钩，仅在原游戏代码加 3 个内部入口暴露。AI 敌人各自本地模拟，仅玩家交互与任务事件走网络。

**Tech Stack:** Python 3.12 stdlib（socket/threading/base64，手写 WebSocket 帧）、原生浏览器 WebSocket、原 Three.js 游戏（无新依赖）。

**Spec:** `docs/superpowers/specs/2026-11-11-lan-multiplayer-design.md`

**注意:** 项目不是 git 仓库，所有 commit 步骤跳过（如需版本控制先 `git init`）。原 HTML 为只读权限（`-r--r--r--`），动手前先 `chmod u+w`。

---

### Task 1: server.py — HTTP + WebSocket 中继服务器

**Files:**
- Create: `server.py`

- [ ] **Step 1: 写入完整 server.py**

```python
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
        self.players = [None, None]       # Client | None
        self.names = ["玩家1", "玩家2"]
        self.last_state = [None, None]    # {p, ya, pi, hp}
        self.score = [{"k": 0, "d": 0}, {"k": 0, "d": 0}]
        self.respawn_at = [None, None]
        self.begin_sent = False

    def other(self, cid):
        return 1 - cid

    def send(self, cid, obj):
        c = self.players[cid]
        if not c:
            return
        try:
            c.sock.sendall(make_frame(json.dumps(obj, ensure_ascii=False)))
        except OSError:
            pass

    def broadcast(self, obj, except_id=None):
        for i in range(2):
            if i != except_id and self.players[i]:
                self.send(i, obj)

    def both(self):
        return self.players[0] and self.players[1]


# ---------- 消息处理 ----------

def handle_message(room, client, msg):
    t = msg.get("t")
    cid = client.id
    oid = room.other(cid)
    with room.lock:
        if t == "state":
            room.last_state[cid] = {
                "p": msg.get("p"), "ya": msg.get("ya"),
                "pi": msg.get("pi"), "hp": msg.get("hp"),
            }
        elif t == "hit":
            room.send(oid, {"t": "hit", "from": cid, "dmg": msg.get("dmg", 0)})
        elif t == "dead":
            by = msg.get("by", "ai")
            if by == "p":
                room.score[oid]["k"] += 1
            room.score[cid]["d"] += 1
            st = room.last_state[cid]
            pos = st["p"] if st else [0, 1, 0]
            room.broadcast({"t": "dead", "who": cid, "by": by,
                            "score": room.score, "pos": pos})
            room.respawn_at[cid] = time.time() + RESPAWN_S
        elif t == "relay":
            room.send(oid, {"t": "relay", "id": msg.get("id"), "from": cid})
        elif t == "extract":
            room.send(oid, {"t": "extract", "from": cid})
        elif t == "fire":  # 预留：客户端目前不发，收到就转发
            room.send(oid, {"t": "fire", "from": cid,
                            "o": msg.get("o"), "d": msg.get("d")})
        elif t == "hello":
            room.names[cid] = str(msg.get("name") or f"玩家{cid + 1}")[:12]
            room.send(cid, {"t": "welcome", "you": cid})
            if room.both() and not room.begin_sent:
                room.begin_sent = True
                room.broadcast({"t": "begin", "names": room.names})


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
            handle_message(room, client, msg)
    except (ConnectionError, OSError):
        pass
    finally:
        with room.lock:
            if room.players[cid] is client:
                room.players[cid] = None
                room.last_state[cid] = None
                room.respawn_at[cid] = None
                room.send(room.other(cid), {"t": "leave", "who": cid})
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
        body = b"405"
        sock.sendall(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 4\r\n\r\n405")
        return
    if path in ("/", "/index.html"):
        body = html_bytes
        ctype = "text/html; charset=utf-8"
    elif path == "/favicon.ico":
        body = b""
        ctype = "image/x-icon"
    else:
        body = b"404"
        ctype = "text/plain; charset=utf-8"
    head = (
        "HTTP/1.1 200 OK\r\n"
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
                slot = next((i for i in range(2) if room.players[i] is None), None)
                if slot is None:
                    sock.sendall(make_frame(json.dumps({"t": "full"})))
                    sock.close()
                    return
                client = Client(sock, slot)
                room.players[slot] = client
            client_loop(room, client, sock)
        else:
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
            if not room.both():
                continue
            now = time.time()
            for i in range(2):
                at = room.respawn_at[i]
                if at and now >= at:
                    room.respawn_at[i] = None
                    st = room.last_state[i]
                    pos = st["p"] if st else [0, 1, 0]
                    room.broadcast({"t": "respawn", "who": i, "pos": pos})
                    room.last_state[i] = None
            if room.begin_sent:
                for i in range(2):
                    oid = room.other(i)
                    room.send(i, {
                        "t": "states", "you": i,
                        "p0": room.last_state[0],
                        "p1": room.last_state[1],
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

class TestClient:
    """最小 WebSocket 客户端（握手 + 掩码帧），仅自测用。"""

    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=3)
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
        op, data = read_frame(self.s)
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
    a = TestClient(port)
    a.send({"t": "hello", "name": "Alice"})
    wa = a.wait_for("welcome")
    assert wa["you"] == 0, wa

    b = TestClient(port)
    b.send({"t": "hello", "name": "Bob"})
    wb = b.wait_for("welcome")
    assert wb["you"] == 1, wb
    ba = a.wait_for("begin")
    bb = b.wait_for("begin")
    assert ba["names"] == ["Alice", "Bob"], ba

    # 状态中继
    a.send({"t": "state", "p": [1, 2, 3], "ya": 0.5, "pi": 0.1, "hp": 100})
    st = b.wait_for("states")
    assert st["p0"]["p"] == [1, 2, 3], st

    # 伤害转发
    a.send({"t": "hit", "dmg": 30})
    hit = b.wait_for("hit")
    assert hit == {"t": "hit", "from": 0, "dmg": 30}, hit

    # 击杀计分（by=p 才给对方加分）
    b.send({"t": "dead", "by": "p"})
    dead = a.wait_for("dead")
    assert dead["who"] == 1 and dead["score"][0]["k"] == 1, dead

    # 重生
    rs = a.wait_for("respawn")
    assert rs["who"] == 1, rs
    b.wait_for("respawn")

    # 任务事件
    a.send({"t": "relay", "id": "RELAY_A"})
    rl = b.wait_for("relay")
    assert rl["id"] == "RELAY_A", rl
    b.send({"t": "extract"})
    ex = a.wait_for("extract")
    assert ex["from"] == 1, ex

    # 断线通知
    b.close()
    lv = a.wait_for("leave")
    assert lv["who"] == 1, lv
    a.close()
    ls.close()
    print("SELFTEST PASS")


def main():
    if "--selftest" in sys.argv:
        selftest()
        return
    port = 8080
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
```

- [ ] **Step 2: 语法检查**

Run: `python3 -m py_compile server.py`
Expected: 无输出（通过）

- [ ] **Step 3: 跑协议自测**

Run: `python3 server.py --selftest`
Expected: `SELFTEST PASS`

失败则修 `server.py` 直到通过（帧编解码/握手是唯一易错点）。

---

### Task 2: 原游戏暴露 3 个内部入口

**Files:**
- Modify: `几猫国产3A大作_V1.html`（game 对象 `debugCut` 处，约 char 1403605）

- [ ] **Step 1: 恢复写权限**

Run: `chmod u+w 几猫国产3A大作_V1.html`

- [ ] **Step 2: 用 Edit 工具改 game 对象**

先 Read 该区域（用 grep 定位行号后 Read 附近行），然后替换：

old_string（唯一，仅一处 `debugCut`）:
```
debugCut(){let P=c.relays.find(V=>!V.cut);P&&me(P)}};function M(P)
```

new_string:
```
debugCut(){let P=c.relays.find(V=>!V.cut);P&&me(P)},cutRelayById(P){let V=c.relays.find(Y=>Y.id===P);V&&!V.cut&&me(V)},finish(){ee()},respawn(P){n.reset({pos:P||n.pos,yaw:n.yaw}),n.enabled=!0,r.requestLock&&r.requestLock(),o.show(!0),l.show("none"),r.setGameplay&&r.setGameplay(!0),M("playing")}};function M(P)
```

说明：
- `cutRelayById(id)` — 包装内部 `me()`，按中继 id 切断（同步对方进度，触发本方波次/无线电）
- `finish()` — 包装内部 `ee()`，撤离完成
- `respawn(posVec3)` — 复活玩家（`player.reset`）并从 dead 状态回到 playing，任务进度不动（不走 `H()` 全量重置）

- [ ] **Step 3: 确认唯一命中**

Run: `grep -c 'cutRelayById' 几猫国产3A大作_V1.html`
Expected: `1`

---

### Task 3: 客户端联机模块（追加到 HTML 末尾）

**Files:**
- Modify: `几猫国产3A大作_V1.html`（在结尾 `</body>` 前插入）

- [ ] **Step 1: 用 Edit 工具在 `</body>` 前插入联机模块**

old_string:
```
</script>

</body>
</html>
```

new_string（完整联机模块 + 样式）:

```html
</script>

<style id="net-css">
#net-panel{position:fixed;left:50%;bottom:12%;transform:translateX(-50%);z-index:9000;
  display:flex;flex-direction:column;gap:8px;align-items:center;padding:16px 22px;
  background:rgba(8,12,10,.88);border:1px solid rgba(216,240,208,.35);border-radius:6px;
  font:14px/1.5 "PingFang SC",ui-monospace,monospace;color:#d8f0d0;min-width:320px}
#net-panel[hidden]{display:none}
#net-panel .net-title{font-weight:700;letter-spacing:.3em;font-size:15px}
#net-addr{width:100%;box-sizing:border-box;background:rgba(0,0,0,.45);
  border:1px solid rgba(216,240,208,.4);color:#d8f0d0;padding:7px 9px;
  font:13px ui-monospace,monospace;border-radius:3px;outline:none}
#net-join{cursor:pointer;background:#d8f0d0;color:#0a0f0c;border:0;border-radius:3px;
  padding:7px 26px;font:700 14px "PingFang SC",sans-serif;letter-spacing:.2em}
#net-join:disabled{opacity:.45;cursor:default}
#net-status{font-size:12px;opacity:.75;min-height:1.2em}
#net-score{position:fixed;top:14px;left:50%;transform:translateX(-50%);z-index:9000;
  font:700 20px/1 "PingFang SC",ui-monospace,monospace;color:#d8f0d0;
  background:rgba(8,12,10,.7);padding:7px 18px;border-radius:4px;letter-spacing:.15em;
  pointer-events:none}
#net-score[hidden]{display:none}
#net-respawn{position:fixed;inset:0;z-index:10000;display:flex;align-items:center;
  justify-content:center;background:rgba(0,0,0,.55);color:#ff6a3d;
  font:700 34px/1 "PingFang SC",ui-monospace,monospace;letter-spacing:.2em;cursor:default}
#net-respawn[hidden]{display:none}
.net-plate{position:fixed;left:0;top:0;z-index:8000;pointer-events:none;
  transform:translate(-50%,-100%);text-align:center;
  font:600 12px/1.3 "PingFang SC",ui-monospace,monospace;color:#d8f0d0;
  text-shadow:0 1px 3px rgba(0,0,0,.9);will-change:transform}
.net-plate .net-hp{width:64px;height:5px;margin:3px auto 0;background:rgba(0,0,0,.55);
  border:1px solid rgba(216,240,208,.5);border-radius:2px;overflow:hidden}
.net-plate .net-hp i{display:block;height:100%;background:#ff6a3d;width:100%}
</style>
<script id="net-lan">
(()=>{
"use strict";
const iv=setInterval(()=>{
  const G=window.__G;
  if(!(G&&G.game&&G.combat&&G.player&&G.hud&&G.scene&&G.THREE))return;
  clearInterval(iv);
  try{init(G);}catch(e){console.error("[net] init failed",e);}
},100);

function init(G){
  const THREE=G.THREE;
  let ws=null,netActive=false,myId=null,applyingRemote=false;
  let remote=null,score=[{k:0,d:0},{k:0,d:0}],deathAt=0;

  // ---------- UI ----------
  const app=document.getElementById("app");
  const panel=document.createElement("div");
  panel.id="net-panel";
  panel.innerHTML='<div class="net-title">联机对战</div>'+
    '<input id="net-addr" placeholder="ws://192.168.1.5:8080" spellcheck="false">'+
    '<button id="net-join">连接</button><div id="net-status"></div>';
  app.appendChild(panel);
  const addrInput=panel.querySelector("#net-addr");
  const joinBtn=panel.querySelector("#net-join");
  const statusEl=panel.querySelector("#net-status");
  if(location.protocol==="http:"||location.protocol==="https:")
    addrInput.value=(location.protocol==="https:"?"wss://":"ws://")+location.host;

  const scoreboard=document.createElement("div");
  scoreboard.id="net-score";scoreboard.hidden=true;
  app.appendChild(scoreboard);

  const respawnOv=document.createElement("div");
  respawnOv.id="net-respawn";respawnOv.hidden=true;
  document.body.appendChild(respawnOv);

  const syncPanel=()=>{panel.hidden=!(G.game.state==="title"&&!netActive);};
  G.events.on("game:state",syncPanel);syncPanel();
  const updateScore=()=>{scoreboard.textContent=`击杀 ${score[0].k} : ${score[1].k}  死亡 ${score[0].d} : ${score[1].d}`;};

  // ---------- 对方玩家实体 ----------
  function ensureRemote(name){
    if(remote)return;
    const grp=new THREE.Group();
    const body=new THREE.Mesh(
      new THREE.CapsuleGeometry(.4,.9,4,12),
      new THREE.MeshStandardMaterial({color:0xff6a3d,roughness:.6,metalness:.2}));
    body.position.y=.85;body.castShadow=true;
    const nose=new THREE.Mesh(
      new THREE.BoxGeometry(.16,.16,.3),
      new THREE.MeshStandardMaterial({color:0x111111}));
    nose.position.set(0,1.3,-.38);
    grp.add(body);grp.add(nose);
    G.scene.add(grp);
    const plate=document.createElement("div");
    plate.className="net-plate";
    plate.innerHTML=`<span class="net-name">${name||"对手"}</span><div class="net-hp"><i></i></div>`;
    document.body.appendChild(plate);
    remote={grp,plate,hpBar:plate.querySelector(".net-hp i"),
      pos:new THREE.Vector3(),target:new THREE.Vector3(),
      yaw:0,tYaw:0,hp:100,alive:true};
  }
  function destroyRemote(){
    if(!remote)return;
    G.scene.remove(remote.grp);
    remote.plate.remove();
    remote=null;
  }

  // ---------- 协议 ----------
  function send(o){if(ws&&ws.readyState===1)ws.send(JSON.stringify(o));}
  const setStatus=t=>{statusEl.textContent=t;};

  joinBtn.onclick=()=>{
    const url=addrInput.value.trim();
    if(!url){setStatus("请输入服务器地址");return;}
    setStatus("连接中…");joinBtn.disabled=true;
    try{ws=new WebSocket(url);}catch(e){setStatus("地址无效");joinBtn.disabled=false;return;}
    ws.onopen=()=>{send({t:"hello",name:"几猫"});setStatus("已连接，等待对手…");};
    ws.onmessage=ev=>{let m;try{m=JSON.parse(ev.data);}catch{return;}onMsg(m);};
    ws.onerror=()=>{setStatus("连接失败");};
    ws.onclose=()=>{
      const was=netActive;
      netActive=false;ws=null;joinBtn.disabled=false;
      destroyRemote();scoreboard.hidden=true;respawnOv.hidden=true;
      if(was){G.hud.message("连接断开",3);setStatus("连接断开");}
      else setStatus("连接失败");
      syncPanel();
    };
  };

  function onMsg(m){
    switch(m.t){
      case "welcome":myId=m.you;break;
      case "begin":{
        netActive=true;panel.hidden=true;scoreboard.hidden=false;
        score=[{k:0,d:0},{k:0,d:0}];updateScore();
        const names=m.names||[];
        ensureRemote(names[myId===0?1:0]);
        try{G.game.start({skipIntro:!0});}catch(e){console.error("[net] start",e);}
        break;}
      case "states":{
        if(myId===null&&typeof m.you==="number")myId=m.you;
        const o=m[myId===0?"p1":"p0"];
        if(o&&remote&&o.p){
          remote.target.set(o.p[0],o.p[1],o.p[2]);
          remote.tYaw=o.ya||0;
          remote.hp=typeof o.hp==="number"?o.hp:100;
          remote.alive=remote.hp>0;
        }
        if(m.score){score=m.score;updateScore();}
        break;}
      case "hit":{
        if(!netActive||m.from===myId)break;
        applyingRemote=true;
        try{
          const fp=remote?remote.target.clone():undefined;
          G.player.damage(m.dmg||0,fp);
        }finally{applyingRemote=false;}
        break;}
      case "dead":{
        if(m.score){score=m.score;updateScore();}
        if(m.who===myId){
          deathAt=performance.now();respawnOv.hidden=false;
        }else{
          if(remote){remote.alive=false;remote.hp=0;}
          if(m.by==="p"&&G.hud.feed)G.hud.feed("你击杀了对手");
        }
        break;}
      case "respawn":{
        if(m.who===myId){
          respawnOv.hidden=true;
          try{G.game.respawn(new THREE.Vector3(m.pos[0],m.pos[1],m.pos[2]));}
          catch(e){console.error("[net] respawn",e);}
        }else if(remote&&m.pos){
          remote.alive=true;remote.hp=100;
          remote.target.set(m.pos[0],m.pos[1],m.pos[2]);
          remote.pos.copy(remote.target);
        }
        break;}
      case "relay":{
        applyingRemote=true;
        try{G.game.cutRelayById(m.id);}catch(e){console.error("[net] relay",e);}
        applyingRemote=false;
        break;}
      case "extract":{
        applyingRemote=true;
        try{G.hud.message("对手已到达撤离点！",3);G.game.finish();}
        catch(e){console.error("[net] extract",e);}
        applyingRemote=false;
        break;}
      case "leave":
        G.hud.message("对手已离开",3);
        destroyRemote();
        break;
      case "full":setStatus("房间已满");break;
    }
  }

  // ---------- 上报 ----------
  setInterval(()=>{
    if(!netActive||!ws||ws.readyState!==1||!G.player)return;
    const p=G.player.pos;
    send({t:"state",
      p:[+p.x.toFixed(3),+p.y.toFixed(3),+p.z.toFixed(3)],
      ya:+G.player.yaw.toFixed(4),pi:+G.player.pitch.toFixed(4),
      hp:Math.max(0,Math.round(G.player.health))});
  },50);

  G.events.on("player:dead",()=>{
    if(!netActive)return;
    send({t:"dead",by:applyingRemote?"p":"ai"});
    deathAt=performance.now();respawnOv.hidden=false;
  });
  G.events.on("objective:done",e=>{
    if(netActive&&!applyingRemote&&e&&e.id)send({t:"relay",id:e.id});
  });
  G.events.on("extraction:reached",()=>{
    if(netActive&&!applyingRemote)send({t:"extract"});
  });

  // ---------- 射击命中：把对方玩家加入 hitscan ----------
  // ponytail: 攻击方判定（局域网互信）；与物理/敌人取更近者，墙和敌人可挡子弹。
  // 升级路径: 服务器校验射线 + 延迟补偿。
  const orig=G.combat.playerShot.bind(G.combat);
  G.combat.playerShot=(o,d,opts={})=>{
    const res=orig(o,d,opts);
    if(!netActive||!remote||!remote.alive)return res;
    const t=rayCapsule(o,d,
      {x:remote.pos.x,y:remote.pos.y+.4,z:remote.pos.z},
      {x:remote.pos.x,y:remote.pos.y+1.3,z:remote.pos.z},.4);
    const range=opts.range||300;
    if(t===null||t<0||t>range)return res;
    if(res&&res.dist<t)return res;          // 墙/敌人更近
    const point=o.clone().addScaledVector(d,t);
    const normal=d.clone().negate();
    const dmg=opts.damage||28;
    send({t:"hit",dmg});
    if(G.hud.hitmarker)G.hud.hitmarker("hit");
    G.events.emit("impact",{point,normal,dist:t,surface:"enemy",
      enemy:null,part:"body",dir:d,from:"player"});
    return {point,normal,dist:t,surface:"enemy",enemy:null,
      part:"body",dir:d,from:"player"};
  };

  // 射线 vs 胶囊（iq 的 capIntersect 移植；d 需单位向量，返回距离或 null）
  function rayCapsule(o,d,a,b,r){
    const bax=b.x-a.x,bay=b.y-a.y,baz=b.z-a.z;
    const oax=o.x-a.x,oay=o.y-a.y,oaz=o.z-a.z;
    const baba=bax*bax+bay*bay+baz*baz;
    const bard=bax*d.x+bay*d.y+baz*d.z;
    const baoa=bax*oax+bay*oay+baz*oaz;
    const rdoa=d.x*oax+d.y*oay+d.z*oaz;
    const oaoa=oax*oax+oay*oay+oaz*oaz;
    let A=baba-bard*bard;
    let B=baba*rdoa-baoa*bard;
    let C=baba*oaoa-baoa*baoa-r*r*baba;
    let h=B*B-A*C;
    if(h>=0){
      const t=(-B-Math.sqrt(h))/A;
      const y=baoa+t*bard;
      if(t>=0&&y>0&&y<baba)return t;                 // 柱身
      const oc=y<=0?{x:oax,y:oay,z:oaz}
                   :{x:o.x-b.x,y:o.y-b.y,z:o.z-b.z}; // 端帽
      const b2=d.x*oc.x+d.y*oc.y+d.z*oc.z;
      const c2=oc.x*oc.x+oc.y*oc.y+oc.z*oc.z-r*r;
      const h2=b2*b2-c2;
      if(h2>0){
        const t2=-b2-Math.sqrt(h2);
        if(t2>=0)return t2;
      }
    }
    return null;
  }

  // ---------- 渲染循环：插值 + 名牌投影 + 重生倒计时 ----------
  const v=new THREE.Vector3();
  function frame(){
    requestAnimationFrame(frame);
    if(!respawnOv.hidden){
      const left=Math.max(0,RESPAWN_S_UI-(performance.now()-deathAt)/1000);
      respawnOv.textContent=left>0?left.toFixed(1)+"s 后重生":"重生中…";
    }
    if(!remote)return;
    remote.pos.lerp(remote.target,.3);
    let dy=remote.tYaw-remote.yaw;
    while(dy>Math.PI)dy-=Math.PI*2;
    while(dy<-Math.PI)dy+=Math.PI*2;
    remote.yaw+=dy*.3;
    remote.grp.position.copy(remote.pos);
    remote.grp.rotation.y=remote.yaw;
    remote.grp.visible=remote.alive;
    remote.plate.style.display=remote.alive?"block":"none";
    if(remote.alive){
      remote.hpBar.style.width=Math.max(0,Math.min(100,remote.hp))+"%";
      v.set(remote.pos.x,remote.pos.y+2.05,remote.pos.z).project(G.camera);
      if(v.z>1||v.z<-1){remote.plate.style.display="none";}
      else{
        remote.plate.style.transform=
          `translate(-50%,-100%) translate(${(v.x*.5+.5)*innerWidth}px,${(-v.y*.5+.5)*innerHeight}px)`;
      }
    }
  }
  const RESPAWN_S_UI=5;
  requestAnimationFrame(frame);
}
})();
</script>

</body>
</html>
```

- [ ] **Step 2: 校验追加脚本语法**

Run:
```bash
node -e '
const fs=require("fs");
const s=fs.readFileSync("几猫国产3A大作_V1.html","utf8");
const m=s.match(/<script id="net-lan">([\s\S]*?)<\/script>/);
if(!m){console.error("net-lan script not found");process.exit(1);}
fs.writeFileSync("/tmp/net-lan-check.js",m[1]);
' && node --check /tmp/net-lan-check.js && echo SYNTAX_OK
```
Expected: `SYNTAX_OK`

- [ ] **Step 3: 校验内部入口已暴露**

Run: `grep -c 'cutRelayById' 几猫国产3A大作_V1.html`
Expected: `2`（定义 1 处 + 联机模块调用 1 处）

---

### Task 4: 端到端验证

- [ ] **Step 1: 协议自测回归**

Run: `python3 server.py --selftest`
Expected: `SELFTEST PASS`

- [ ] **Step 2: 起服务器 + HTTP 冒烟**

Run: `python3 server.py --port 18080 &` 然后 `curl -s -o /dev/null -w "%{http_code} %{size_download}\n" http://127.0.0.1:18080/`
Expected: `200 1422xxx`（约 1.4MB，字节数≈原 HTML + 追加模块）

- [ ] **Step 3: 双客户端手动验收（用户执行）**

1. 房主：`python3 server.py`，把打印的 `http://IP:8080` 发给对方
2. 两台（或两浏览器窗口）打开地址 → 标题页出现"联机对战"面板 → 连接
3. 双方都连上后自动开局（跳过片头）
4. 验收清单：
   - [ ] 能看到对方玩家（橙色胶囊 + 名牌 + 血条），移动平滑
   - [ ] 互相射击：命中出 hitmarker，掉血名牌变短，红屏受击反馈
   - [ ] 击杀后计分板 +1，被杀方 5s 后原地重生，任务进度不丢
   - [ ] AI 敌人照常刷新攻击；AI 杀死玩家不给对方加分（by=ai）
   - [ ] 切中继双方同步（波次/无线电一致）；一人撤离双方都出结算
   - [ ] 断开一方：另一方收到"对手已离开"，单机部分继续可玩
   - [ ] 不连接直接玩：原版单机流程完全不受影响

---

## Self-Review

1. **Spec 覆盖:** server/HTTP/WS/20Hz/两人房 ✓；hello/state/hit/dead/relay/extract 协议 ✓（`fire` 服务器端转发保留、客户端不发 —— YAGNI，名牌受击反馈已够）；胶囊渲染 ✓；hitscan 接入 ✓；5s 重生 ✓；任务同步 ✓；计分 ✓；断线处理 ✓；selftest ✓；地址输入框 + 默认 location.host ✓。
2. **占位符:** 无 TBD/TODO，所有步骤含完整代码或精确命令。
3. **类型一致性:** `cutRelayById(id)` / `finish()` / `respawn(vec3)` 三处定义与调用一致；消息字段 `t/you/who/from/dmg/p/ya/pi/hp/score/names/id/pos/by` 服务器与客户端拼写一致。
