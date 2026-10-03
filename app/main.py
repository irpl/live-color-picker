"""Live color picker: clients pick colors, a shared display shows them in real time."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import secrets
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path

import segno
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

log = logging.getLogger("live_color_picker")

STATIC_DIR = Path(__file__).parent / "static"
IDLE_TIMEOUT = float(os.environ.get("IDLE_TIMEOUT", "4"))
SWEEP_INTERVAL = 0.25
COLOR_RE = re.compile(r"#[0-9a-fA-F]{6}")
CLIENT_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")
LABEL_RE = re.compile(r"Player ([1-9][0-9]{0,5})")
# Address phones should open. Behind a proxy the request URL can be wrong (http, internal host),
# so a deployment sets this; otherwise the URL the display was opened on is used.
PUBLIC_URL = os.environ.get("PUBLIC_URL", "")


@dataclass
class Client:
    id: str
    label: str
    first_color: str | None = None
    first_color_at: float | None = None
    color: str | None = None
    updated_at: float = 0.0
    active: bool = False
    # The socket currently speaking for this client, and the page-load session it came from.
    socket: WebSocket | None = None
    session: str = ""

    def public(self) -> dict:
        return {"id": self.id, "label": self.label, "color": self.color}


class Outbox:
    """Pending messages for one display socket, coalesced per client.

    Only the newest message per client is kept, so a slow display receives the
    latest state instead of an ever-growing backlog of intermediate colors.
    """

    def __init__(self) -> None:
        self.pending: dict[str, dict] = {}
        self.ready = asyncio.Event()

    def put(self, key: str, message: dict) -> None:
        self.pending[key] = message  # replaces in place, keeping the order clients first appeared
        self.ready.set()

    async def drain(self) -> list[dict]:
        await self.ready.wait()
        self.ready.clear()
        messages, self.pending = list(self.pending.values()), {}
        return messages


@dataclass
class Hub:
    """In-memory state shared by all sockets (single-process deployment)."""

    idle_timeout: float = IDLE_TIMEOUT
    clients: dict[str, Client] = field(default_factory=dict)
    # Active client ids in the order they (re)joined the display.
    active_order: list[str] = field(default_factory=list)
    displays: set[Outbox] = field(default_factory=set)
    _next_label: int = 1

    def get_or_create(self, client_id: str, label_hint: str = "") -> Client:
        client = self.clients.get(client_id)
        if client is None:
            client = Client(id=client_id, label=self._new_label(label_hint))
            self.clients[client_id] = client
        return client

    def _new_label(self, hint: str) -> str:
        # Reuse the label a client had before a server restart, unless someone else has it.
        match = LABEL_RE.fullmatch(hint)
        if match and all(c.label != hint for c in self.clients.values()):
            self._next_label = max(self._next_label, int(match.group(1)) + 1)
            return hint
        label = f"Player {self._next_label}"
        self._next_label += 1
        return label

    def snapshot(self) -> list[dict]:
        return [self.clients[cid].public() for cid in self.active_order]

    def set_color(self, client: Client, color: str, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        if client.first_color is None:
            client.first_color = color
            client.first_color_at = time.time()
        client.color = color
        client.updated_at = now
        if not client.active:
            client.active = True
            self.active_order.append(client.id)
        self.broadcast(client.id, {"type": "color", **client.public()})

    def deactivate(self, client: Client) -> None:
        if not client.active:
            return
        client.active = False
        self.active_order.remove(client.id)
        self.broadcast(client.id, {"type": "leave", "id": client.id})

    def sweep(self, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        for cid in list(self.active_order):
            client = self.clients[cid]
            if now - client.updated_at >= self.idle_timeout:
                self.deactivate(client)

    def broadcast(self, key: str, message: dict) -> None:
        for outbox in self.displays:
            outbox.put(key, message)

    def add_display(self) -> Outbox:
        outbox = Outbox()
        outbox.put("", {"type": "snapshot", "clients": self.snapshot(), "idle_timeout": self.idle_timeout})
        self.displays.add(outbox)
        return outbox


hub = Hub()


async def _sweeper() -> None:
    while True:
        await asyncio.sleep(SWEEP_INTERVAL)
        try:
            hub.sweep()
        except Exception:
            log.exception("idle sweep failed")


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(_sweeper())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="Live Color Picker", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
async def client_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "client.html")


@app.get("/display", include_in_schema=False)
async def display_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "display.html")


@app.get("/join-url", include_in_schema=False)
async def join_url(request: Request) -> dict:
    return {"url": _join_url(request)}


@app.get("/qr.svg", include_in_schema=False)
async def join_qr(request: Request) -> Response:
    """QR code for the phone page, shown on the display so people can join."""
    buf = io.BytesIO()
    segno.make(_join_url(request), error="m").save(
        buf, kind="svg", border=2, dark="#000", light="#fff", xmldecl=False, omitsize=True,
    )
    return Response(buf.getvalue(), media_type="image/svg+xml", headers={"Cache-Control": "no-cache"})


def _join_url(request: Request) -> str:
    return PUBLIC_URL.rstrip("/") + "/" if PUBLIC_URL else str(request.base_url)


@app.get("/api/clients")
async def list_clients() -> list[dict]:
    """Every client seen so far, with the first color they picked and their latest one."""
    return [
        {
            **c.public(),
            "first_color": c.first_color,
            "first_color_at": c.first_color_at,
            "active": c.active,
        }
        for c in hub.clients.values()
    ]


_background: set[asyncio.Task] = set()


async def _close_quietly(ws: WebSocket) -> None:
    with suppress(Exception):
        await ws.close(code=4000)


def _parse_color(text: str | None) -> str | None:
    """Return the color from a client frame, or None for anything malformed (which is ignored)."""
    if not text or len(text) > 256:
        return None
    try:
        msg = json.loads(text)
    except (ValueError, RecursionError):
        return None
    color = msg.get("color") if isinstance(msg, dict) else None
    return color.lower() if isinstance(color, str) and COLOR_RE.fullmatch(color) else None


@app.websocket("/ws/client")
async def client_socket(ws: WebSocket, id: str = "", session: str = "", label: str = "") -> None:
    if not (CLIENT_ID_RE.fullmatch(id) and CLIENT_ID_RE.fullmatch(session)):
        await ws.close(code=1008)
        return
    await ws.accept()
    existing = hub.clients.get(id)
    if existing is not None and existing.socket is not None and existing.session != session:
        # Id is live in another page (e.g. a duplicated browser tab): give this one its own slot.
        id, label = secrets.token_urlsafe(12), ""
    client = hub.get_or_create(id, label)
    # Same page reconnecting before the server noticed its old socket died: take over the slot.
    previous, client.socket, client.session = client.socket, ws, session
    if previous is not None:
        # Close in the background: a half-open old socket must not stall the new one.
        task = asyncio.create_task(_close_quietly(previous))
        _background.add(task)
        task.add_done_callback(_background.discard)
    try:
        await ws.send_json({
            "type": "welcome", "id": client.id, "label": client.label,
            "color": client.color, "idle_timeout": hub.idle_timeout,
        })
        while True:
            frame = await ws.receive()
            if frame["type"] == "websocket.disconnect":
                break
            color = _parse_color(frame.get("text"))
            if color is not None:
                hub.set_color(client, color)
    except (WebSocketDisconnect, OSError, RuntimeError):
        pass
    finally:
        if client.socket is ws:
            client.socket = None
            hub.deactivate(client)


@app.websocket("/ws/display")
async def display_socket(ws: WebSocket) -> None:
    await ws.accept()
    outbox = hub.add_display()

    async def send_loop() -> None:
        while True:
            for message in await outbox.drain():
                await ws.send_json(message)

    async def receive_loop() -> None:
        while True:  # displays only listen; this detects disconnects
            if (await ws.receive())["type"] == "websocket.disconnect":
                return

    tasks = [asyncio.create_task(send_loop()), asyncio.create_task(receive_loop())]
    try:
        # Whichever side fails first (send error or disconnect) ends the session.
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        hub.displays.discard(outbox)
        for task in tasks:
            task.cancel()
        for result in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(result, Exception) and not isinstance(result, (WebSocketDisconnect, OSError)):
                log.warning("display socket ended with %r", result)
