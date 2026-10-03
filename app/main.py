"""Live color picker: clients pick colors, a master screen shows them in real time."""

from __future__ import annotations

import asyncio
import os
import re
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

STATIC_DIR = Path(__file__).parent / "static"
IDLE_TIMEOUT = float(os.environ.get("IDLE_TIMEOUT", "4"))
SWEEP_INTERVAL = 0.25
COLOR_RE = re.compile(r"^#[0-9a-fA-F]{6}$")
CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


@dataclass
class Client:
    id: str
    label: str
    first_color: str | None = None
    first_color_at: float | None = None
    color: str | None = None
    updated_at: float = 0.0
    active: bool = False
    connections: int = 0

    def public(self) -> dict:
        return {"id": self.id, "label": self.label, "color": self.color}


@dataclass
class Hub:
    """In-memory state shared by all sockets (single-process deployment)."""

    idle_timeout: float = IDLE_TIMEOUT
    clients: dict[str, Client] = field(default_factory=dict)
    # Active client ids in the order they (re)joined the master display.
    active_order: list[str] = field(default_factory=list)
    # One outgoing queue per master socket, so a slow master never blocks clients.
    masters: set[asyncio.Queue] = field(default_factory=set)
    _next_label: int = 1

    def get_or_create(self, client_id: str) -> Client:
        client = self.clients.get(client_id)
        if client is None:
            client = Client(id=client_id, label=f"Player {self._next_label}")
            self._next_label += 1
            self.clients[client_id] = client
        return client

    def snapshot(self) -> list[dict]:
        return [self.clients[cid].public() for cid in self.active_order]

    async def set_color(self, client: Client, color: str, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        if client.first_color is None:
            client.first_color = color
            client.first_color_at = time.time()
        client.color = color
        client.updated_at = now
        if not client.active:
            client.active = True
            self.active_order.append(client.id)
        await self.broadcast({"type": "color", **client.public()})

    async def deactivate(self, client: Client) -> None:
        if not client.active:
            return
        client.active = False
        self.active_order.remove(client.id)
        await self.broadcast({"type": "leave", "id": client.id})

    async def sweep(self, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        for cid in list(self.active_order):
            client = self.clients[cid]
            if now - client.updated_at >= self.idle_timeout:
                await self.deactivate(client)

    async def broadcast(self, message: dict) -> None:
        for queue in self.masters:
            queue.put_nowait(message)

    def add_master(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait({"type": "snapshot", "clients": self.snapshot(), "idle_timeout": self.idle_timeout})
        self.masters.add(queue)
        return queue


hub = Hub()


async def _sweeper() -> None:
    while True:
        await asyncio.sleep(SWEEP_INTERVAL)
        await hub.sweep()


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


@app.get("/master", include_in_schema=False)
async def master_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "master.html")


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


@app.websocket("/ws/client")
async def client_socket(ws: WebSocket, id: str = "") -> None:
    if not CLIENT_ID_RE.match(id):
        await ws.close(code=1008)
        return
    await ws.accept()
    client = hub.get_or_create(id)
    client.connections += 1
    await ws.send_json({"type": "welcome", "id": client.id, "label": client.label, "color": client.color})
    try:
        while True:
            msg = await ws.receive_json()
            color = msg.get("color") if isinstance(msg, dict) else None
            if isinstance(color, str) and COLOR_RE.match(color):
                await hub.set_color(client, color.lower())
    except (WebSocketDisconnect, ValueError):
        pass
    finally:
        client.connections -= 1
        if client.connections == 0:
            await hub.deactivate(client)


@app.websocket("/ws/master")
async def master_socket(ws: WebSocket) -> None:
    await ws.accept()
    queue = hub.add_master()

    async def pump() -> None:
        while True:
            await ws.send_json(await queue.get())

    sender = asyncio.create_task(pump())
    try:
        while True:
            await ws.receive_text()  # masters only listen; this detects disconnects
    except WebSocketDisconnect:
        pass
    finally:
        hub.masters.discard(queue)
        sender.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await sender
