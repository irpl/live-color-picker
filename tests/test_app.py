import asyncio

import pytest
from fastapi.testclient import TestClient

from app import main


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(main, "hub", main.Hub(idle_timeout=4))
    with TestClient(main.app) as c:
        yield c


def test_pages_served(client):
    assert "pad" in client.get("/").text
    assert "stage" in client.get("/master").text


def test_color_flows_to_master_and_is_recorded(client):
    with client.websocket_connect("/ws/master") as master:
        assert master.receive_json() == {"type": "snapshot", "clients": [], "idle_timeout": 4}
        with client.websocket_connect("/ws/client?id=alice") as a, client.websocket_connect("/ws/client?id=bob") as b:
            assert a.receive_json()["label"] == "Player 1"
            assert b.receive_json()["label"] == "Player 2"
            a.send_json({"color": "#FF0000"})
            assert master.receive_json() == {"type": "color", "id": "alice", "label": "Player 1", "color": "#ff0000"}
            b.send_json({"color": "#00ff00"})
            assert master.receive_json()["id"] == "bob"
            a.send_json({"color": "not-a-color"})  # ignored
            a.send_json({"color": "#0000ff"})
            assert master.receive_json()["color"] == "#0000ff"

            recorded = {c["id"]: c for c in client.get("/api/clients").json()}
            assert recorded["alice"]["first_color"] == "#ff0000"
            assert recorded["alice"]["color"] == "#0000ff"
            assert recorded["bob"]["active"] is True

            # A master joining late sees both active clients in join order.
            with client.websocket_connect("/ws/master") as late:
                snap = late.receive_json()
                assert [c["id"] for c in snap["clients"]] == ["alice", "bob"]
        # Disconnecting clients are removed from the master view.
        leaves = {master.receive_json()["id"], master.receive_json()["id"]}
        assert leaves == {"alice", "bob"}


def test_idle_clients_are_removed_and_rejoin():
    hub = main.Hub(idle_timeout=4)
    events = asyncio.Queue()
    hub.masters.add(events)
    c = hub.get_or_create("x")

    async def scenario():
        await hub.set_color(c, "#123456", now=100)
        await hub.sweep(now=103)
        assert hub.active_order == ["x"]
        await hub.sweep(now=104.1)
        assert hub.active_order == []
        await hub.set_color(c, "#654321", now=110)
        assert hub.active_order == ["x"]
        assert c.first_color == "#123456"

    asyncio.run(scenario())
    types = [events.get_nowait()["type"] for _ in range(events.qsize())]
    assert types == ["color", "leave", "color"]


def test_rejects_bad_client_id(client):
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws/client?id=bad id!") as ws:
            ws.receive_json()
