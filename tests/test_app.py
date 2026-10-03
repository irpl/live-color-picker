import asyncio
import time

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
        with client.websocket_connect("/ws/client?id=alice&session=s1") as a, client.websocket_connect("/ws/client?id=bob&session=s1") as b:
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
    c = hub.get_or_create("x")

    async def scenario():
        outbox = hub.add_master()
        types = lambda msgs: [m["type"] for m in msgs]
        assert types(await outbox.drain()) == ["snapshot"]
        hub.set_color(c, "#123456", now=100)
        hub.sweep(now=103)
        assert hub.active_order == ["x"]
        assert types(await outbox.drain()) == ["color"]
        hub.sweep(now=104.1)
        assert hub.active_order == []
        assert types(await outbox.drain()) == ["leave"]
        hub.set_color(c, "#654321", now=110)
        assert hub.active_order == ["x"]
        assert c.first_color == "#123456"
        assert types(await outbox.drain()) == ["color"]

    asyncio.run(scenario())


def test_slow_master_only_gets_latest_color_per_client():
    hub = main.Hub()
    a, b = hub.get_or_create("a"), hub.get_or_create("b")

    async def scenario():
        outbox = hub.add_master()
        for i in range(100):
            hub.set_color(a, f"#0000{i:02x}")
        hub.set_color(b, "#ffffff")
        hub.set_color(a, "#abcdef")
        msgs = await outbox.drain()
        assert [(m["type"], m.get("id")) for m in msgs] == [("snapshot", None), ("color", "a"), ("color", "b")]
        assert msgs[1]["color"] == "#abcdef"

    asyncio.run(scenario())


def test_duplicate_tab_gets_its_own_slot(client):
    with client.websocket_connect("/ws/client?id=dup&session=tab1") as first, \
            client.websocket_connect("/ws/client?id=dup&session=tab2") as second:
        w1, w2 = first.receive_json(), second.receive_json()
        assert w1["id"] == "dup"
        assert w2["id"] != "dup" and w2["label"] != w1["label"]


def test_reconnect_from_same_page_takes_over_slot(client):
    from starlette.websockets import WebSocketDisconnect

    with client.websocket_connect("/ws/master") as master:
        master.receive_json()
        with client.websocket_connect("/ws/client?id=p&session=page1") as old:
            assert old.receive_json()["label"] == "Player 1"
            old.send_json({"color": "#111111"})
            assert master.receive_json()["type"] == "color"
            # Old socket not yet noticed as dead; the same page reconnects and takes over.
            with client.websocket_connect("/ws/client?id=p&session=page1") as new:
                welcome = new.receive_json()
                assert (welcome["id"], welcome["label"], welcome["color"]) == ("p", "Player 1", "#111111")
                with pytest.raises(WebSocketDisconnect) as closed:
                    old.receive_json()
                assert closed.value.code == 4000
                new.send_json({"color": "#222222"})
                # The replaced socket closing must not remove the tile.
                assert master.receive_json() == {"type": "color", "id": "p", "label": "Player 1", "color": "#222222"}
                assert main.hub.active_order == ["p"]
            assert master.receive_json() == {"type": "leave", "id": "p"}


def test_rejects_invalid_colors_and_ids(client):
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws/client?id=abc%0A&session=s1") as ws:
            ws.receive_json()
    with client.websocket_connect("/ws/client?id=v&session=s1") as ws:
        ws.receive_json()
        ws.send_json({"color": "#ff0000\n"})
        ws.send_text("[" * 100000 + "]" * 100000)
        ws.send_text("not json")
        ws.send_bytes(b"\x00\x01")
        # Garbage is ignored and the socket stays usable.
        ws.send_json({"color": "#00FF00"})
        for _ in range(200):
            if client.get("/api/clients").json()[0]["color"] is not None:
                break
            time.sleep(0.01)
    assert [c["color"] for c in client.get("/api/clients").json()] == ["#00ff00"]


def test_master_disconnect_is_cleaned_up(client):
    with client.websocket_connect("/ws/master") as master:
        master.receive_json()
        assert len(main.hub.masters) == 1
    with client.websocket_connect("/ws/client?id=z&session=s1") as c:  # forces a round trip after the master left
        c.receive_json()
    assert len(main.hub.masters) == 0


def test_rejects_bad_client_id(client):
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws/client?id=bad id!&session=s1") as ws:
            ws.receive_json()
