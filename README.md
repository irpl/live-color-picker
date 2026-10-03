# live-color-picker

Phones pick colors, and one shared "master" screen shows every active picker live.

- **Client** (`/`): one big touch-friendly color pad. Drag a finger across it: left to right changes the hue, top to bottom goes from white to black. Each change goes to the server over a WebSocket, at most about 30 times a second.
- **Master** (`/master`): shows each active client as a tile filled with that client's current color. Every tile gets the same share of the screen. With one client the tile fills the screen. With two the screen splits in half, and so on. Rows and columns are chosen so the tiles stay close to square.
- **Idle removal:** a client that sends no color for `IDLE_TIMEOUT` seconds (4 by default) is removed from the master, and the remaining tiles spread out to fill the screen. Its tile comes back as soon as it picks again. A client that disconnects is removed straight away. A duplicated browser tab gets its own tile.
- **Recording:** the server records each client's first color and its latest color. `GET /api/clients` returns them.

## Run

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open `http://<your-ip>:8000/master` on the big screen and `http://<your-ip>:8000/` on phones on the same network.
Set `IDLE_TIMEOUT=3` (or any number of seconds) to change how long a client can be idle before it is removed.

## Test

```bash
pip install -r requirements-dev.txt
pytest
```

## Notes

- State is kept in memory, so run a single worker process (the uvicorn default). Restarting the server clears it.
- The master page has no authentication. Anyone who knows the URL can open it.

## WebSocket protocol

| Endpoint | Direction | Message |
|---|---|---|
| `/ws/client?id=<tab-id>` | server → client | `{"type":"welcome","id","label","color"}` |
| | client → server | `{"color":"#rrggbb"}` |
| `/ws/master` | server → master | `{"type":"snapshot","clients":[{id,label,color}],"idle_timeout"}` on connect |
| | | `{"type":"color","id","label","color"}` (adds a tile if it is new) |
| | | `{"type":"leave","id"}` |
