#!/usr/bin/env python3
import asyncio
import json
import csv
import os
import logging
import shutil
from datetime import datetime
from pathlib import Path

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

SUPERVISOR_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
HA_WS_URL = "ws://supervisor/core/api/websocket"
HA_REST_URL = "http://supervisor/core/api"
DATA_DIR = Path("/data")
CSV_FILE = DATA_DIR / "entity_log.csv"
OPTIONS_FILE = DATA_DIR / "options.json"


def load_options():
    try:
        with open(OPTIONS_FILE) as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Chyba při načítání options.json: {e}")
        return {"entities": []}


def get_entity_value(entity_config, state_data):
    """Vrátí (hodnota, jednotka) pro danou entitu."""
    if state_data is None:
        return "unavailable", ""

    entity_type = entity_config.get("type", "sensor")
    state = state_data.get("state", "unavailable")
    attrs = state_data.get("attributes", {})

    if entity_type == "light":
        brightness = attrs.get("brightness")
        if state == "on" and brightness is not None:
            return round(brightness / 255 * 100, 1), "%"
        elif state == "off":
            return 0, "%"
        return "unavailable", "%"

    unit = attrs.get("unit_of_measurement", "")
    return state, unit


def format_size(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / 1024 ** 2:.2f} MB"


class EntityLogger:
    def __init__(self, options):
        self.entities = options.get("entities", [])
        self.entity_ids = {e["entity_id"] for e in self.entities}
        self.current_states: dict = {}
        self.running = True
        self.record_count = self._count_records()

    def _count_records(self):
        if not CSV_FILE.exists():
            return 0
        try:
            with open(CSV_FILE, "r", encoding="utf-8") as f:
                return max(0, sum(1 for _ in f) - 1)
        except Exception:
            return 0

    def write_log_entry(self, trigger_entity_id: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_exists = CSV_FILE.exists()
        try:
            with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow([
                        "timestamp", "trigger_entity",
                        "entity_id", "name", "type", "value", "unit",
                    ])
                for ec in self.entities:
                    value, unit = get_entity_value(ec, self.current_states.get(ec["entity_id"]))
                    writer.writerow([
                        timestamp,
                        trigger_entity_id,
                        ec["entity_id"],
                        ec.get("name", ec["entity_id"]),
                        ec.get("type", "sensor"),
                        value,
                        unit,
                    ])
            self.record_count += len(self.entities)
        except OSError as e:
            logger.error(f"Chyba zápisu (disk plný?): {e}")

    async def fetch_initial_states(self, session: aiohttp.ClientSession):
        headers = {"Authorization": f"Bearer {SUPERVISOR_TOKEN}"}
        for entity_id in self.entity_ids:
            try:
                async with session.get(
                    f"{HA_REST_URL}/states/{entity_id}", headers=headers
                ) as resp:
                    if resp.status == 200:
                        self.current_states[entity_id] = await resp.json()
                        logger.info(f"Načten stav: {entity_id}")
                    else:
                        logger.warning(f"Entita nenalezena ({resp.status}): {entity_id}")
            except Exception as e:
                logger.error(f"Chyba načítání {entity_id}: {e}")

    async def run_websocket(self):
        while self.running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                logger.error(f"WebSocket chyba: {e} — znovu za 15 s")
                await asyncio.sleep(15)

    async def _connect_and_listen(self):
        async with aiohttp.ClientSession() as session:
            await self.fetch_initial_states(session)

            async with session.ws_connect(HA_WS_URL) as ws:
                msg = await ws.receive_json()
                if msg["type"] != "auth_required":
                    raise RuntimeError(f"Čekal auth_required, dostal {msg['type']}")

                await ws.send_json({"type": "auth", "access_token": SUPERVISOR_TOKEN})
                msg = await ws.receive_json()
                if msg["type"] != "auth_ok":
                    raise RuntimeError(f"Autentizace selhala: {msg}")

                logger.info("Připojen k Home Assistant")

                await ws.send_json({
                    "id": 1,
                    "type": "subscribe_events",
                    "event_type": "state_changed",
                })
                await ws.receive_json()
                logger.info("Přihlášen k odběru state_changed událostí")

                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    data = json.loads(msg.data)
                    if data.get("type") != "event":
                        continue

                    event_data = data.get("event", {}).get("data", {})
                    entity_id = event_data.get("entity_id")
                    if entity_id not in self.entity_ids:
                        continue

                    new_state = event_data.get("new_state")
                    if new_state:
                        self.current_states[entity_id] = new_state

                    logger.info(f"Změna: {entity_id} → zapisuji záznam")
                    self.write_log_entry(entity_id)


# ---------------------------------------------------------------------------
# Web UI
# ---------------------------------------------------------------------------

INDEX_HTML = """\
<!DOCTYPE html>
<html lang="cs">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Entity Logger</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:sans-serif;background:#f0f4f8;color:#333;padding:24px}}
    h1{{color:#03a9f4;margin-bottom:20px}}
    h2{{margin:24px 0 10px;color:#555}}
    .cards{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:24px}}
    .card{{background:#fff;border-radius:10px;padding:18px 24px;
           box-shadow:0 2px 6px rgba(0,0,0,.08);min-width:160px}}
    .card .label{{font-size:12px;color:#888;text-transform:uppercase;letter-spacing:.5px}}
    .card .value{{font-size:28px;font-weight:700;margin-top:4px;color:#03a9f4}}
    .card .value.warn{{color:#f57c00}}
    .btns{{display:flex;gap:12px;flex-wrap:wrap}}
    .btn{{display:inline-flex;align-items:center;gap:8px;padding:12px 22px;
          border-radius:8px;text-decoration:none;color:#fff;font-size:15px;
          border:none;cursor:pointer;font-family:inherit}}
    .btn-dl{{background:#03a9f4}}
    .btn-clear{{background:#ef5350}}
    .btn:hover{{filter:brightness(.9)}}
    table{{width:100%;border-collapse:collapse;background:#fff;
           border-radius:10px;overflow:hidden;box-shadow:0 2px 6px rgba(0,0,0,.08)}}
    th{{background:#03a9f4;color:#fff;padding:10px 14px;text-align:left;font-weight:600}}
    td{{padding:10px 14px;border-bottom:1px solid #eee}}
    tr:last-child td{{border-bottom:none}}
    .badge{{display:inline-block;padding:2px 10px;border-radius:12px;
            font-size:12px;font-weight:600}}
    .sensor{{background:#e3f2fd;color:#0288d1}}
    .light{{background:#fff9c4;color:#f9a825}}
    .input_number{{background:#e8f5e9;color:#388e3c}}
    .unavail{{color:#bbb;font-style:italic}}
  </style>
</head>
<body>
  <h1>Entity Logger</h1>

  <div class="cards">
    <div class="card">
      <div class="label">Zaznamenaných řádků</div>
      <div class="value">{record_count}</div>
    </div>
    <div class="card">
      <div class="label">Velikost souboru</div>
      <div class="value {size_class}">{file_size}</div>
    </div>
    <div class="card">
      <div class="label">Volné místo na disku</div>
      <div class="value {disk_class}">{disk_free}</div>
    </div>
    <div class="card">
      <div class="label">Monitorované entity</div>
      <div class="value">{entity_count}</div>
    </div>
  </div>

  <div class="btns" style="margin-bottom:24px">
    <a class="btn btn-dl" href="download">&#8675; Stáhnout CSV</a>
    <a class="btn btn-clear" href="clear"
       onclick="return confirm('Opravdu vymazat všechny záznamy?')">&#128465; Vymazat záznamy</a>
  </div>

  <h2>Aktuální stav entit</h2>
  <table>
    <tr>
      <th>Entity ID</th><th>Název</th><th>Typ</th><th>Aktuální hodnota</th>
    </tr>
    {entity_rows}
  </table>
</body>
</html>
"""


def build_routes(entity_logger: EntityLogger):
    routes = web.RouteTableDef()

    @routes.get("/")
    async def index(request):
        file_size = "—"
        size_class = ""
        if CSV_FILE.exists():
            sz = CSV_FILE.stat().st_size
            file_size = format_size(sz)
            size_class = "warn" if sz > 500 * 1024 * 1024 else ""

        try:
            total, used, free = shutil.disk_usage("/data")
            disk_free = format_size(free)
            disk_class = "warn" if free < 100 * 1024 * 1024 else ""
        except Exception:
            disk_free = "N/A"
            disk_class = ""

        rows = []
        for ec in entity_logger.entities:
            state_data = entity_logger.current_states.get(ec["entity_id"])
            value, unit = get_entity_value(ec, state_data)
            if value == "unavailable":
                val_html = '<span class="unavail">nedostupná</span>'
            else:
                val_html = f"<b>{value}</b> {unit}".strip()
            etype = ec.get("type", "sensor")
            rows.append(
                f"<tr>"
                f"<td><code>{ec['entity_id']}</code></td>"
                f"<td>{ec.get('name', '—')}</td>"
                f"<td><span class='badge {etype}'>{etype}</span></td>"
                f"<td>{val_html}</td>"
                f"</tr>"
            )

        html = INDEX_HTML.format(
            record_count=entity_logger.record_count,
            file_size=file_size,
            size_class=size_class,
            disk_free=disk_free,
            disk_class=disk_class,
            entity_count=len(entity_logger.entities),
            entity_rows="\n".join(rows),
        )
        return web.Response(text=html, content_type="text/html")

    @routes.get("/download")
    async def download(request):
        if not CSV_FILE.exists():
            return web.Response(text="Žádná data k dispozici.", status=404)
        filename = f"entity_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return web.FileResponse(
            CSV_FILE,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @routes.get("/clear")
    async def clear(request):
        if CSV_FILE.exists():
            CSV_FILE.unlink()
            entity_logger.record_count = 0
            logger.info("Log soubor smazán")
        raise web.HTTPFound("/")

    return routes


async def main():
    options = load_options()
    if not options.get("entities"):
        logger.warning("Žádné entity v konfiguraci — upravte nastavení doplňku v HA")

    entity_logger = EntityLogger(options)

    app = web.Application()
    app.add_routes(build_routes(entity_logger))

    port = int(os.environ.get("INGRESS_PORT", 8099))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web UI běží na portu {port}")

    asyncio.create_task(entity_logger.run_websocket())

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
