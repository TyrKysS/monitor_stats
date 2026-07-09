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
CONFIG_FILE = DATA_DIR / "user_config.json"
OPTIONS_FILE = DATA_DIR / "options.json"
STATE_FILE = DATA_DIR / "logging_state.json"

PRIORITY_DOMAINS = [
    "sensor", "input_number", "light", "binary_sensor",
    "switch", "climate", "cover", "fan", "media_player",
]
DOMAIN_LABELS = {
    "sensor": "Senzory",
    "input_number": "Input Number (pomocníci)",
    "light": "Světla",
    "binary_sensor": "Binární senzory",
    "switch": "Přepínače",
    "climate": "Klimatizace / topení",
    "cover": "Rolovací prvky",
    "fan": "Ventilátory",
    "media_player": "Přehrávače",
}


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def load_logging_state() -> bool:
    try:
        with open(STATE_FILE) as f:
            return json.load(f).get("active", True)
    except Exception:
        return True


def save_logging_state(active: bool) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump({"active": active}, f)


def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Chyba čtení user_config.json: {e}")
    try:
        with open(OPTIONS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"entities": []}


def save_config(entities: list) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump({"entities": entities}, f, ensure_ascii=False, indent=2)
    logger.info(f"Konfigurace uložena ({len(entities)} entit)")


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------

def domain_to_type(entity_id: str) -> str:
    domain = entity_id.split(".")[0]
    return {"light": "light", "input_number": "input_number"}.get(domain, "sensor")


def get_entity_value(entity_config: dict, state_data: dict | None):
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


def format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / 1024 ** 2:.2f} MB"


# ---------------------------------------------------------------------------
# Core logger
# ---------------------------------------------------------------------------

class EntityLogger:
    def __init__(self, entities: list, logging_active: bool = True):
        self.entities = entities
        self.entity_ids: set = {e["entity_id"] for e in entities}
        self.current_states: dict = {}
        self.logging_active = logging_active
        self.running = True
        self.record_count = self._count_records()

    def _count_records(self) -> int:
        if not CSV_FILE.exists():
            return 0
        try:
            with open(CSV_FILE, "r", encoding="utf-8") as f:
                return max(0, sum(1 for _ in f) - 1)
        except Exception:
            return 0

    def reload_config(self, entities: list) -> None:
        self.entities = entities
        self.entity_ids = {e["entity_id"] for e in entities}
        asyncio.create_task(self._fetch_missing_states())
        logger.info(f"Konfigurace živě obnovena: {self.entity_ids}")

    async def _fetch_missing_states(self) -> None:
        headers = {"Authorization": f"Bearer {SUPERVISOR_TOKEN}"}
        async with aiohttp.ClientSession() as session:
            for ec in self.entities:
                eid = ec["entity_id"]
                if eid not in self.current_states:
                    try:
                        async with session.get(
                            f"{HA_REST_URL}/states/{eid}", headers=headers
                        ) as resp:
                            if resp.status == 200:
                                self.current_states[eid] = await resp.json()
                    except Exception as e:
                        logger.error(f"Chyba načítání {eid}: {e}")

    def write_log_entry(self, trigger_entity_id: str) -> None:
        if not self.logging_active:
            return
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_exists = CSV_FILE.exists()
        try:
            with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if not file_exists:
                    header = ["timestamp"]
                    header += [ec.get("name", ec["entity_id"]) for ec in self.entities]
                    writer.writerow(header)
                row = [timestamp]
                for ec in self.entities:
                    value, _unit = get_entity_value(
                        ec, self.current_states.get(ec["entity_id"])
                    )
                    row.append(value)
                writer.writerow(row)
            self.record_count += 1
        except OSError as e:
            logger.error(f"Chyba zápisu (disk plný?): {e}")

    async def fetch_initial_states(self, session: aiohttp.ClientSession) -> None:
        headers = {"Authorization": f"Bearer {SUPERVISOR_TOKEN}"}
        for ec in self.entities:
            eid = ec["entity_id"]
            try:
                async with session.get(
                    f"{HA_REST_URL}/states/{eid}", headers=headers
                ) as resp:
                    if resp.status == 200:
                        self.current_states[eid] = await resp.json()
                        logger.info(f"Načten stav: {eid}")
                    else:
                        logger.warning(f"Entita nenalezena ({resp.status}): {eid}")
            except Exception as e:
                logger.error(f"Chyba načítání {eid}: {e}")

    async def run_websocket(self) -> None:
        while self.running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                logger.error(f"WebSocket chyba: {e} — znovu za 15 s")
                await asyncio.sleep(15)

    async def _connect_and_listen(self) -> None:
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
                    "id": 1, "type": "subscribe_events",
                    "event_type": "state_changed",
                })
                await ws.receive_json()
                logger.info("Přihlášen k odběru state_changed")
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
                    logger.info(f"Změna: {entity_id} — logging_active={self.logging_active}")
                    self.write_log_entry(entity_id)


# ---------------------------------------------------------------------------
# Helpers for entity picker (config page)
# ---------------------------------------------------------------------------

async def fetch_all_entities() -> list[dict]:
    headers = {"Authorization": f"Bearer {SUPERVISOR_TOKEN}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{HA_REST_URL}/states", headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception as e:
        logger.error(f"Chyba načítání entit: {e}")
    return []


def build_select_options(all_states: list[dict], selected_id: str = "") -> str:
    groups: dict[str, list] = {}
    for state in sorted(all_states, key=lambda s: s["entity_id"]):
        domain = state["entity_id"].split(".")[0]
        groups.setdefault(domain, []).append(state)

    html = '<option value="">— vyberte entitu —</option>'
    ordered = PRIORITY_DOMAINS + [d for d in groups if d not in PRIORITY_DOMAINS]
    for domain in ordered:
        if domain not in groups:
            continue
        label = DOMAIN_LABELS.get(domain, domain)
        html += f'<optgroup label="{label}">'
        for s in groups[domain]:
            eid = s["entity_id"]
            fname = s.get("attributes", {}).get("friendly_name", eid)
            sel = " selected" if eid == selected_id else ""
            fname_esc = fname.replace('"', "&quot;").replace("<", "&lt;")
            eid_esc = eid.replace('"', "&quot;")
            html += (
                f'<option value="{eid_esc}" data-name="{fname_esc}"{sel}>'
                f"{fname} ({eid})</option>"
            )
        html += "</optgroup>"
    return html


def _row_html(options_html: str, selected_id: str = "", name: str = "") -> str:
    safe_name = name.replace('"', "&quot;")
    return (
        f'<div class="entity-row">'
        f'<select name="entity_id" onchange="autoName(this)">{options_html}</select>'
        f'<input name="name" type="text" placeholder="Vlastní název (volitelný)" value="{safe_name}">'
        f'<button type="button" class="btn-rm"'
        f' onclick="this.parentElement.remove()" title="Odebrat">&#215;</button>'
        f"</div>"
    )


# ---------------------------------------------------------------------------
# HTML — INDEX (100 % statická stránka, vše dynamické řeší JS přes /api/status)
# ---------------------------------------------------------------------------

INDEX_HTML = """<!DOCTYPE html>
<html lang="cs">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Entity Logger</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:sans-serif;background:#f0f4f8;color:#333;padding:24px}
    h1{color:#03a9f4;margin-bottom:6px}
    nav{margin-bottom:20px}
    nav a{display:inline-block;padding:7px 16px;border-radius:6px;
          text-decoration:none;color:#555;font-size:14px}
    nav a.active{background:#03a9f4;color:#fff}
    nav a:hover:not(.active){background:#e0e0e0}
    h2{margin:24px 0 10px;color:#555;font-size:16px}
    .notice{background:#fff3e0;border-left:4px solid #ff9800;
            padding:12px 16px;border-radius:6px;margin-bottom:20px;display:none}
    .notice a{color:#e65100}
    .status-bar{display:flex;align-items:center;gap:14px;flex-wrap:wrap;
                background:#fff;border-radius:10px;padding:14px 20px;
                margin-bottom:20px;box-shadow:0 2px 6px rgba(0,0,0,.08)}
    .status-ind{font-size:15px;font-weight:700;flex:1}
    .status-ind.active{color:#43a047}
    .status-ind.stopped{color:#ef5350}
    .status-ind.connecting{color:#fb8c00}
    .cards{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:20px}
    .card{background:#fff;border-radius:10px;padding:16px 22px;
          box-shadow:0 2px 6px rgba(0,0,0,.08);min-width:150px}
    .card .label{font-size:11px;color:#888;text-transform:uppercase;letter-spacing:.5px}
    .card .value{font-size:26px;font-weight:700;margin-top:4px;color:#03a9f4}
    .card .value.warn{color:#f57c00}
    .btns{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:24px}
    .btn{display:inline-flex;align-items:center;gap:7px;padding:10px 18px;
         border-radius:8px;text-decoration:none;color:#fff;font-size:14px;
         border:none;cursor:pointer;font-family:inherit;font-weight:600;
         transition:filter .15s}
    .btn:disabled{opacity:.5;cursor:not-allowed}
    .btn:not(:disabled):hover{filter:brightness(.88)}
    .btn-start{background:#43a047}
    .btn-stop{background:#ef5350}
    .btn-dl{background:#03a9f4}
    .btn-clear{background:#757575}
    table{width:100%;border-collapse:collapse;background:#fff;border-radius:10px;
          overflow:hidden;box-shadow:0 2px 6px rgba(0,0,0,.08)}
    th{background:#03a9f4;color:#fff;padding:10px 14px;text-align:left;font-weight:600;font-size:13px}
    td{padding:10px 14px;border-bottom:1px solid #eee;font-size:13px}
    tr:last-child td{border-bottom:none}
    .badge{display:inline-block;padding:2px 9px;border-radius:12px;
           font-size:11px;font-weight:700}
    .sensor{background:#e3f2fd;color:#0288d1}
    .light{background:#fff9c4;color:#f9a825}
    .input_number{background:#e8f5e9;color:#388e3c}
    .unavail{color:#bbb;font-style:italic}
    .ts{font-size:11px;color:#aaa;margin-top:6px}
  </style>
</head>
<body>
  <h1>Entity Logger</h1>
  <nav>
    <a href="." class="active">Přehled</a>
    <a href="config">Konfigurace entit</a>
  </nav>

  <div id="notice" class="notice">
    &#9888; Nejsou nastaveny žádné entity.
    <a href="config">Přejděte do Konfigurace</a> a vyberte, co chcete logovat.
  </div>

  <!-- Status + Start/Stop -->
  <div class="status-bar">
    <span id="status-ind" class="status-ind connecting">&#9679; Připojuji se...</span>
    <button id="toggle-btn" class="btn btn-start" onclick="toggleLogging()" disabled>
      &#9654; Spustit logování
    </button>
  </div>

  <!-- Stats -->
  <div class="cards">
    <div class="card">
      <div class="label">Zaznamenaných řádků</div>
      <div class="value" id="stat-records">—</div>
    </div>
    <div class="card">
      <div class="label">Velikost souboru</div>
      <div class="value" id="stat-size">—</div>
    </div>
    <div class="card">
      <div class="label">Volné místo na disku</div>
      <div class="value" id="stat-disk">—</div>
    </div>
    <div class="card">
      <div class="label">Monitorované entity</div>
      <div class="value" id="stat-entities">—</div>
    </div>
  </div>

  <!-- Actions -->
  <div class="btns">
    <a class="btn btn-dl" href="download">&#8675; Stáhnout CSV</a>
    <a class="btn btn-clear" href="clear"
       onclick="return confirm('Opravdu vymazat všechny záznamy?')">&#128465; Vymazat záznamy</a>
  </div>

  <!-- Live entity table -->
  <h2>Aktuální stav entit <span class="ts" id="last-update"></span></h2>
  <table>
    <thead>
      <tr>
        <th>Entity ID</th>
        <th>Název</th>
        <th>Typ</th>
        <th>Aktuální hodnota</th>
      </tr>
    </thead>
    <tbody id="entity-tbody">
      <tr><td colspan="4" style="color:#bbb;text-align:center;padding:20px">Načítám...</td></tr>
    </tbody>
  </table>

  <script>
    let isLogging = null;
    let toggling = false;

    function esc(s) {
      return String(s)
        .replace(/&/g,'&amp;').replace(/</g,'&lt;')
        .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    }

    async function refresh() {
      let d;
      try {
        d = await fetch('api/status').then(r => r.json());
      } catch(e) {
        document.getElementById('status-ind').textContent = '⚠ Nelze se připojit k doplňku';
        document.getElementById('status-ind').className = 'status-ind stopped';
        return;
      }

      // Notice
      document.getElementById('notice').style.display =
        d.entities.length === 0 ? 'block' : 'none';

      // Status indicator + toggle button
      const ind = document.getElementById('status-ind');
      const btn = document.getElementById('toggle-btn');
      isLogging = d.logging;
      if (d.logging) {
        ind.textContent = '● Logování aktivní';
        ind.className = 'status-ind active';
        btn.innerHTML = '■ Zastavit logování';
        btn.className = 'btn btn-stop';
      } else {
        ind.textContent = '■ Logování zastaveno';
        ind.className = 'status-ind stopped';
        btn.innerHTML = '► Spustit logování';
        btn.className = 'btn btn-start';
      }
      if (!toggling) btn.disabled = false;

      // Stats
      document.getElementById('stat-records').textContent =
        d.record_count.toLocaleString('cs-CZ');
      const sizeEl = document.getElementById('stat-size');
      sizeEl.textContent = d.file_size;
      sizeEl.className = 'value' + (d.file_size_warn ? ' warn' : '');
      const diskEl = document.getElementById('stat-disk');
      diskEl.textContent = d.disk_free;
      diskEl.className = 'value' + (d.disk_warn ? ' warn' : '');
      document.getElementById('stat-entities').textContent = d.entities.length;

      // Entity table
      const tbody = document.getElementById('entity-tbody');
      if (d.entities.length === 0) {
        tbody.innerHTML =
          '<tr><td colspan="4" style="color:#bbb;text-align:center;padding:20px">' +
          'Žádné entity &mdash; nastavte v <a href="config">Konfiguraci</a></td></tr>';
      } else {
        tbody.innerHTML = d.entities.map(function(e) {
          var val;
          if (e.value === 'unavailable') {
            val = '<span class="unavail">nedostupná</span>';
          } else {
            val = '<b>' + esc(e.value) + '</b>' + (e.unit ? ' ' + esc(e.unit) : '');
          }
          return '<tr>' +
            '<td><code>' + esc(e.entity_id) + '</code></td>' +
            '<td>' + esc(e.name) + '</td>' +
            '<td><span class="badge ' + esc(e.type) + '">' + esc(e.type) + '</span></td>' +
            '<td>' + val + '</td>' +
            '</tr>';
        }).join('');
      }

      // Timestamp
      const now = new Date();
      document.getElementById('last-update').textContent =
        'Aktualizováno ' + now.toLocaleTimeString('cs-CZ');
    }

    async function toggleLogging() {
      if (toggling || isLogging === null) return;
      toggling = true;
      const btn = document.getElementById('toggle-btn');
      btn.disabled = true;
      const action = isLogging ? 'stop' : 'start';
      try {
        await fetch(action);
      } catch(e) {}
      await refresh();
      toggling = false;
    }

    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# HTML — CONFIG PAGE (Python .format() pro entity picker)
# ---------------------------------------------------------------------------

CONFIG_HTML = """\
<!DOCTYPE html>
<html lang="cs">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Entity Logger &ndash; Konfigurace</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:sans-serif;background:#f0f4f8;color:#333;padding:24px}}
    h1{{color:#03a9f4;margin-bottom:6px}}
    nav{{margin-bottom:20px}}
    nav a{{display:inline-block;padding:7px 16px;border-radius:6px;
           text-decoration:none;color:#555;font-size:14px}}
    nav a.active{{background:#03a9f4;color:#fff}}
    nav a:hover:not(.active){{background:#e0e0e0}}
    h2{{margin:0 0 12px;color:#555}}
    p.desc{{color:#666;margin-bottom:20px;font-size:14px;line-height:1.5}}
    .entity-row{{display:flex;gap:10px;align-items:center;background:#fff;
                 border-radius:8px;padding:10px 14px;margin-bottom:8px;
                 box-shadow:0 1px 4px rgba(0,0,0,.07)}}
    .entity-row select{{flex:2;padding:8px 10px;border:1px solid #ccc;
                        border-radius:6px;font-size:13px;min-width:0}}
    .entity-row input{{flex:1;padding:8px 10px;border:1px solid #ccc;
                       border-radius:6px;font-size:13px;min-width:0}}
    .btn-rm{{background:#ef5350;border:none;color:#fff;border-radius:6px;
             padding:8px 13px;cursor:pointer;font-size:18px;line-height:1;
             flex-shrink:0}}
    .btn-rm:hover{{background:#c62828}}
    .col-hdr{{font-size:11px;color:#888;text-transform:uppercase;
              letter-spacing:.5px;flex:1}}
    .header-row{{display:flex;gap:10px;padding:0 14px;margin-bottom:4px}}
    .btn{{display:inline-flex;align-items:center;gap:7px;padding:11px 20px;
          border-radius:8px;text-decoration:none;color:#fff;font-size:14px;
          border:none;cursor:pointer;font-family:inherit;font-weight:600}}
    .btn:hover{{filter:brightness(.88)}}
    .btn-add{{background:#7e57c2;margin-top:14px}}
    .btn-save{{background:#43a047;margin-top:20px}}
    .tip{{font-size:12px;color:#aaa;margin-top:10px}}
  </style>
</head>
<body>
  <h1>Entity Logger</h1>
  <nav>
    <a href=".">Přehled</a>
    <a href="config" class="active">Konfigurace entit</a>
  </nav>

  <h2>Vyberte entity k monitorování</h2>
  <p class="desc">
    Záznam vznikne při <strong>každé změně</strong> libovolné vybrané entity &mdash;
    do CSV se vždy zapíší hodnoty <strong>všech</strong> entit najednou.
    Po uložení se změny projeví okamžitě bez restartu.
  </p>

  <form method="POST" action="config">
    <div class="header-row">
      <span class="col-hdr" style="flex:2">Entita</span>
      <span class="col-hdr">Název v CSV</span>
      <span style="flex:0 0 42px"></span>
    </div>
    <div id="rows">{rows}</div>

    <button type="button" class="btn btn-add" onclick="addRow()">+ Přidat entitu</button>
    <br>
    <button type="submit" class="btn btn-save">&#10003; Uložit konfiguraci</button>
    <p class="tip">Změny se projeví okamžitě &mdash; bez restartu doplňku.</p>
  </form>

  <script>
    const OPTS = `{options_html}`;

    function makeRow(selectedId, name) {{
      const d = document.createElement('div');
      d.className = 'entity-row';
      d.innerHTML =
        '<select name="entity_id" onchange="autoName(this)">' + OPTS + '</select>' +
        '<input name="name" type="text" placeholder="Vlastní název (volitelný)"' +
          ' value="' + (name || '').replace(/"/g, '&quot;') + '">' +
        '<button type="button" class="btn-rm"' +
          ' onclick="this.parentElement.remove()" title="Odebrat">&#215;</button>';
      if (selectedId) d.querySelector('select').value = selectedId;
      return d;
    }}

    function addRow(sel, name) {{
      document.getElementById('rows').appendChild(makeRow(sel || '', name || ''));
    }}

    function autoName(sel) {{
      const inp = sel.parentElement.querySelector('input[name="name"]');
      if (!inp.value && sel.value) {{
        inp.value = sel.options[sel.selectedIndex].dataset.name || sel.value;
      }}
    }}
  </script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def build_routes(entity_logger: EntityLogger):
    routes = web.RouteTableDef()

    # -- Index (static HTML, JS fetches /api/status) -----------------------
    @routes.get("/")
    async def index(request):
        return web.Response(text=INDEX_HTML, content_type="text/html")

    # -- JSON status API ---------------------------------------------------
    @routes.get("/api/status")
    async def api_status(request):
        file_size, file_size_warn = "—", False
        if CSV_FILE.exists():
            sz = CSV_FILE.stat().st_size
            file_size = format_size(sz)
            file_size_warn = sz > 500 * 1024 * 1024
        disk_free, disk_warn = "N/A", False
        try:
            _, _, free = shutil.disk_usage("/data")
            disk_free = format_size(free)
            disk_warn = free < 100 * 1024 * 1024
        except Exception:
            pass

        entities = []
        for ec in entity_logger.entities:
            value, unit = get_entity_value(
                ec, entity_logger.current_states.get(ec["entity_id"])
            )
            entities.append({
                "entity_id": ec["entity_id"],
                "name": ec.get("name", ec["entity_id"]),
                "type": ec.get("type", "sensor"),
                "value": str(value),
                "unit": unit,
            })

        return web.json_response({
            "logging": entity_logger.logging_active,
            "record_count": entity_logger.record_count,
            "file_size": file_size,
            "file_size_warn": file_size_warn,
            "disk_free": disk_free,
            "disk_warn": disk_warn,
            "entities": entities,
        })

    # -- Start / Stop ------------------------------------------------------
    @routes.get("/start")
    async def start_logging(request):
        entity_logger.logging_active = True
        save_logging_state(True)
        logger.info("Logování spuštěno")
        return web.json_response({"ok": True, "logging": True})

    @routes.get("/stop")
    async def stop_logging(request):
        entity_logger.logging_active = False
        save_logging_state(False)
        logger.info("Logování zastaveno")
        return web.json_response({"ok": True, "logging": False})

    # -- Download / Clear --------------------------------------------------
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
        raise web.HTTPFound(".")

    # -- Config page -------------------------------------------------------
    @routes.get("/config")
    async def config_page(request):
        all_states = await fetch_all_entities()
        rows_html = ""
        if entity_logger.entities:
            for ec in entity_logger.entities:
                opts = build_select_options(all_states, ec["entity_id"])
                rows_html += _row_html(opts, ec["entity_id"], ec.get("name", ""))
        else:
            rows_html = _row_html(build_select_options(all_states))

        js_opts = (
            build_select_options(all_states)
            .replace("\\", "\\\\")
            .replace("`", "\\`")
            .replace("</", "<\\/")
        )
        html = CONFIG_HTML.format(rows=rows_html, options_html=js_opts)
        return web.Response(text=html, content_type="text/html")

    @routes.post("/config")
    async def config_save(request):
        data = await request.post()
        entity_ids = data.getall("entity_id", [])
        names = data.getall("name", [])
        entities, seen = [], set()
        for i, eid in enumerate(entity_ids):
            eid = eid.strip()
            if not eid or eid in seen:
                continue
            seen.add(eid)
            name = names[i].strip() if i < len(names) else ""
            entities.append({
                "entity_id": eid,
                "name": name or eid,
                "type": domain_to_type(eid),
            })
        save_config(entities)
        entity_logger.reload_config(entities)
        raise web.HTTPFound(".")

    return routes


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    config = load_config()
    logging_active = load_logging_state()
    entity_logger = EntityLogger(config.get("entities", []), logging_active)

    app = web.Application()
    app.add_routes(build_routes(entity_logger))

    port = int(os.environ.get("INGRESS_PORT", 8099))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web UI běží na portu {port}, logging_active={logging_active}")

    asyncio.create_task(entity_logger.run_websocket())

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
