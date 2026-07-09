# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Home Assistant add-on ("Entity Logger") published via `repository.yaml` as a Home Assistant add-on repository. The add-on logs the state/value of user-selected HA entities to a CSV file whenever any of them changes, and exposes a small web UI (served through HA Ingress) to configure which entities to track, view live values, start/stop logging, and download/clear the CSV.

There is no build system, package manager, or test suite — it's a single-file Python service (`monitor_stats/main.py`, ~785 lines) packaged as a Docker container per HA add-on conventions.

## Running / developing

- The add-on is built and run by the Home Assistant Supervisor from `monitor_stats/Dockerfile` (python:3.11-alpine, installs `monitor_stats/requirements.txt`, runs `python3 main.py`).
- There's no local dev server script — the app depends on Supervisor-provided env vars and network access:
  - `SUPERVISOR_TOKEN` — auth token for the Supervisor REST/WebSocket API.
  - `INGRESS_PORT` — port the web UI binds to (defaults to 8099).
- To iterate, install the repo in a Home Assistant instance (Settings → Add-ons → Add-on Store → repositories) and rebuild/restart the add-on there, or bump `version` in `monitor_stats/config.yaml` when publishing a change (HA requires a version bump for Supervisor to pick up updates).
- No linter/formatter/test commands are configured in this repo.

## Architecture (`monitor_stats/main.py`)

Everything lives in one file, organized top-to-bottom into clear sections:

- **Persistence helpers** — three files under `/data` (the HA add-on's persistent volume):
  - `options.json` — entity list as set via the add-on's Supervisor-managed config schema (fallback).
  - `user_config.json` — entity list as saved through the add-on's own `/config` web form; takes precedence over `options.json` when present (`load_config()`).
  - `logging_state.json` — whether logging is currently active/paused, persisted across restarts.
  - `entity_log.csv` — the append-only output log.
- **`EntityLogger`** — the core stateful object. Holds the configured entity list, a `current_states` cache (entity_id → last known HA state payload), and `logging_active`. Two live data paths feed it:
  1. `fetch_initial_states()` — REST call to `{HA_REST_URL}/states/<id>` for each configured entity on (re)connect.
  2. `run_websocket()` / `_connect_and_listen()` — long-lived WebSocket connection to `ws://supervisor/core/api/websocket`, subscribed to `state_changed` events. On every change to a tracked entity, `write_log_entry()` appends one CSV row **per configured entity** (not just the one that changed) — the whole entity set's current values are snapshotted together, tagged with which entity triggered the write.
  - Auto-reconnects with a 15s backoff on WebSocket failure (`run_websocket`'s outer loop).
  - `reload_config()` lets the config change take effect live (no process restart) — it swaps the entity list and kicks off `_fetch_missing_states()` for any newly-added entities.
- **Value formatting** — `get_entity_value()` special-cases `light` entities (converts `brightness` 0–255 to a 0–100% value) and falls back to raw `state` + `unit_of_measurement` attribute for everything else. `domain_to_type()` maps an entity_id's domain to one of `sensor` / `input_number` / `light` for display purposes.
- **Web UI / routes** (`build_routes`, aiohttp `RouteTableDef`) — two HTML pages defined as Python string templates, not files or a templating engine:
  - `INDEX_HTML` — fully static; all dynamic content is filled client-side via `fetch('api/status')` polled every 3s (vanilla JS, no framework).
  - `CONFIG_HTML` — uses Python `.format()` with double-braced CSS (`{{`/`}}`) since it's an f-string-style template; server-side renders the initial `<select>` options and existing entity rows, and also injects a JS-escaped copy of the options HTML (`options_html`) so new rows can be added client-side without a round trip.
  - Key routes: `GET /` (index), `GET /api/status` (JSON snapshot: logging state, record count, file size, disk free, live entity values), `GET /start` `GET /stop` (toggle logging, persisted to `logging_state.json`), `GET /download` (CSV as attachment), `GET /clear` (delete CSV), `GET/POST /config` (entity picker — POST saves to `user_config.json` and calls `reload_config()`).
  - Routes are built inside `build_routes(entity_logger)` as a closure so handlers can mutate the single shared `EntityLogger` instance.
- Everything is in Czech (log messages, UI copy, domain labels) — match that when touching user-facing strings or log output in this file.

## Add-on metadata

- `monitor_stats/config.yaml` defines the add-on's HA-facing schema (`options`/`schema` for the entity list used as the pre-`user_config.json` default), ingress settings, and `version` — bump this on every release.
- `repository.yaml` is the top-level Home Assistant add-on repository manifest (name/url/maintainer) that lets users add this repo as an add-on source in HA.
