
import os
import json
import time
from datetime import datetime, timezone

import requests
import pandas as pd

# -----------------------------
# Grundkonfiguration
# -----------------------------
FPL_BASE = "https://fantasy.premierleague.com/api"
ENDPOINT_BOOTSTRAP = f"{FPL_BASE}/bootstrap-static/"
ENDPOINT_FIXTURES = f"{FPL_BASE}/fixtures/"

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Supabase (valfritt – om dessa inte finns hoppar vi över DB-insättning)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA", "public")
TABLE_PLAYERS = os.getenv("SUPABASE_TABLE_PLAYERS", "fpl_players")
TABLE_FIXTURES = os.getenv("SUPABASE_TABLE_FIXTURES", "fpl_fixtures")
TABLE_EVENTS = os.getenv("SUPABASE_TABLE_EVENTS", "fpl_events_live")

SESSION = requests.Session()
SESSION.headers.update({
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
})

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

def http_get_json(url):
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def save_json(data, name):
    path = f"data/{name}_{ts()}.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return path

def supabase_upsert(table, rows):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("No Supabase credentials")
        return

    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=id"
    resp = SESSION.post(url, data=json.dumps(rows), timeout=60)
    print(f"Supabase insert to {table}: {resp.status_code} {resp.text}")

def main():
    print("== FPL pipeline start ==")

    # ----- PLAYERS / BOOTSTRAP -----
    bootstrap = http_get_json(ENDPOINT_BOOTSTRAP)
    save_json(bootstrap, "bootstrap")

    players = pd.json_normalize(bootstrap["elements"])
    players_records = players.where(pd.notnull(players), None).to_dict(orient="records")

    print(f"Players found: {len(players_records)}")
    supabase_upsert(TABLE_PLAYERS, players_records)

    # ----- FIXTURES -----
    fixtures = http_get_json(ENDPOINT_FIXTURES)
    save_json(fixtures, "fixtures")

    fixtures_df = pd.json_normalize(fixtures)
    fixtures_records = fixtures_df.where(pd.notnull(fixtures_df), None).to_dict(orient="records")

    print(f"Fixtures found: {len(fixtures_records)}")
    supabase_upsert(TABLE_FIXTURES, fixtures_records)

    print("== FPL pipeline end ==")

if __name__ == "__main__":
    main()
