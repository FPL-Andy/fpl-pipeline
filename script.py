
import os
import json
from datetime import datetime, timezone

import requests
import pandas as pd
import numpy as np  # följer med pandas

# -----------------------------
# Grundkonfiguration
# -----------------------------
FPL_BASE = "https://fantasy.premierleague.com/api"
ENDPOINT_BOOTSTRAP = f"{FPL_BASE}/bootstrap-static/"
ENDPOINT_FIXTURES  = f"{FPL_BASE}/fixtures/"

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Supabase (REST)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_PLAYERS = os.getenv("SUPABASE_TABLE_PLAYERS", "fpl_players")
TABLE_FIXTURES = os.getenv("SUPABASE_TABLE_FIXTURES", "fpl_fixtures")

SESSION = requests.Session()

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

def http_get_json(url):
    r = SESSION.get(url, timeout=45, headers={"User-Agent": "FPL-Pipeline/1.0"})
    r.raise_for_status()
    return r.json()

def save_json(data, name):
    path = os.path.join(DATA_DIR, f"{name}_{ts()}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path

def supabase_post_json(table: str, json_payload: str):
    """Skicka en FÄRDIG JSON-sträng (array med objekt) till Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❗ No Supabase credentials; skipping DB insert")
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=id"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    resp = SESSION.post(url, headers=headers, data=json_payload, timeout=60)
    ok = "OK" if resp.ok else "ERROR"
    print(f"→ Supabase insert {table}: {resp.status_code} {ok}")
    if not resp.ok:
        print("Response text:", resp.text[:1000])

# -----------------------------
# Hjälpfunktioner (kolumnval + typning + JSON-säkring)
# -----------------------------
def keep_only(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Behåll ENDAST tabellens kolumner. Lägg till saknade som None."""
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = None
    return out[columns]

def coerce_int(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Tvinga kolumner till heltal (nullable Int64) så vi slipper '2.0'."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def coerce_bool(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Tvinga kolumner till boolean (nullable)."""
    for c in cols:
