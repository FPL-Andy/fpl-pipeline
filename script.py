
import os
import json
from datetime import datetime, timezone

import requests
import pandas as pd
import numpy as np

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

def supabase_insert(table: str, records: list[dict]):
    """Skicka LISTA av dictar till Supabase. Loggar status + feltext."""
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
    payload = json.dumps(records, allow_nan=False)  # stoppa NaN/Inf
    resp = SESSION.post(url, headers=headers, data=payload, timeout=60)
    ok = "OK" if resp.ok else "ERROR"
    print(f"→ Supabase insert {table}: {resp.status_code} {ok}")
    if not resp.ok:
        print("Response text:", resp.text[:1000])

def filter_columns(df: pd.DataFrame, allowed: list[str]) -> pd.DataFrame:
    """Behåll ENBART kolumner som finns i tabellen. Lägg till saknade som None."""
    out = df.copy()
    for c in allowed:
        if c not in out.columns:
            out[c] = None
    return out[allowed]

def clean_df_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """Gör df JSON-säker: ersätt ±Inf/NaN med None."""
    out = df.replace([np.inf, -np.inf], np.nan)
    out = out.where(pd.notnull(out), None)
    return out

def main():
    print(">>> script.py startar...", flush=True)
    print("== FPL pipeline start ==")

    # -------- PLAYERS --------
    bootstrap = http_get_json(ENDPOINT_BOOTSTRAP)
    save_json(bootstrap, "bootstrap")

    players_df = pd.json_normalize(bootstrap.get("elements", []))
    print(f"Players in source: {len(players_df)}")

    players_allowed = [
        "id", "first_name", "second_name", "web_name", "team",
        "now_cost", "total_points", "selected_by_percent", "minutes",
        "form", "ep_next", "ep_this"
    ]
    players_clean = filter_columns(players_df, players_allowed)
    players_clean = clean_df_for_json(players_clean)
    players_records = players_clean.to_dict(orient="records")

    # -------- FIXTURES --------
    fixtures = http_get_json(ENDPOINT_FIXTURES)
    save_json(fixtures, "fixtures")

    fixtures_df = pd.json_normalize(fixtures)
    print(f"Fixtures in source: {len(fixtures_df)}")

    # NOTE: Vi tar bort 'stats' för att hålla det stabilt (kan innehålla ogiltiga värden).
    fixtures_allowed = [
        "id", "event", "team_h", "team_a", "team_h_score", "team_a_score",
        "kickoff_time", "finished", "started", "minutes"
        # "stats"  <-- borttagen för enkelhet och stabilitet
    ]
    fixtures_clean = filter_columns(fixtures_df, fixtures_allowed)
    fixtures_clean = clean_df_for_json(fixtures_clean)
    fixtures_records = fixtures_clean.to_dict(orient="records")

    # -------- WRITE TO SUPABASE --------
    supabase_insert(TABLE_PLAYERS, players_records)
    supabase_insert(TABLE_FIXTURES, fixtures_records)

    print("== FPL pipeline end ==")

if __name__ == "__main__":
    main()
    
