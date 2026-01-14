
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
# Hjälpfunktioner för typning
# -----------------------------
def keep_only(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Behåll ENDAST tabellens kolumner. Lägg till saknade som None."""
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = None
    return out[columns]

def coerce_int(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Tvinga kolumner till heltal (nullable Int64)."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def coerce_bool(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Tvinga kolumner till boolean (nullable)."""
    for c in cols:
        if c in df.columns:
            # till booleans via sannings-tabell
            df[c] = df[c].map({True: True, False: False, 1: True, 0: False}).astype("boolean")
    return df

def coerce_ts_iso(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Gör tidskolumn till ISO-sträng (UTC) eller None."""
    if col in df.columns:
        dt = pd.to_datetime(df[col], errors="coerce", utc=True)
        # ISO med 'Z' på slutet; None för NaT
        df[col] = dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        df.loc[dt.isna(), col] = None
    return df

def df_to_json_payload(df: pd.DataFrame) -> str:
    """
    Gör df JSON-säker och returnera färdig JSON-sträng (lista med objekt):
    - ersätt NaN/NaT med None
    - serialisera till ISO-strängar där det behövs
    - allow_nan=False garanterar giltig JSON (inga NaN/Inf)
    """
    out = df.where(pd.notnull(df), None)
    return out.to_json(orient="records", date_format="iso", allow_nan=False)

# -----------------------------
# Huvudflöde
# -----------------------------
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
    players_clean = keep_only(players_df, players_allowed)
    players_clean = coerce_int(players_clean, ["id", "team", "now_cost", "total_points", "minutes"])
    # övriga får vara text/float som JSON – PostgREST mappar fint
    players_payload = df_to_json_payload(players_clean)

    # -------- FIXTURES --------
    fixtures = http_get_json(ENDPOINT_FIXTURES)
    save_json(fixtures, "fixtures")

    fixtures_df = pd.json_normalize(fixtures)
    print(f"Fixtures in source: {len(fixtures_df)}")

    # Skicka EJ 'stats' (stabilitet först)
    fixtures_allowed = [
        "id", "event", "team_h", "team_a", "team_h_score", "team_a_score",
        "kickoff_time", "finished", "started", "minutes"
    ]
    fixtures_clean = keep_only(fixtures_df, fixtures_allowed)
    fixtures_clean = coerce_int(fixtures_clean,
        ["id", "event", "team_h", "team_a", "team_h_score", "team_a_score", "minutes"]
    )
    fixtures_clean = coerce_bool(fixtures_clean, ["finished", "started"])
    fixtures_clean = coerce_ts_iso(fixtures_clean, "kickoff_time")

    # (debug) – ta bort om du vill när det funkar
    print("Fixtures dtypes:", fixtures_clean.dtypes.to_dict())

    fixtures_payload = df_to_json_payload(fixtures_clean)

    # -------- WRITE TO SUPABASE --------
    supabase_post_json(TABLE_PLAYERS, players_payload)
    supabase_post_json(TABLE_FIXTURES, fixtures_payload)

    print("== FPL pipeline end ==")

if __name__ == "__main__":
    main()
