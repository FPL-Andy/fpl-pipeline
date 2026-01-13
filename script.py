
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
    "User-Agent": "FPL-Pipeline/1.0 (+https://github.com/yourname/fpl-pipeline)"
})

# -----------------------------
# Hjälpfunktioner
