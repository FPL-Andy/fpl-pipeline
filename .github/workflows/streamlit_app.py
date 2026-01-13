
import os
import requests
import pandas as pd
import streamlit as st

# -----------------------------
# Inställningar & utilities
# -----------------------------
st.set_page_config(page_title="FPL Dashboard", layout="wide")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
SCHEMA = os.getenv("SUPABASE_SCHEMA", "public")

TABLE_PLAYERS = os.getenv("SUPABASE_TABLE_PLAYERS", "fpl_players")
TABLE_FIXTURES = os.getenv("SUPABASE_TABLE_FIXTURES", "fpl_fixtures")
TABLE_EVENTS = os.getenv("SUPABASE_TABLE_EVENTS", "fpl_events_live")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

@st.cache_data(ttl=300)
def sb_select(table: str, limit: int = 10000, order_col: str | None = None, ascending: bool = True) -> pd.DataFrame:
    """
    Enkel SELECT via Supabase PostgREST.
    Cache: 5 min (ttl=300) för att göra dashboarden snärtig.
    """
    if not (SUPABASE_URL and SUPABASE_KEY):
        return pd.DataFrame()

    # PostgREST query
    url = f"{SUPABASE_URL}/rest/v1/{table}?select=*&limit={limit}"
    if order_col:
        order_dir = "asc" if ascending else "desc"
        url += f"&order={order_col}.{order_dir}"

    r = requests.get(url, headers=HEADERS, timeout=30)
    if not r.ok:
        st.error(f"Fel vid hämtning från {table}: {r.status_code} {r.text}")
        return pd.DataFrame()
    return pd.DataFrame(r.json())

def map_positions(players_df: pd.DataFrame) -> pd.DataFrame:
    """
    FPL element_type: 1=GK, 2=DEF, 3=MID, 4=FWD.
    (I bootstrap finns ofta kolumnen 'element_type')
    """
    pos_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
    if "element_type" in players_df.columns:
        players_df = players_df.copy()
        players_df["position"] = players_df["element_type"].map(pos_map).fillna("N/A")
    return players_df

def build_team_lookup(fixtures_df: pd.DataFrame, players_df: pd.DataFrame) -> dict:
    """
    Skapa en lookup {team_id: team_name}. I vår minimala tabell finns inte teamnamn som standard.
    Workaround:
      - Vi uppskattar namn från players_df om kolumnen 'team' finns (id) och 'web_name' etc.
      - Om du vill ha EXAKTA namn, kan vi spara 'teams' från bootstrap i egen tabell och joina här.
    Här använder vi placeholders 'Team 1'..'Team 20' om inget bättre finns.
    """
    # Skapa default
    team_ids = set()
    if "team" in players_df.columns:
        team_ids.update(players_df["team"].dropna().unique().tolist())
    for col in ("team_h", "team_a"):
        if col in fixtures_df.columns:
            team_ids.update(pd.Series(fixtures_df[col]).dropna().unique().tolist())
    team_ids = sorted([int(t) for t in team_ids if pd.notnull(t)])

    lookup = {tid: f"Team {tid}" for tid in team_ids}

    # Om du senare sparar 'teams' i en tabell, läs in den här:
    # teams_df = sb_select("fpl_teams")  # exempel
    # lookup = dict(zip(teams_df["id"], teams_df["name"]))

    return lookup

def add_team_names_to_fixtures(fixtures_df: pd.DataFrame, team_lookup: dict) -> pd.DataFrame:
    df = fixtures_df.copy()
    if "team_h" in df.columns:
        df["team_h_name"] = df["team_h"].map(team_lookup).fillna(df.get("team_h", ""))
    if "team_a" in df.columns:
        df["team_a_name"] = df["team_a"].map(team_lookup).fillna(df.get("team_a", ""))
    return df

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return x

# -----------------------------
# Sidhuvud
# -----------------------------
st.title("Fantasy Premier League — Dashboard")
st.caption("Data hämtas från Supabase (upplyft av din GitHub‑pipeline).")

# -----------------------------
# Hämta data
# -----------------------------
players_df = sb_select(TABLE_PLAYERS, limit=5000)
fixtures_df = sb_select(TABLE_FIXTURES, limit=5000)
events_live_df = sb_select(TABLE_EVENTS, limit=50000)

# Prepp
players_df = map_positions(players_df)
team_lookup = build_team_lookup(fixtures_df, players_df)
fixtures_df = add_team_names_to_fixtures(fixtures_df, team_lookup)

# -----------------------------
# Översikt
# -----------------------------
tab_overview, tab_players, tab_fixtures, tab_live = st.tabs(["Översikt", "Spelare", "Matcher", "Live (GW)"])

with tab_overview:
    c1, c2, c3 = st.columns(3)
    c1.metric("Antal spelare", f"{len(players_df)}")
    teams_count = players_df["team"].nunique() if "team" in players_df.columns and not players_df.empty else 0
    c2.metric("Antal lag (unika team-id)", f"{teams_count}")
    c3.metric("Antal fixtures", f"{len(fixtures_df)}")

    st.subheader("Snabb statistik")
    left, right = st.columns([2, 1])

    with left:
        st.markdown("**Toppspelare (poäng)**")
        if not players_df.empty and "total_points" in players_df.columns:
            top_n = st.slider("Visa topp N", 5, 50, 15, step=5)
            cols = ["web_name", "position", "team", "total_points", "now_cost", "minutes"]
            cols = [c for c in cols if c in players_df.columns]
            st.dataframe(players_df.sort_values("total_points", ascending=False)[cols].head(top_n), use_container_width=True)
        else:
            st.info("Spelarpoäng saknas. Vänta tills pipelinen fyllt tabellen.")

    with right:
        if not players_df.empty and "position" in players_df.columns:
            st.markdown("**Positioner**")
            dist = players_df["position"].value_counts().rename_axis("position").reset_index(name="antal")
            st.bar_chart(dist, x="position", y="antal", height=250, use_container_width=True)
        else:
            st.info("Positionsdata saknas.")

with tab_players:
    st.subheader("Spelarlista och filter")
    if players_df.empty:
        st.info("Inga spelardata ännu.")
    else:
        # Filtrering
        colA, colB, colC, colD = st.columns(4)
        pos_options = ["Alla"] + sorted([p for p in players_df.get("position", pd.Series()).dropna().unique().tolist()])
        pos_val = colA.selectbox("Position", pos_options, index=0)

        min_points = int(players_df.get("total_points", pd.Series([0])).fillna(0).min()) if "total_points" in players_df.columns else 0
        max_points = int(players_df.get("total_points", pd.Series([0])).fillna(0).max()) if "total_points" in players_df.columns else 0
        points_range = colB.slider("Poängintervall", min_points, max_points if max_points>0 else 100, (min_points, max_points if max_points>0 else 100))

        team_ids = sorted(players_df.get("team", pd.Series()).dropna().unique().tolist()) if "team" in players_df.columns else []
        team_labels = ["Alla"] + [f"{tid} ({team_lookup.get(int(tid), f'Team {tid}')})" for tid in team_ids]
        team_choice = colC.selectbox("Team", team_labels, index=0)
        search_name = colD.text_input("Sök namn (web_name)")

        df = players_df.copy()

        # Filter: position
        if pos_val != "Alla" and "position" in df.columns:
            df = df[df["position"] == pos_val]

        # Filter: poäng
        if "total_points" in df.columns:
            df = df[(df["total_points"] >= points_range[0]) & (df["total_points"] <= points_range[1])]

        # Filter: team
        if team_choice != "Alla" and "team" in df.columns:
            chosen_id = int(team_choice.split(" ", 1)[0])  # tar första token som id
            df = df[df["team"] == chosen_id]

        # Filter: namn
        if search_name and "web_name" in df.columns:
            df = df[df["web_name"].str.contains(search_name, case=False, na=False)]

        # Visa tabell
        cols = [c for c in ["web_name","position","team","total_points","now_cost","minutes","form","selected_by_percent"] if c in df.columns]
        st.dataframe(df.sort_values("total_points", ascending=False)[cols], use_container_width=True)

with tab_fixtures:
    st.subheader("Matcher (fixtures)")
    if fixtures_df.empty:
        st.info("Inga fixtures ännu.")
    else:
        # Filter: GW (event)
        events = sorted(fixtures_df.get("event", pd.Series()).dropna().unique().tolist())
        col1, col2 = st.columns(2)
        gw = col1.selectbox("Välj gameweek (event)", ["Alla"] + [int(e) for e in events], index=0, format_func=safe_int)

        finished_filter = col2.selectbox("Status", ["Alla", "Finished", "Upcoming"])
        df = fixtures_df.copy()
        if gw != "Alla" and "event" in df.columns:
            df = df[df["event"] == gw]

        # Statusfilter
        if finished_filter == "Finished" and "finished" in df.columns:
            df = df[df["finished"] == True]
        elif finished_filter == "Upcoming" and "finished" in df.columns:
            df = df[(df["finished"] == False) | (df["finished"].isna())]

        # Visa tabell
        show_cols = [c for c in ["event","kickoff_time","team_h_name","team_a_name","team_h_score","team_a_score","finished","minutes"] if c in df.columns]
        st.dataframe(df.sort_values(["event","kickoff_time"], ascending=[True, True])[show_cols], use_container_width=True)

        # Enkel summering per lag (mål för/egna)
        st.markdown("**Summering: mål för/emot (spelade matcher)**")
        if {"team_h", "team_a", "team_h_score", "team_a_score", "finished"}.issubset(df.columns):
            played = df[df["finished"] == True]
            if not played.empty:
                home = played.groupby("team_h")["team_h_score"].sum().rename("goals_for_home")
                away = played.groupby("team_a")["team_a_score"].sum().rename("goals_for_away")
                conceded_home = played.groupby("team_h")["team_a_score"].sum().rename("goals_against_home")
                conceded_away = played.groupby("team_a")["team_h_score"].sum().rename("goals_against_away")

                agg = pd.concat([home, away, conceded_home, conceded_away], axis=1).fillna(0)
                agg["goals_for"] = agg["goals_for_home"] + agg["goals_for_away"]
                agg["goals_against"] = agg["goals_against_home"] + agg["goals_against_away"]
                agg["team_name"] = agg.index.map(lambda x: team_lookup.get(int(x), f"Team {x}"))
                st.bar_chart(agg.sort_values("goals_for", ascending=False), x="team_name", y="goals_for", height=300, use_container_width=True)
            else:
                st.info("Inga spelade matcher i urvalet.")

with tab_live:
    st.subheader("Live‑poäng per GW")
    if events_live_df.empty:
        st.info("Inga live‑eventdata ännu (det fylls när en GW är aktiv eller efter matcher).")
    else:
        # Välj GW
        gws = sorted(events_live_df.get("event_id", pd.Series()).dropna().unique().tolist())
        gw = st.selectbox("Välj gameweek", gws, index=len(gws)-1 if gws else 0)
        df = events_live_df[events_live_df["event_id"] == gw].copy()
        if df.empty:
            st.info("Inga rader för vald GW.")
        else:
            # Visa toppspelare i livepoäng
            cols_pref = ["element","total_points","minutes","goals_scored","assists","clean_sheets","bps"]
            cols_avail = [c for c in cols_pref if c in df.columns]
            st.dataframe(df.sort_values("total_points", ascending=False)[cols_avail], use_container_width=True)

            # Liten poängfördelning
            if "total_points" in df.columns:
                st.markdown("**Poängfördelning (histogram)**")
                st.bar_chart(df["total_points"].value_counts().sort_index(), height=250, use_container_width=True)

# Sidfot
st.markdown("---")
st.caption("Byggd av din FPL‑pipeline (GitHub Actions) + Supabase + Streamlit.")
