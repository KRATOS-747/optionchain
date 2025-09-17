# streamlit_opt_chain_live_fixed.py
"""
Improved Streamlit app for polling Upstox option chain.

Key improvements made:
 - Use requests.Session for connection pooling
 - Safer, atomic writes for latest/history files
 - More defensive parsing of contract/chain responses
 - Clearer logging to Streamlit sidebar
 - Avoid raising on non-200 responses (provides user-visible error)
 - Type hints and small refactors for readability
 - Keep the original "live" toggle logic but ensure we break cleanly
"""

import streamlit as st
import requests
import json
import time
from datetime import date
from typing import Any, Dict, List, Optional
import pandas as pd
from dateutil import parser as date_parser
from pathlib import Path
import tempfile
import os

# --------------------------
# Config - change paths if needed
# --------------------------
ACCESS_TOKEN_FILE = Path("/Users/akshayjoshi/Documents/FINCODE/access_token.txt")
HISTORY_FILE = Path("/Users/akshayjoshi/Documents/FINCODE/nifty_option_chain_history.jsonl")
LATEST_FILE = Path("/Users/akshayjoshi/Documents/FINCODE/nifty_option_chain_latest.json")
BASE = "https://api.upstox.com"   # change to sandbox URL if you use sandbox
DEFAULT_INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"

# --------------------------
# Utility functions
# --------------------------
def load_token(path: Path = ACCESS_TOKEN_FILE) -> Optional[str]:
    try:
        # Streamlit secrets override local file (useful when deployed)
        if "upstox" in st.secrets and "access_token" in st.secrets["upstox"]:
            return st.secrets["upstox"]["access_token"]
    except Exception:
        # running outside of Streamlit or no secrets defined
        pass
    try:
        return path.read_text().strip()
    except FileNotFoundError:
        return None

def get_contracts(session: requests.Session, token: str, instrument_key: str = DEFAULT_INSTRUMENT_KEY) -> Dict[str, Any]:
    url = f"{BASE}/v2/option/contract"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    resp = session.get(url, headers=headers, params={"instrument_key": instrument_key}, timeout=15)
    # don't raise here; return JSON or a dict explaining the error
    try:
        payload = resp.json()
    except Exception:
        payload = {"error": f"Invalid JSON from contracts endpoint (status {resp.status_code})"}
    if not resp.ok:
        payload.setdefault("error", f"HTTP {resp.status_code}")
    return payload

def choose_nearest_expiry(contract_json: Dict[str, Any]) -> Optional[str]:
    data = contract_json.get("data", []) if isinstance(contract_json, dict) else []
    dates: List[tuple] = []
    for item in data:
        dstr = None
        if isinstance(item, dict):
            dstr = item.get("expiry_date") or item.get("expiry") or item.get("expiryDate") or item.get("date")
        elif isinstance(item, str):
            dstr = item
        if not dstr:
            continue
        try:
            dt = date_parser.parse(dstr).date()
            if dt >= date.today():
                dates.append((dt, dt.isoformat()))
        except Exception:
            continue
    if not dates:
        return None
    dates.sort()
    return dates[0][1]

def fetch_option_chain(session: requests.Session, token: str, expiry: str, instrument_key: str = DEFAULT_INSTRUMENT_KEY) -> Dict[str, Any]:
    url = f"{BASE}/v2/option/chain"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    params = {"instrument_key": instrument_key, "expiry_date": expiry}
    resp = session.get(url, headers=headers, params=params, timeout=30)
    try:
        payload = resp.json()
    except Exception:
        payload = {"error": f"Invalid JSON from chain endpoint (status {resp.status_code})"}
    if not resp.ok:
        payload.setdefault("error", f"HTTP {resp.status_code}")
    return payload

def atomic_write_json(path: Path, data: Any, indent: Optional[int] = None) -> None:
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(data, f, indent=indent)
        os.replace(tmp_path, str(path))
    except Exception:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        raise

def append_jsonl(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, default=str)
    with path.open("a") as f:
        f.write(line + "\n")


# Flatten function (tolerant to missing keys)
def option_chain_json_to_df(json_obj: Dict[str, Any]) -> pd.DataFrame:
    if isinstance(json_obj, dict) and "snapshot" in json_obj and isinstance(json_obj["snapshot"], dict):
        json_obj = json_obj["snapshot"]

    data = json_obj.get("data", []) if isinstance(json_obj, dict) else []
    rows: List[Dict[str, Any]] = []

    for item in data:
        if not isinstance(item, dict):
            continue
        strike = item.get("strike_price") or item.get("strike") or item.get("strikePrice")
        call = item.get("call_options", {}) or item.get("CE", {}) or item.get("call", {})
        put = item.get("put_options", {}) or item.get("PE", {}) or item.get("put", {})

        def extract_side(side_obj: dict, prefix: str) -> dict:
            out = {}
            if not isinstance(side_obj, dict):
                return out
            md = side_obj.get("market_data", {}) or side_obj.get("marketData", {}) or {}
            og = side_obj.get("option_greeks", {}) or side_obj.get("greeks", {}) or {}
            out[f"{prefix}_ltp"] = md.get("ltp") or md.get("last_price") or md.get("lastTradedPrice")
            out[f"{prefix}_bid"] = md.get("bid") or md.get("best_bid")
            out[f"{prefix}_ask"] = md.get("ask") or md.get("best_ask")
            out[f"{prefix}_oi"] = md.get("oi") or md.get("open_interest")
            out[f"{prefix}_volume"] = md.get("volume") or md.get("traded_volume")
            out[f"{prefix}_iv"] = og.get("iv") or og.get("implied_volatility")
            out[f"{prefix}_delta"] = og.get("delta")
            out[f"{prefix}_gamma"] = og.get("gamma")
            out[f"{prefix}_theta"] = og.get("theta")
            out[f"{prefix}_vega"] = og.get("vega")
            out[f"{prefix}_pop"] = og.get("pop") or side_obj.get("pop")
            return out

        row = {
            "strike": strike,
            "underlying": item.get("underlying") or item.get("instrument_key"),
            "timestamp": item.get("updated_at") or item.get("last_updated") or item.get("timestamp"),
        }
        row.update(extract_side(call, "call"))
        row.update(extract_side(put, "put"))
        rows.append(row)

    df = pd.DataFrame(rows)
    for col in df.columns:
        if col in ("underlying", "timestamp"):
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "call_ltp" in df.columns and "put_ltp" in df.columns:
        df["straddle_price"] = df["call_ltp"].fillna(0) + df["put_ltp"].fillna(0)
    if "strike" in df.columns:
        df = df.sort_values("strike").reset_index(drop=True)
    return df


# --------------------------
# Streamlit UI
# --------------------------
st.set_page_config(page_title="Nifty Option Chain — live (polling)", layout="wide")
st.title("Nifty Option Chain — Polling snapshot → JSONL → Table (improved)")

# Sidebar settings
st.sidebar.markdown("## Settings")
instrument_key = st.sidebar.text_input("Instrument Key", value=DEFAULT_INSTRUMENT_KEY)
poll_seconds = st.sidebar.number_input("Poll interval (seconds)", min_value=2, max_value=600, value=10, step=1)
auto_find_expiry = st.sidebar.checkbox("Auto-find nearest expiry (recommended)", value=True)
history_file_show = st.sidebar.text_input("History JSONL file", value=str(HISTORY_FILE))

# Start/Stop control
if "live" not in st.session_state:
    st.session_state.live = False
start_stop = st.sidebar.button("Start" if not st.session_state.live else "Stop")
if start_stop:
    st.session_state.live = not st.session_state.live

# small logging area in sidebar
if "logs" not in st.session_state:
    st.session_state.logs = []

def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.logs.insert(0, f"[{ts}] {msg}")

# Display token and basic checks
session = requests.Session()
token = load_token()
if token:
    st.sidebar.success("Loaded access token")
else:
    st.sidebar.error(f"Token file not found: {ACCESS_TOKEN_FILE}")

# Main content placeholders
status_placeholder = st.empty()
table_placeholder = st.empty()
download_col, info_col = st.columns([1, 2])

# Pre-fetch expiry if requested
expiry: Optional[str] = None
if token:
    try:
        contracts_json = get_contracts(session, token, instrument_key=instrument_key)
        if auto_find_expiry:
            expiry = choose_nearest_expiry(contracts_json)
        else:
            if isinstance(contracts_json.get("data"), list):
                examples = contracts_json.get("data")[:10]
                st.sidebar.write("Sample expiries / contract data (first 10):")
                st.sidebar.write(examples)
    except Exception as e:
        st.sidebar.error(f"Contract API error: {e}")

if expiry:
    st.sidebar.info(f"Using expiry: {expiry}")
else:
    st.sidebar.warning("No expiry auto-selected. Either contract API returned none or token issue. You can still enter expiry manually.")

# Manual expiry input (fallback)
expiry_input = st.sidebar.text_input("Expiry (YYYY-MM-DD) — leave empty to use auto", value=expiry or "")
if expiry_input:
    expiry = expiry_input

# Helper to render latest snapshot -> table
def render_snapshot_table(snapshot_json):
    if not snapshot_json or not isinstance(snapshot_json, dict):
        table_placeholder.warning("No snapshot JSON available to convert.")
        return None
    df = option_chain_json_to_df(snapshot_json.get("data") and snapshot_json or snapshot_json.get("snapshot", snapshot_json))
    if df.empty:
        table_placeholder.warning("Flattened DataFrame is empty (API returned no strikes).")
        return df
    with table_placeholder.container():
        st.subheader(f"Latest option chain snapshot — expiry {expiry or 'unknown'}")
        st.text(f"Snapshot timestamp (UTC): {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
        st.dataframe(df, use_container_width=True, height=600)
    return df

# If not live, show latest file if present
if not st.session_state.live:
    try:
        if LATEST_FILE.exists():
            latest = json.loads(LATEST_FILE.read_text())
            render_snapshot_table(latest.get("snapshot") if isinstance(latest, dict) else latest)
        else:
            table_placeholder.info("No latest snapshot file found yet. Click 'Fetch one snapshot now' to create one.")

        if st.button("Fetch one snapshot now"):
            if not token:
                st.error("No token loaded.")
            elif not expiry:
                st.error("No expiry selected. Either enable auto-find expiry or enter expiry in sidebar.")
            else:
                try:
                    status_placeholder.info("Fetching option chain...")
                    sn = fetch_option_chain(session, token, expiry, instrument_key=instrument_key)
                    # save snapshot
                    append_jsonl(Path(history_file_show), {"timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "snapshot": sn})
                    atomic_write_json(LATEST_FILE, {"timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "snapshot": sn}, indent=2)
                    status_placeholder.success("Saved snapshot to history & latest file.")
                    log("One-off fetch saved")
                    render_snapshot_table(sn)
                except Exception as e:
                    status_placeholder.error(f"Fetch error: {e}")
    except Exception as e:
        table_placeholder.error(f"Error reading latest file: {e}")

# Live mode: simplified loop that checks session_state.live each iteration so the Stop button works
if st.session_state.live:
    try:
        status_placeholder.info(f"Live mode ON — polling every {poll_seconds} seconds")
        # single long-running loop - note: this will block the script and Streamlit's UI until stopped
        while st.session_state.live:
            start_ts = time.time()
            try:
                if not token:
                    status_placeholder.error("No token loaded — stopping live.")
                    st.session_state.live = False
                    break
                if not expiry:
                    status_placeholder.warning("No expiry selected — attempting to auto-find again.")
                    contracts_json = get_contracts(session, token, instrument_key=instrument_key)
                    expiry = choose_nearest_expiry(contracts_json)
                    if expiry:
                        st.sidebar.info(f"Auto-found expiry: {expiry}")
                    else:
                        status_placeholder.error("Still no expiry found. Stop & check contract API/token.")
                        st.session_state.live = False
                        break

                snapshot = fetch_option_chain(session, token, expiry, instrument_key=instrument_key)
                # save to files
                append_jsonl(Path(history_file_show), {"timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "snapshot": snapshot})
                atomic_write_json(LATEST_FILE, {"timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "snapshot": snapshot}, indent=2)
                status_placeholder.success(f"Fetched & saved snapshot ({time.strftime('%Y-%m-%d %H:%M:%S')})")
                log("Live fetch saved")
                # render table
                if "snapshot" in snapshot and isinstance(snapshot["snapshot"], dict):
                    render_snapshot_table(snapshot["snapshot"])
                else:
                    render_snapshot_table(snapshot)
            except Exception as e:
                status_placeholder.error(f"Error during fetch/save: {e}")
                log(f"Error during fetch/save: {e}")
            # throttle to poll_seconds
            elapsed = time.time() - start_ts
            to_sleep = max(0, poll_seconds - elapsed)
            # frequently check if the user pressed Stop by sleeping in short chunks
            slept = 0.0
            chunk = 0.5
            while slept < to_sleep and st.session_state.live:
                time.sleep(min(chunk, to_sleep - slept))
                slept += min(chunk, to_sleep - slept)
    except Exception as e:
        st.error(f"Live polling stopped due to error: {e}")
        st.session_state.live = False

# Footer: show history file path and download latest CSV
st.markdown("---")
st.write(f"History file (JSONL): `{history_file_show}`")
# Offer to download latest CSV (if available)
try:
    if LATEST_FILE.exists():
        latest = json.loads(LATEST_FILE.read_text())
        df_latest = option_chain_json_to_df(latest.get("snapshot") if isinstance(latest, dict) else latest)
        if not df_latest.empty:
            csv_data = df_latest.to_csv(index=False)
            st.download_button("Download latest as CSV", csv_data, file_name=f"nifty_chain_latest_{expiry or 'unknown'}.csv", mime="text/csv")
except Exception:
    pass

# show logs in sidebar
with st.sidebar.expander("App log (recent)"):
    for entry in st.session_state.logs[:100]:
        st.write(entry)

st.caption("Notes: Polling the REST option-chain every N seconds is fine for a single underlying. For real tick-level updates or many instruments, use the Upstox Market Data WebSocket V3 and subscribe to specific option instrument keys.")
