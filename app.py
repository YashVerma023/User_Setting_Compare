import os
import io
import re
import time
from typing import List, Tuple

import pandas as pd
import streamlit as st
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

# ============================ CONFIG ============================
APP_TITLE = "User Settings — Compile from Drive & Compare"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
TOKEN_PATH = "token.json"
CLIENT_SECRETS_FILE = "client_secrets.json"

# Accept either "Sheet1" or "Users" (any case, with/without trailing spaces)
SHEET1_CANDIDATES = ["Sheet1", "Users"]

# Canonical column names
CANONICAL_NAMES = {
    "user alias": "User Alias",
    "alias": "User Alias",
    "useralias": "User Alias",
    "user_alias": "User Alias",
    "user-alias": "User Alias",
    "user name alias": "User Alias",
    "name alias": "User Alias",
    "alisa": "User Alias",
    "user alisa": "User Alias",
    "user id": "User ID",
    "userid": "User ID",
    "user_id": "User ID",
    "broker": "Broker",
    "max loss": "Max Loss",
    "maxloss": "Max Loss",
    "server": "Server",
    "telegram id(s)": "Telegram ID(s)",
    "telegram ids": "Telegram ID(s)",
    "telegram id": "Telegram ID(s)",
    "allocation": "Telegram ID(s)",  # for Sheet1 -> Telegram ID(s)
    "algo": "Algo",
    "operator": "Operator",
}
SPECIFIED_ORDER = ["User Alias", "User ID", "Broker", "Max Loss", "Server", "Telegram ID(s)", "Algo"]
COMPARE_COLS = ["User ID", "Max Loss", "Server", "Telegram ID(s)", "Algo"]
SHEET_TO_COMPARE = "Specified_Compiled"
SHEET1_NAME = "Sheet1"  # legacy default (we now also accept Users)

# ====================== THEME / PAGE ======================
st.set_page_config(page_title=APP_TITLE, page_icon="🧭", layout="wide")
st.markdown(
    """
    <style>
      .app-title {font-size: 2rem; font-weight: 700; margin-bottom: .25rem;}
      .app-subtle {color: #64748b; margin-bottom: 1rem;}
      .stMetric {background: #fff; border-radius: 12px; padding: 8px 12px; box-shadow: 0 1px 3px rgba(0,0,0,.06);}
      .block-gap {margin-top: 12px; margin-bottom: 12px;}
      .pill {display:inline-block;padding:2px 8px;border-radius:999px;background:#eef2ff;color:#3730a3;font-size:12px;margin-right:6px;}
      .bordered-container {border:1px solid #e5e7eb;border-radius:12px;padding:10px;background:#fff;}
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown(f"<div class='app-title'>🧭 {APP_TITLE}</div>", unsafe_allow_html=True)
st.markdown("<div class='app-subtle'>Compile operator/spec sheets from Google Drive and compare changes safely.</div>", unsafe_allow_html=True)

# ====================== COMMON HELPERS ======================
def _norm_header(s: str) -> str:
    s = str(s).replace("\n", " ").replace("\r", " ")
    return " ".join(s.split()).strip()

def _norm_sheet_name(name: str) -> str:
    return _norm_header(name).strip().lower()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for col in df.columns:
        key = _norm_header(col).lower()
        key_compact = key.replace("_", " ").replace("-", " ")
        if key in CANONICAL_NAMES:
            rename_map[col] = CANONICAL_NAMES[key]
        elif key_compact in CANONICAL_NAMES:
            rename_map[col] = CANONICAL_NAMES[key_compact]
        else:
            tight = "".join(key_compact.split())
            if tight in CANONICAL_NAMES:
                rename_map[col] = CANONICAL_NAMES[tight]
    return df.rename(columns=rename_map)

def ensure_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    return df

def to_excel_bytes(sheet_map: dict) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for name, df in sheet_map.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    bio.seek(0)
    return bio.read()

def to_int(series: pd.Series) -> pd.Series:
    """Convert to nullable integer (Int64) so blanks stay <NA>."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def extract_folder_id(link: str) -> str:
    link = link.strip()
    m = re.search(r"/folders/([A-Za-z0-9_\-]+)", link)
    if m: return m.group(1)
    m = re.search(r"[?&]id=([A-Za-z0-9_\-]+)", link)
    if m: return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_\-]{20,}", link): return link
    raise ValueError("Could not extract folder id from the provided link/id.")

def _normalize_column_names(cols: List[str]) -> List[str]:
    return [_norm_header(c) for c in cols]

def build_alias_map(df: pd.DataFrame) -> dict:
    """
    Returns {User ID (str) -> User_Alias (str)} if both columns are present after normalization.
    """
    tmp = normalize_columns(df.copy())
    if "User ID" not in tmp.columns or "User Alias" not in tmp.columns:
        return {}
    m = tmp[["User ID", "User Alias"]].copy()
    m["User ID"] = m["User ID"].astype(str)
    m["User Alias"] = m["User Alias"].astype(str)
    m = m.sort_index().drop_duplicates(subset=["User ID"], keep="last")
    return dict(zip(m["User ID"], m["User Alias"]))

# ====================== DRIVE AUTH & IO ======================
def authenticate_drive():
    """
    Order of preference:
    1) Credentials from Streamlit Secrets -> [google_token] (token.json content)
    2) Local token.json + client_secrets.json (fallback for local dev)
    """
    # Cache across reruns
    if 'drive_service' in st.session_state:
        return st.session_state['drive_service']

    # 1) Streamlit Secrets token (recommended on Streamlit Cloud)
    if "google_token" in st.secrets:
        token_info = dict(st.secrets["google_token"])
        token_info["scopes"] = token_info.get("scopes", SCOPES)
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)
        if not creds.valid:
            if creds.refresh_token:
                creds.refresh(Request())
            else:
                st.error("google_token has no refresh_token. Recreate token.json with offline access and update Secrets.")
                st.stop()
        service = build('drive', 'v3', credentials=creds)
        st.session_state['drive_service'] = service
        return service

    # 2) Local files (fallback)
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if not os.path.exists(CLIENT_SECRETS_FILE):
            raise FileNotFoundError(f"Missing '{CLIENT_SECRETS_FILE}'. Provide OAuth client or set [google_token] in Secrets.")
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    service = build('drive', 'v3', credentials=creds)
    st.session_state['drive_service'] = service
    return service

def list_files_in_folder(service, folder_id: str) -> List[dict]:
    files, page_token = [], None
    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
            pageSize=1000, pageToken=page_token
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files

def download_csv_as_df(service, file_id: str, skiprows: int = 6) -> pd.DataFrame:
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh, skiprows=skiprows)

# ====================== MAPPING (upload only; no defaults) ======================
def read_server_mapping(upload) -> dict:
    """
    Requires an uploaded mapping file (.xlsx or .csv) with columns:
    Server, Operator, Algo
    Returns: {Server: {"Operator": <str>, "Algo": <str|int>}}
    """
    if upload is None:
        st.error("Please upload a ServerMapping file (.xlsx or .csv) with columns Server, Operator, Algo.")
        st.stop()

    name = upload.name.lower()
    try:
        if name.endswith(".csv"):
            mdf = pd.read_csv(upload)
        else:
            mdf = pd.read_excel(upload)
    except Exception as e:
        st.error(f"Failed to read mapping file: {e}")
        st.stop()

    # Normalize headers
    mdf.columns = [_norm_header(c) for c in mdf.columns]
    lower = {c.lower(): c for c in mdf.columns}
    required = {"server", "operator", "algo"}
    if not required.issubset(lower.keys()):
        st.error(f"Mapping must contain columns: Server, Operator, Algo. Found: {list(mdf.columns)}")
        st.stop()

    server_col = lower["server"]
    operator_col = lower["operator"]
    algo_col = lower["algo"]

    # Build dict
    mdf = mdf[[server_col, operator_col, algo_col]].copy()
    mdf[server_col] = mdf[server_col].astype(str).str.strip()
    mdf[operator_col] = mdf[operator_col].astype(str).str.strip()
    # Algo may be int/string; keep as-is for display; don't coerce here.
    mdf = mdf[mdf[server_col] != ""]
    mapping = {}
    for _, r in mdf.iterrows():
        mapping[r[server_col]] = {"Operator": r[operator_col], "Algo": r[algo_col]}
    if not mapping:
        st.error("Mapping file produced an empty mapping. Please check contents.")
        st.stop()
    return mapping

# ====================== COMPILER (Drive -> Excel) ======================
def process_csv_files(service, files: List[dict], server_map: dict, skiprows: int = 6) -> pd.DataFrame:
    all_data = []
    for f in files:
        if not str(f.get("name", "")).lower().endswith(".csv"):
            continue
        try:
            df = download_csv_as_df(service, f["id"], skiprows=skiprows)
            df = normalize_columns(df)

            server = str(f["name"]).split()[0].strip()
            op = server_map.get(server, {}).get("Operator", "")
            algo = server_map.get(server, {}).get("Algo", "")

            df["Server"] = server
            df["Operator"] = op
            df["Algo"] = str(algo)

            df = ensure_columns(df, ["User Alias", "User ID", "Broker", "Max Loss",
                                     "Server", "Telegram ID(s)", "Algo", "Operator"])

            # ✅ Force integers where required (nullable Int64)
            df["Telegram ID(s)"] = to_int(df["Telegram ID(s)"])
            df["Max Loss"]       = to_int(df["Max Loss"])

            # Drop DEAL / DEALER / FEED aliases
            mask = df["User Alias"].astype(str).str.contains("DEAL|FEED", case=False, na=False)
            df = df[~mask]

            all_data.append(df)
        except Exception as e:
            st.warning(f"Skipping '{f.get('name','?')}' due to error: {e}")
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def generate_summary(df: pd.DataFrame) -> pd.DataFrame:
    required = {"User ID", "Server", "Algo", "Operator"}
    if not required.issubset(df.columns):
        return pd.DataFrame()
    summary = df.groupby(['Algo', 'Server', 'Operator'], dropna=False)['User ID'].count().reset_index()
    summary.columns = ['Algo', 'Server', 'Operator', 'Count of User ID']
    grand_total = pd.DataFrame([{
        'Algo': 'Grand Total', 'Server': '', 'Operator': '',
        'Count of User ID': summary['Count of User ID'].sum()
    }])
    return pd.concat([summary, grand_total], ignore_index=True)

# ====================== COMPARATOR (Excel -> diffs) ======================
def clean_for_compare(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = _normalize_column_names(df.columns)
    missing = [c for c in COMPARE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{SHEET_TO_COMPARE}' is missing columns: {missing}")

    df = df[COMPARE_COLS].copy()

    # Types: keep key/labels as strings
    df["User ID"] = df["User ID"].astype(str)
    df["Server"]  = df["Server"].astype(str)
    df["Algo"]    = df["Algo"].astype(str)

    # ✅ Keep these as nullable integers
    df["Telegram ID(s)"] = to_int(df["Telegram ID(s)"])
    df["Max Loss"]       = to_int(df["Max Loss"])

    # Dedup on key
    df = df.sort_index().drop_duplicates(subset=["User ID"], keep="last").reset_index(drop=True)
    return df

def read_specified_compiled(xlsx_bytes: bytes) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=SHEET_TO_COMPARE, engine="openpyxl")

def read_sheet1_last(xlsx_bytes: bytes) -> Tuple[pd.DataFrame, dict]:
    """
    Read the 'last' workbook sheet which can be:
      Sheet1 / Users (case-insensitive; trailing spaces allowed)

    Map columns to compare schema:
      UserID -> User ID
      ALLOCATION -> Telegram ID(s)
      MAX LOSS / Max_Loss / ... -> Max Loss
      SERVER -> Server
      ALGO -> Algo

    Also returns an alias map {User ID -> User_Alias} from the last workbook (if present).
    """
    # 1) Choose the correct sheet
    xls = pd.ExcelFile(io.BytesIO(xlsx_bytes), engine="openpyxl")
    normalized_to_real = {_norm_sheet_name(s): s for s in xls.sheet_names}
    wanted = {_norm_sheet_name(s) for s in SHEET1_CANDIDATES}
    selected_real = None
    for norm, real in normalized_to_real.items():
        if norm in wanted:
            selected_real = real
            break
    if not selected_real:
        raise ValueError(
            f"Could not find a sheet named any of {SHEET1_CANDIDATES} "
            f"(case-insensitive; spaces ignored). Found sheets: {xls.sheet_names}"
        )

    # 2) Read and normalize headers (also canonicalize with your mapping)
    df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=selected_real, engine="openpyxl")
    df = normalize_columns(df)
    df.rename(columns={c: _norm_header(c) for c in df.columns}, inplace=True)
    lower_to_real = { _norm_header(c).lower(): c for c in df.columns }

    def pick(*names):
        for n in names:
            key = _norm_header(n).lower()
            if key in lower_to_real:
                return lower_to_real[key]
        raise ValueError(f"Missing column in '{selected_real}': one of {names}")

    col_user   = pick("UserID", "User ID", "userid", "user_id")
    col_alloc  = pick("ALLOCATION", "Telegram ID(s)", "Telegram IDs", "Telegram ID")
    col_mloss  = pick("MAX LOSS", "Max Loss", "maxloss", "MAX_LOSS", "Max_Loss", "max_loss")
    col_server = pick("SERVER", "Server")
    col_algo   = pick("ALGO", "Algo")

    # 3) Build compare frame
    out = pd.DataFrame({
        "User ID": df[col_user].astype(str),
        "Max Loss": to_int(df[col_mloss]),
        "Server": df[col_server].astype(str),
        "Telegram ID(s)": to_int(df[col_alloc]),
        "Algo": df[col_algo].astype(str),
    })
    out = clean_for_compare(out)

    # 4) Build alias map from *last* workbook (if present)
    last_alias_map = {}
    if "User Alias" in df.columns:
        am = df[[col_user, "User Alias"]].copy()
        am[col_user] = am[col_user].astype(str)
        am["User Alias"] = am["User Alias"].astype(str)
        am = am.sort_index().drop_duplicates(subset=[col_user], keep="last")
        last_alias_map = dict(zip(am[col_user], am["User Alias"]))

    return out, last_alias_map

def compare_frames(
    last_df: pd.DataFrame,
    latest_df: pd.DataFrame,
    last_alias: dict = None,
    latest_alias: dict = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns: (added_df, removed_df, modified_df, all_diffs)
    with an extra 'User_Alias' column filled from last_alias if available, else latest_alias.
    """
    last_alias = last_alias or {}
    latest_alias = latest_alias or {}

    last_idx = last_df.set_index("User ID")
    latest_idx = latest_df.set_index("User ID")
    last_ids, latest_ids = set(last_idx.index), set(latest_idx.index)

    added_ids = sorted(list(latest_ids - last_ids))
    removed_ids = sorted(list(last_ids - latest_ids))
    common_ids = sorted(list(latest_ids & last_ids))

    cols_out = [
        "User ID", "User_Alias", "Change Type",
        "Max Loss (Last)", "Max Loss (Latest)",
        "Server (Last)", "Server (Latest)",
        "Telegram ID(s) (Last)", "Telegram ID(s) (Latest)",
        "Algo (Last)", "Algo (Latest)",
        "Diff Columns",
    ]
    rows = []

    def fmt(v, integer=False):
        if pd.isna(v):
            return ""
        return int(v) if integer else str(v)

    def neq(x, y):
        if pd.isna(x) and pd.isna(y):
            return False
        return x != y

    def get_alias(uid: str) -> str:
        # Prefer alias from LAST if present; otherwise fall back to LATEST; else blank.
        v = last_alias.get(uid, None)
        if v is not None and str(v).strip() != "":
            return str(v)
        v = latest_alias.get(uid, None)
        if v is not None and str(v).strip() != "":
            return str(v)
        return ""

    for uid in added_ids:
        r = latest_idx.loc[uid]
        rows.append([
            uid, get_alias(uid), "ADDED",
            "", fmt(r["Max Loss"], integer=True),
            "", fmt(r["Server"]),
            "", fmt(r["Telegram ID(s)"], integer=True),
            "", fmt(r["Algo"]),
            "ALL"
        ])

    for uid in removed_ids:
        r = last_idx.loc[uid]
        rows.append([
            uid, get_alias(uid), "REMOVED",
            fmt(r["Max Loss"], integer=True), "",
            fmt(r["Server"]), "",
            fmt(r["Telegram ID(s)"], integer=True), "",
            fmt(r["Algo"]), "",
            "ALL"
        ])

    for uid in common_ids:
        a, b = last_idx.loc[uid], latest_idx.loc[uid]
        diffs = []
        if neq(a["Max Loss"], b["Max Loss"]): diffs.append("Max Loss")
        if neq(a["Server"], b["Server"]): diffs.append("Server")
        if neq(a["Telegram ID(s)"], b["Telegram ID(s)"]): diffs.append("Telegram ID(s)")
        if neq(a["Algo"], b["Algo"]): diffs.append("Algo")

        if diffs:
            rows.append([
                uid, get_alias(uid), "MODIFIED",
                fmt(a["Max Loss"], integer=True), fmt(b["Max Loss"], integer=True),
                fmt(a["Server"]), fmt(b["Server"]),
                fmt(a["Telegram ID(s)"], integer=True), fmt(b["Telegram ID(s)"], integer=True),
                fmt(a["Algo"]), fmt(b["Algo"]),
                ", ".join(diffs),
            ])

    all_diffs = pd.DataFrame(rows, columns=cols_out)
    return (
        all_diffs[all_diffs["Change Type"] == "ADDED"].reset_index(drop=True),
        all_diffs[all_diffs["Change Type"] == "REMOVED"].reset_index(drop=True),
        all_diffs[all_diffs["Change Type"] == "MODIFIED"].reset_index(drop=True),
        all_diffs.reset_index(drop=True)
    )

# ====================== UI HELPERS ======================
def render_modified_with_filters(modified_df: pd.DataFrame):
    """Render the Modified tab with interactive filters and downloadable results."""
    st.caption("Use filters to narrow down the Modified rows.")

    if modified_df.empty:
        st.info("No modified rows.")
        return

    # Build choices
    diff_tokens = sorted({t.strip()
                          for s in modified_df["Diff Columns"].dropna().astype(str)
                          for t in s.split(",") if t.strip()})
    if not diff_tokens:
        diff_tokens = ["Max Loss", "Server", "Telegram ID(s)", "Algo"]

    # Servers & Algos (from both last/latest)
    servers = sorted(set(modified_df["Server (Last)"].astype(str)) |
                     set(modified_df["Server (Latest)"].astype(str)))
    algos = sorted(set(modified_df["Algo (Last)"].astype(str)) |
                   set(modified_df["Algo (Latest)"].astype(str)))

    # Numeric bounds for Max Loss / Telegram IDs
    def _nan_to_series(col):
        s = pd.to_numeric(modified_df[col], errors="coerce")
        return s.dropna()

    maxloss_last_vals = _nan_to_series("Max Loss (Last)")
    maxloss_latest_vals = _nan_to_series("Max Loss (Latest)")
    tel_last_vals = _nan_to_series("Telegram ID(s) (Last)")
    tel_latest_vals = _nan_to_series("Telegram ID(s) (Latest)")

    # UI
    with st.expander("Filters", expanded=True):
        fcol1, fcol2, fcol3 = st.columns([1.2, 1, 1])
        with fcol1:
            selected_tokens = st.multiselect("Changed columns include…", diff_tokens, default=diff_tokens)
            user_query = st.text_input("User ID contains", value="")
        with fcol2:
            sel_servers = st.multiselect("Server (Last/Latest)", servers, default=servers)
            sel_algos = st.multiselect("Algo (Last/Latest)", algos, default=algos)
        with fcol3:
            if not maxloss_latest_vals.empty:
                ml_min, ml_max = int(maxloss_latest_vals.min()), int(maxloss_latest_vals.max())
            else:
                ml_min, ml_max = 0, 0
            ml_range = st.slider(
                "Max Loss (Latest) range",
                min_value=int(ml_min),
                max_value=int(ml_max) if ml_max >= ml_min else int(ml_min),
                value=(int(ml_min), int(ml_max)) if ml_max >= ml_min else (int(ml_min), int(ml_min))
            )
            if not tel_latest_vals.empty:
                tl_min, tl_max = int(tel_latest_vals.min()), int(tel_latest_vals.max())
            else:
                tl_min, tl_max = 0, 0
            tl_range = st.slider(
                "Telegram ID(s) (Latest) range",
                min_value=int(tl_min),
                max_value=int(tl_max) if tl_max >= tl_min else int(tl_min),
                value=(int(tl_min), int(tl_max)) if tl_max >= tl_min else (int(tl_min), int(tl_min))
            )

    # Apply filters
    filt = modified_df.copy()

    if selected_tokens and len(selected_tokens) < len(diff_tokens):
        mask = filt["Diff Columns"].apply(lambda s: any(tok in str(s) for tok in selected_tokens))
        filt = filt[mask]

    if user_query.strip():
        q = user_query.strip().lower()
        filt = filt[filt["User ID"].astype(str).str.lower().str.contains(q)]

    if sel_servers and len(sel_servers) < len(servers):
        filt = filt[
            filt["Server (Last)"].astype(str).isin(sel_servers) |
            filt["Server (Latest)"].astype(str).isin(sel_servers)
        ]

    if sel_algos and len(sel_algos) < len(algos):
        filt = filt[
            filt["Algo (Last)"].astype(str).isin(sel_algos) |
            filt["Algo (Latest)"].astype(str).isin(sel_algos)
        ]

    if not filt.empty and (ml_max >= ml_min):
        ml_num = pd.to_numeric(filt["Max Loss (Latest)"], errors="coerce")
        filt = filt[(ml_num.isna()) | ((ml_num >= ml_range[0]) & (ml_num <= ml_range[1]))]

    if not filt.empty and (tl_max >= tl_min):
        tl_num = pd.to_numeric(filt["Telegram ID(s) (Latest)"], errors="coerce")
        filt = filt[(tl_num.isna()) | ((tl_num >= tl_range[0]) & (tl_num <= tl_range[1]))]

    m1, m2 = st.columns(2)
    with m1: st.metric("Rows after filters", len(filt))
    with m2:
        st.caption("Changed columns legend:")
        st.markdown("".join([f"<span class='pill'>{t}</span>" for t in diff_tokens]), unsafe_allow_html=True)

    st.dataframe(filt, use_container_width=True, height=480)

    dl1, dl2 = st.columns(2)
    with dl1:
        xbytes = to_excel_bytes({"Modified_Filtered": filt})
        st.download_button(
            "⬇️ Download filtered Modified (Excel)",
            data=xbytes,
            file_name="modified_filtered.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    with dl2:
        st.download_button(
            "⬇️ Download filtered Modified (CSV)",
            data=filt.to_csv(index=False).encode(),
            file_name="modified_filtered.csv",
            mime="text/csv",
            use_container_width=True
        )

# ====================== SIDEBAR ======================
mode = st.sidebar.radio("Mode", ["Compile from Google Drive", "Compare Latest vs Last (Sheet1)"], index=0)

with st.sidebar.expander("ServerMapping"):
    mapping_file = st.file_uploader("Upload ServerMapping (.xlsx or .csv)", type=["xlsx", "csv"])
    st.caption("Required columns: **Server, Operator, Algo** (case-insensitive).")
    st.download_button(
        "Download Mapping Template (CSV)",
        data=pd.DataFrame({"Server": ["VS1","VS2"], "Operator": ["NAME","NAME"], "Algo": [8,1]}).to_csv(index=False).encode(),
        file_name="ServerMapping_template.csv",
        mime="text/csv",
        use_container_width=True
    )

with st.sidebar.expander("Google Auth"):
    st.caption("If running locally with files, this deletes local token.json. (Secrets-based tokens are managed in Streamlit Cloud.)")
    if st.button("🔁 Reset local token.json"):
        try:
            if os.path.exists(TOKEN_PATH):
                os.remove(TOKEN_PATH)
                st.success("Deleted token.json. You will be asked to sign in next time (local mode).")
            else:
                st.info("No token.json found.")
        except Exception as e:
            st.error(f"Could not delete token: {e}")

# ====================== MODES ======================

# -------- Mode 1: Compile from Drive --------
if mode == "Compile from Google Drive":
    st.subheader("📦 Compile CSVs in a Drive folder into `Compiled_User_Settings.xlsx`")
    link = st.text_input("Paste Google Drive folder link (or folder ID)")

    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        skiprows = st.number_input("CSV header lines to skip before real header", min_value=0, max_value=50, value=6, step=1)
    with c2:
        out_name = st.text_input("Output filename", value="Compiled_User_Settings.xlsx")
    with c3:
        st.markdown("<div class='block-gap'></div>", unsafe_allow_html=True)
        run_compile = st.button("🚀 Compile Now", type="primary", use_container_width=True)

    if run_compile:
        # Require mapping upload (no defaults, no persistence)
        server_map = read_server_mapping(mapping_file)

        try:
            folder_id = extract_folder_id(link)
        except ValueError as e:
            st.error(str(e))
            st.stop()

        try:
            with st.status("Authenticating with Google…", expanded=False) as s:
                service = authenticate_drive()
                s.update(label="Listing files in folder…")
                files = list_files_in_folder(service, folder_id)
                csv_files = [f for f in files if str(f.get("name","")).lower().endswith(".csv")]
                s.update(label=f"Found {len(csv_files)} CSV files. Processing…")
                time.sleep(0.2)

            if not csv_files:
                st.warning("No CSV files found in this folder.")
                st.stop()

            with st.spinner("Downloading & processing…"):
                compiled_df = process_csv_files(service, csv_files, server_map, skiprows=skiprows)

            if compiled_df.empty:
                st.error("No valid data after processing.")
                st.stop()

            operator_compiled = compiled_df.copy()

            specified = compiled_df.copy()
            specified = ensure_columns(specified, SPECIFIED_ORDER)
            specified = specified[SPECIFIED_ORDER]

            summary = generate_summary(compiled_df)

            xbytes = to_excel_bytes({
                "Operator_Compiled": operator_compiled,
                "Specified_Compiled": specified,
                "Summary": summary
            })
            st.success(f"Built workbook from {len([f for f in csv_files])} CSV file(s).")

            m1, m2, m3 = st.columns(3)
            with m1: st.metric("Rows compiled", f"{len(compiled_df):,}")
            with m2: st.metric("Unique Users", compiled_df["User ID"].nunique())
            with m3: st.metric("Servers", compiled_df["Server"].nunique())

            st.download_button(
                "⬇️ Download Compiled_User_Settings.xlsx",
                data=xbytes,
                file_name=out_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

            tabs = st.tabs(["Specified_Compiled (preview)", "Summary", "Processed files"])
            with tabs[0]:
                st.dataframe(specified.head(25), use_container_width=True)
            with tabs[1]:
                st.dataframe(summary, use_container_width=True)
            with tabs[2]:
                st.write("\n".join([f["name"] for f in csv_files]))

        except FileNotFoundError as e:
            st.error(str(e))
        except HttpError as e:
            st.error(f"Google API error: {e}")
        except Exception as e:
            st.exception(e)

# -------- Mode 2: Compare Latest vs Last (Sheet1) --------
else:
    st.subheader("🔍 Compare LATEST compiled (`Specified_Compiled`) vs LAST workbook (`Sheet1`/`Users`)")

    col1, col2 = st.columns(2)
    with col1:
        f_last = st.file_uploader("Upload **Last Workbook** (.xlsx) — contains `Sheet1` or `Users`", type=["xlsx"], key="last_file")
    with col2:
        f_latest = st.file_uploader("Upload **Latest Compiled** (.xlsx) — uses `Specified_Compiled`", type=["xlsx"], key="latest_file")

    if f_last and f_latest:
        last_bytes = f_last.read()
        latest_bytes = f_latest.read()

        try:
            # Read & normalize
            last_df, last_alias = read_sheet1_last(last_bytes)          # from Sheet1/Users schema (+ alias map)
            latest_raw = read_specified_compiled(latest_bytes)          # Specified_Compiled as-is
            latest_alias = build_alias_map(latest_raw)                  # {User ID -> User_Alias}
            latest_df = clean_for_compare(latest_raw)                   # only compare fields

            st.success("Loaded: LAST from Sheet1/Users, LATEST from Specified_Compiled.")

            added_df, removed_df, modified_df, all_diffs = compare_frames(last_df, latest_df, last_alias, latest_alias)

            # --- Derived categories: Differences in MS / CC based on User_Alias prefixes ---
            def filter_by_alias_prefixes(df: pd.DataFrame, prefixes: List[str]) -> pd.DataFrame:
                if df.empty:
                    return df
                p = tuple(prefixes)
                mask = df["User_Alias"].astype(str).str.strip().str.upper().str.startswith(p)
                return df[mask].reset_index(drop=True)

            ms_prefixes = ["MSR", "MSP", "MSS", "MSN"]
            cc_prefixes = ["CC", "CCV", "CCG", "MSG", "MSV"]

            diffs_ms_df = filter_by_alias_prefixes(all_diffs, ms_prefixes)
            diffs_cc_df = filter_by_alias_prefixes(all_diffs, cc_prefixes)

            # Metrics
            k1, k2, k3, k4, k5, k6 = st.columns(6)
            with k1: st.metric("Added", len(added_df))
            with k2: st.metric("Removed", len(removed_df))
            with k3: st.metric("Modified", len(modified_df))
            with k4: st.metric("All Differences", len(all_diffs))
            with k5: st.metric("Differences in MS", len(diffs_ms_df))
            with k6: st.metric("Differences in CC", len(diffs_cc_df))

            st.markdown("---")
            tabs = st.tabs([
                "All Differences",
                "Added",
                "Removed",
                "Modified (Filterable)",
                "Differences in MS",
                "Differences in CC"
            ])
            with tabs[0]:
                st.dataframe(all_diffs, use_container_width=True, height=480)
            with tabs[1]:
                st.dataframe(added_df, use_container_width=True, height=480)
            with tabs[2]:
                st.dataframe(removed_df, use_container_width=True, height=480)
            with tabs[3]:
                render_modified_with_filters(modified_df)
            with tabs[4]:
                st.caption("Aliases starting with: MSR, MSP, MSS, MSN")
                st.dataframe(diffs_ms_df, use_container_width=True, height=480)
            with tabs[5]:
                st.caption("Aliases starting with: CC, CCV, CCG, MSG, MSV")
                st.dataframe(diffs_cc_df, use_container_width=True, height=480)

            # Export
            xbytes = to_excel_bytes({
                "All_Differences": all_diffs,
                "Added": added_df,
                "Removed": removed_df,
                "Modified": modified_df,
                "Differences_in_MS": diffs_ms_df,
                "Differences_in_CC": diffs_cc_df
            })
            st.download_button(
                "⬇️ Download Differences (Excel)",
                data=xbytes,
                file_name="user_settings_differences.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

            with st.expander("Preview (first 10 rows)"):
                cc1, cc2 = st.columns(2)
                with cc1:
                    st.write("**Latest (cleaned for compare)**")
                    st.dataframe(latest_df.head(10), use_container_width=True)
                with cc2:
                    st.write("**Last (cleaned for compare)**")
                    st.dataframe(last_df.head(10), use_container_width=True)

        except ValueError as ve:
            st.error(str(ve))
        except Exception as e:
            st.exception(e)
    else:
        st.info("Upload both Excel files to start.")
