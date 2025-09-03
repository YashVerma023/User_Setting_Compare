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

# --- Removed hard-coded SERVER_ALGO_MAP. Mapping comes only from uploaded file. ---

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
    "allocation": "Telegram ID(s)",  # mapping for Sheet1 -> Telegram ID(s)
    "algo": "Algo",
    "operator": "Operator",
}
SPECIFIED_ORDER = ["User Alias", "User ID", "Broker", "Max Loss", "Server", "Telegram ID(s)", "Algo"]
COMPARE_COLS = ["User ID", "Max Loss", "Server", "Telegram ID(s)", "Algo"]
SHEET_TO_COMPARE = "Specified_Compiled"
SHEET1_NAME = "Sheet1"  # the "last" workbook sheet to compare against

# ====================== COMMON HELPERS ======================
def _norm_header(s: str) -> str:
    s = str(s).replace("\n", " ").replace("\r", " ")
    return " ".join(s.split()).strip()

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

def extract_folder_id(link: str) -> str:
    link = link.strip()
    m = re.search(r"/folders/([A-Za-z0-9_\-]+)", link)
    if m: return m.group(1)
    m = re.search(r"[?&]id=([A-Za-z0-9_\-]+)", link)
    if m: return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_\-]{20,}", link): return link
    raise ValueError("Could not extract folder id from the provided link/id.")

def to_excel_bytes(sheet_map: dict) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for name, df in sheet_map.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    bio.seek(0)
    return bio.read()

# ====================== DRIVE AUTH & IO ======================
def authenticate_drive():
    """
    Order of preference:
    1) Credentials from Streamlit Secrets -> [google_token] (token.json content)
    2) Local token.json + client_secrets.json (fallback for local dev)
    """
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
        return build('drive', 'v3', credentials=creds)

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
    return build('drive', 'v3', credentials=creds)

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
    Server, Operator, Algo  (case-insensitive; extra columns ignored)
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
    mdf[algo_col] = mdf[algo_col].where(pd.notna(mdf[algo_col]), "")

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
            df["Algo"] = algo

            df = ensure_columns(df, ["User Alias", "User ID", "Broker", "Max Loss",
                                     "Server", "Telegram ID(s)", "Algo", "Operator"])

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
def _normalize_column_names(cols: List[str]) -> List[str]:
    return [_norm_header(c) for c in cols]

def clean_for_compare(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = _normalize_column_names(df.columns)
    missing = [c for c in COMPARE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{SHEET_TO_COMPARE}' is missing columns: {missing}")
    df = df[COMPARE_COLS].copy()

    df["User ID"] = df["User ID"].astype(str)
    df["Server"] = df["Server"].astype(str)
    df["Algo"] = df["Algo"].astype(str)
    df["Telegram ID(s)"] = df["Telegram ID(s)"].astype(float)

    for c in ["Server", "Telegram ID(s)", "Algo"]:
        df[c] = df[c].fillna("")

    numeric_max_loss = pd.to_numeric(df["Max Loss"], errors="coerce")
    df["_MaxLoss_num"] = numeric_max_loss.fillna(pd.NA)
    df["Max Loss"] = df["Max Loss"].apply(lambda x: "" if pd.isna(x) else str(x))

    df = df.sort_index().drop_duplicates(subset=["User ID"], keep="last").reset_index(drop=True)
    return df

def read_specified_compiled(xlsx_bytes: bytes) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=SHEET_TO_COMPARE, engine="openpyxl")

def read_sheet1_last(xlsx_bytes: bytes) -> pd.DataFrame:
    """
    Read 'Sheet1' from the LAST workbook and map its columns to the comparison schema:
      UserID -> User ID (key)
      ALLOCATION -> Telegram ID(s)
      MAX LOSS -> Max Loss
      SERVER -> Server
      ALGO -> Algo
    """
    df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=SHEET1_NAME, engine="openpyxl")
    # normalize headers (keep original cases for data, we'll map by lower)
    norm_map = {c: _norm_header(c) for c in df.columns}
    df.rename(columns=norm_map, inplace=True)

    lower_to_real = { _norm_header(c).lower(): c for c in df.columns }

    def pick(*names):
        for n in names:
            key = _norm_header(n).lower()
            if key in lower_to_real:
                return lower_to_real[key]
        raise ValueError(f"Missing column in {SHEET1_NAME}: one of {names}")

    col_user   = pick("UserID", "User ID", "userid", "user_id")
    col_alloc  = pick("ALLOCATION", "Telegram ID(s)", "Telegram IDs", "Telegram ID")
    col_mloss  = pick("MAX LOSS", "Max Loss", "maxloss")
    col_server = pick("SERVER", "Server")
    col_algo   = pick("ALGO", "Algo")

    out = pd.DataFrame({
        "User ID": df[col_user].astype(str),
        "Max Loss": df[col_mloss],
        "Server": df[col_server].astype(str),
        "Telegram ID(s)": df[col_alloc].astype(str),
        "Algo": df[col_algo].astype(str),
    })
    # Reuse cleaning to align types and helper columns
    out = clean_for_compare(out)
    return out

def compare_frames(last_df: pd.DataFrame, latest_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    last_idx = last_df.set_index("User ID")
    latest_idx = latest_df.set_index("User ID")
    last_ids, latest_ids = set(last_idx.index), set(latest_idx.index)

    added_ids = sorted(list(latest_ids - last_ids))
    removed_ids = sorted(list(last_ids - latest_ids))
    common_ids = sorted(list(latest_ids & last_ids))

    cols_out = [
        "User ID", "Change Type",
        "Max Loss (Last)", "Max Loss (Latest)",
        "Server (Last)", "Server (Latest)",
        "Telegram ID(s) (Last)", "Telegram ID(s) (Latest)",
        "Algo (Last)", "Algo (Latest)",
        "Diff Columns",
    ]
    rows = []

    for uid in added_ids:
        r = latest_idx.loc[uid]
        rows.append([uid, "ADDED", "", r["Max Loss"], "", r["Server"], "", r["Telegram ID(s)"], "", r["Algo"], "ALL"])

    for uid in removed_ids:
        r = last_idx.loc[uid]
        rows.append([uid, "REMOVED", r["Max Loss"], "", r["Server"], "", r["Telegram ID(s)"], "", r["Algo"], "", "ALL"])

    for uid in common_ids:
        a, b = last_idx.loc[uid], latest_idx.loc[uid]
        diffs = []
        maxloss_equal = pd.isna(a["_MaxLoss_num"]) and pd.isna(b["_MaxLoss_num"])
        if not maxloss_equal:
            if (pd.isna(a["_MaxLoss_num"]) != pd.isna(b["_MaxLoss_num"])) or \
               (not pd.isna(a["_MaxLoss_num"]) and not pd.isna(b["_MaxLoss_num"]) and float(a["_MaxLoss_num"]) != float(b["_MaxLoss_num"])) :
                diffs.append("Max Loss")
        if str(a["Server"]) != str(b["Server"]):
            diffs.append("Server")
        if str(a["Telegram ID(s)"]) != str(b["Telegram ID(s)"]):
            diffs.append("Telegram ID(s)")
        if str(a["Algo"]) != str(b["Algo"]):
            diffs.append("Algo")
        if diffs:
            rows.append([
                uid, "MODIFIED",
                a["Max Loss"], b["Max Loss"],
                a["Server"], b["Server"],
                a["Telegram ID(s)"], b["Telegram ID(s)"],
                a["Algo"], b["Algo"],
                ", ".join(diffs),
            ])

    all_diffs = pd.DataFrame(rows, columns=cols_out)
    return (
        all_diffs[all_diffs["Change Type"] == "ADDED"].reset_index(drop=True),
        all_diffs[all_diffs["Change Type"] == "REMOVED"].reset_index(drop=True),
        all_diffs[all_diffs["Change Type"] == "MODIFIED"].reset_index(drop=True),
        all_diffs.reset_index(drop=True)
    )

# ====================== STREAMLIT UI ======================
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

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

# -------- Mode 1: Compile from Drive --------
if mode == "Compile from Google Drive":
    st.subheader("Compile CSVs in a Drive folder into `Compiled_User_Settings.xlsx`")
    link = st.text_input("Paste Google Drive folder link (or folder ID)")

    c1, c2 = st.columns(2)
    with c1:
        skiprows = st.number_input("CSV header lines to skip before real header", min_value=0, max_value=50, value=6, step=1)
    with c2:
        out_name = st.text_input("Output filename", value="Compiled_User_Settings.xlsx")

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
            st.download_button(
                "⬇️ Download Compiled_User_Settings.xlsx",
                data=xbytes,
                file_name=out_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

            with st.expander("Preview (first 10 rows)"):
                st.write("**Specified_Compiled**")
                st.dataframe(specified.head(10), use_container_width=True)
                st.write("**Summary**")
                st.dataframe(summary, use_container_width=True)

            with st.expander("Processed files"):
                st.write("\n".join([f["name"] for f in csv_files]))

        except FileNotFoundError as e:
            st.error(str(e))
        except HttpError as e:
            st.error(f"Google API error: {e}")
        except Exception as e:
            st.exception(e)

# -------- Mode 2: Compare Latest vs Last (Sheet1) --------
else:
    st.subheader("Compare LATEST compiled (`Specified_Compiled`) vs LAST workbook (`Sheet1`)")

    col1, col2 = st.columns(2)
    with col1:
        f_last = st.file_uploader("Upload **Last Workbook** (.xlsx) — must contain Sheet1", type=["xlsx"], key="last_file")
    with col2:
        f_latest = st.file_uploader("Upload **Latest Compiled** (.xlsx) — uses Specified_Compiled", type=["xlsx"], key="latest_file")

    if f_last and f_latest:
        last_bytes = f_last.read()
        latest_bytes = f_latest.read()

        try:
            # Read & normalize
            last_df   = read_sheet1_last(last_bytes)        # from Sheet1 schema
            latest_df = clean_for_compare(read_specified_compiled(latest_bytes))  # from Specified_Compiled

            st.success("Loaded: LAST from Sheet1, LATEST from Specified_Compiled.")

            added_df, removed_df, modified_df, all_diffs = compare_frames(last_df, latest_df)

            k1, k2, k3, k4 = st.columns(4)
            with k1: st.metric("Added", len(added_df))
            with k2: st.metric("Removed", len(removed_df))
            with k3: st.metric("Modified", len(modified_df))
            with k4: st.metric("Total Differences", len(all_diffs))

            st.markdown("---")
            tabs = st.tabs(["All Differences", "Added", "Removed", "Modified"])
            with tabs[0]:
                st.dataframe(all_diffs, use_container_width=True)
            with tabs[1]:
                st.dataframe(added_df, use_container_width=True)
            with tabs[2]:
                st.dataframe(removed_df, use_container_width=True)
            with tabs[3]:
                st.dataframe(modified_df, use_container_width=True)

            xbytes = to_excel_bytes({
                "All_Differences": all_diffs,
                "Added": added_df,
                "Removed": removed_df,
                "Modified": modified_df
            })
            st.download_button(
                "⬇️ Download Differences (Excel)",
                data=xbytes,
                file_name="user_settings_differences.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

            with st.expander("Preview: Latest (Specified_Compiled) & Last (Sheet1) — first 10 rows"):
                st.write("**Latest (cleaned)**")
                st.dataframe(latest_df.head(10), use_container_width=True)
                st.write("**Last (cleaned)**")
                st.dataframe(last_df.head(10), use_container_width=True)

        except ValueError as ve:
            st.error(str(ve))
        except Exception as e:
            st.exception(e)
    else:
        st.info("Upload both Excel files to start.")

