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

# ============================ CONFIG ============================
APP_TITLE = "User Settings — Compile from Drive & Compare"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
TOKEN_PATH = "token.json"
CLIENT_SECRETS_FILE = "client_secrets.json"

# Updated Server -> (Operator, Algo)
SERVER_ALGO_MAP = {
    "VS1": {"Operator": "CHETANB", "Algo": 8},
    "VS10": {"Operator": "VIKASA", "Algo": 1},
    "VS11": {"Operator": "GULSHANS", "Algo": 7},
    "VS12": {"Operator": "BANSHIP", "Algo": 1},
    "VS13": {"Operator": "VIKASA", "Algo": 1},
    "VS14": {"Operator": "SAHILM", "Algo": 15},
    "VS15": {"Operator": "BANSHIP", "Algo": 1},
    "VS16": {"Operator": "GAURAVK", "Algo": 7},
    "VS2": {"Operator": "BANSHIP", "Algo": 1},
    "VS3": {"Operator": "PIYUSHM", "Algo": 2},
    "VS4": {"Operator": "ASHUTOSHM", "Algo": 102},
    "VS5": {"Operator": "SAHILM", "Algo": 15},
    "VS6": {"Operator": "CHETANB", "Algo": 5},
    "VS7": {"Operator": "GAURAVK", "Algo": 7},
    "VS8": {"Operator": "ASHUTOSHM", "Algo": 102},
    "VS9": {"Operator": "GULSHANS", "Algo": 7},
    "VS19": {"Operator": "PRADYUMANS", "Algo": 15},
    "NO SERVER": {"Operator": "A12/A18", "Algo": "12/18"},
    "NOT RUNNING": {"Operator": "NOT RUNNING", "Algo": 0},
    "CC_A1_GSPL6_DEALER": {"Operator": "GSPLDEALER", "Algo": 8},
    "CC_GSPL_DEAL": {"Operator": "GSPLDEALER", "Algo": 8},
    "DEALER_GSPL15": {"Operator": "GSPLDEALER", "Algo": 15},
    "CC_SISL_GS_DEALER": {"Operator": "GSPLDEALER", "Algo": 8},
    "GSPLDEALER": {"Operator": "GSPLDEALER", "Algo": 7},
}

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
    "algo": "Algo",
    "operator": "Operator",
}
SPECIFIED_ORDER = ["User Alias", "User ID", "Broker", "Max Loss", "Server", "Telegram ID(s)", "Algo"]
COMPARE_COLS = ["User ID", "Max Loss", "Server", "Telegram ID(s)", "Algo"]
SHEET_TO_COMPARE = "Specified_Compiled"

# ====================== COMMON HELPERS ======================
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for col in df.columns:
        key = str(col).strip().lower().replace("\n", " ").replace("\r", " ")
        key = " ".join(key.split())
        key_compact = key.replace("_", " ").replace("-", " ")
        if key in CANONICAL_NAMES:
            rename_map[col] = CANONICAL_NAMES[key]
        elif key_compact in CANONICAL_NAMES:
            rename_map[col] = CANONICAL_NAMES[key_compact]
        else:
            key_tight = "".join(key_compact.split())
            if key_tight in CANONICAL_NAMES:
                rename_map[col] = CANONICAL_NAMES[key_tight]
    return df.rename(columns=rename_map)

def ensure_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    return df

def extract_folder_id(link: str) -> str:
    """
    Robustly extract a Drive folder id from various share link formats.
    """
    link = link.strip()
    # patterns: .../folders/<id>, .../drive/folders/<id>, ...id=<id>
    m = re.search(r"/folders/([A-Za-z0-9_\-]+)", link)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([A-Za-z0-9_\-]+)", link)
    if m:
        return m.group(1)
    # fallback: if user pasted just the id
    if re.fullmatch(r"[A-Za-z0-9_\-]{20,}", link):
        return link
    raise ValueError("Could not extract folder id from the provided link/id.")

def to_excel_bytes(sheet_map: dict) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for name, df in sheet_map.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    bio.seek(0)
    return bio.read()

# ====================== DRIVE AUTH & IO ======================
def authenticate_drive() -> "googleapiclient.discovery.Resource":
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if not os.path.exists(CLIENT_SECRETS_FILE):
            raise FileNotFoundError(
                f"Missing '{CLIENT_SECRETS_FILE}'. Place your OAuth client file next to app.py."
            )
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        # Using a random local port; browser will open a new tab
        creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def list_files_in_folder(service, folder_id: str) -> List[dict]:
    """
    List ALL files in a Drive folder (handles pagination). We will filter .csv by name.
    """
    files = []
    page_token = None
    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
            pageSize=1000,
            pageToken=page_token
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
    df = pd.read_csv(fh, skiprows=skiprows)
    return df

# ====================== COMPILER (Drive -> Excel) ======================
def process_csv_files(service, files: List[dict], skiprows: int = 6) -> pd.DataFrame:
    all_data = []
    for f in files:
        # Only .csv by filename (Drive mime can be unreliable)
        if not str(f["name"]).lower().endswith(".csv"):
            continue
        try:
            df = download_csv_as_df(service, f["id"], skiprows=skiprows)
            df = normalize_columns(df)

            server = str(f["name"]).split()[0]
            algo_info = SERVER_ALGO_MAP.get(server, {"Operator": "", "Algo": ""})

            df["Server"] = server
            df["Operator"] = algo_info["Operator"]
            df["Algo"] = algo_info["Algo"]

            df = ensure_columns(df, ["User Alias", "User ID", "Broker", "Max Loss",
                                     "Server", "Telegram ID(s)", "Algo", "Operator"])

            # Filter out DEAL / DEALER / FEED in User Alias (case-insensitive)
            mask = df["User Alias"].astype(str).str.contains("DEAL|FEED", case=False, na=False)
            df = df[~mask]

            all_data.append(df)
        except Exception as e:
            st.warning(f"Skipping '{f['name']}' due to error: {e}")
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
    out = []
    for c in cols:
        c = str(c).replace("\n", " ").replace("\r", " ")
        c = " ".join(c.split())
        out.append(c)
    return out

def clean_for_compare(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = _normalize_column_names(df.columns)
    missing = [c for c in COMPARE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{SHEET_TO_COMPARE}' is missing columns: {missing}")
    df = df[COMPARE_COLS].copy()

    df["User ID"] = df["User ID"].astype(str)
    df["Server"] = df["Server"].astype(str)
    df["Algo"] = df["Algo"].astype(str)
    df["Telegram ID(s)"] = df["Telegram ID(s)"].astype(str)

    for c in ["Server", "Telegram ID(s)", "Algo"]:
        df[c] = df[c].fillna("")

    numeric_max_loss = pd.to_numeric(df["Max Loss"], errors="coerce")
    df["_MaxLoss_num"] = numeric_max_loss.fillna(pd.NA)
    df["Max Loss"] = df["Max Loss"].apply(lambda x: "" if pd.isna(x) else str(x))

    df = df.sort_index().drop_duplicates(subset=["User ID"], keep="last").reset_index(drop=True)
    return df

def read_specified_compiled(xlsx_bytes: bytes) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=SHEET_TO_COMPARE, engine="openpyxl")

def compare_frames(last_df: pd.DataFrame, latest_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    last_idx = last_df.set_index("User ID")
    latest_idx = latest_df.set_index("User ID")
    last_ids = set(last_idx.index)
    latest_ids = set(latest_idx.index)

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
        a = last_idx.loc[uid]
        b = latest_idx.loc[uid]
        diffs = []

        maxloss_equal = pd.isna(a["_MaxLoss_num"]) and pd.isna(b["_MaxLoss_num"])
        if not maxloss_equal:
            if (pd.isna(a["_MaxLoss_num"]) != pd.isna(b["_MaxLoss_num"])) or \
               (not pd.isna(a["_MaxLoss_num"]) and not pd.isna(b["_MaxLoss_num"]) and float(a["_MaxLoss_num"]) != float(b["_MaxLoss_num"])):
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
    return (all_diffs.query("`Change Type`=='ADDED'").reset_index(drop=True),
            all_diffs.query("`Change Type`=='REMOVED'").reset_index(drop=True),
            all_diffs.query("`Change Type`=='MODIFIED'").reset_index(drop=True),
            all_diffs.reset_index(drop=True))

# ====================== STREAMLIT UI ======================
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

mode = st.sidebar.radio("Mode", ["Compile from Google Drive", "Compare two compiled files"], index=0)

# Optional: quick auth reset
with st.sidebar.expander("Google Auth"):
    if st.button("🔁 Reset Google token (sign in again)"):
        try:
            if os.path.exists(TOKEN_PATH):
                os.remove(TOKEN_PATH)
                st.success("Deleted token.json. You will be asked to sign in next time.")
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

            # Process with a progress bar
            processed = []
            all_df = []
            progress = st.progress(0.0, text="Starting…")
            for i, f in enumerate(csv_files, start=1):
                try:
                    df = download_csv_as_df(service, f["id"], skiprows=skiprows)
                    df = normalize_columns(df)
                    server = str(f["name"]).split()[0]
                    algo_info = SERVER_ALGO_MAP.get(server, {"Operator": "", "Algo": ""})
                    df["Server"] = server
                    df["Operator"] = algo_info["Operator"]
                    df["Algo"] = algo_info["Algo"]
                    df = ensure_columns(df, ["User Alias", "User ID", "Broker", "Max Loss", "Server", "Telegram ID(s)", "Algo", "Operator"])
                    mask = df["User Alias"].astype(str).str.contains("DEAL|FEED", case=False, na=False)
                    df = df[~mask]
                    all_df.append(df)
                    processed.append(f["name"])
                except Exception as e:
                    st.warning(f"Skipping '{f['name']}': {e}")
                progress.progress(i / max(1, len(csv_files)), text=f"Processed {i}/{len(csv_files)}")

            compiled_df = pd.concat(all_df, ignore_index=True) if all_df else pd.DataFrame()
            if compiled_df.empty:
                st.error("No valid data after processing.")
                st.stop()

            # Operator_Compiled
            operator_compiled = compiled_df.copy()

            # Specified_Compiled
            specified = compiled_df.copy()
            specified = ensure_columns(specified, SPECIFIED_ORDER)
            specified = specified[SPECIFIED_ORDER]

            # Summary
            summary = generate_summary(compiled_df)

            # Download
            xbytes = to_excel_bytes({
                "Operator_Compiled": operator_compiled,
                "Specified_Compiled": specified,
                "Summary": summary
            })
            st.success(f"Built workbook from {len(processed)} CSV file(s).")
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
                st.write("\n".join(processed))

        except FileNotFoundError as e:
            st.error(str(e))
        except HttpError as e:
            st.error(f"Google API error: {e}")
        except Exception as e:
            st.exception(e)

# -------- Mode 2: Compare two compiled files --------
else:
    st.subheader("Compare `Specified_Compiled` across two compiled workbooks")

    col1, col2 = st.columns(2)
    with col1:
        f_last = st.file_uploader("Upload **Last User Settings** (.xlsx)", type=["xlsx"], key="last_file")
    with col2:
        f_latest = st.file_uploader("Upload **Latest User Settings** (.xlsx)", type=["xlsx"], key="latest_file")

    if f_last and f_latest:
        choice = st.selectbox(
            "Which file is the **Latest**?",
            options=[f_last.name, f_latest.name],
            index=1
        )
        latest_bytes = f_last.read() if choice == f_last.name else f_latest.read()
        last_bytes   = f_latest.read() if choice == f_last.name else f_last.read()

        try:
            last_raw = read_specified_compiled(last_bytes)
            latest_raw = read_specified_compiled(latest_bytes)

            last_df = clean_for_compare(last_raw)
            latest_df = clean_for_compare(latest_raw)

            st.success(f"Loaded sheet '{SHEET_TO_COMPARE}' from both files.")

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

            with st.expander("Preview: Latest & Last (first 10 rows)"):
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
