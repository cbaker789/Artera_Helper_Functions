from __future__ import annotations

import re
import os
import sys
import socket
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ============================
# Column alias dictionaries
# ============================

COLUMN_ALIASES: Dict[str, List[str]] = {
    "full_name": ["name", "patient name", "member name", "full name"],
    "first_name": ["first name", "given name", "patient first name", "first_name"],
    "last_name": ["last name", "surname", "family name", "patient last name","last_name"],
    "dob": ["dob", "date of birth", "birthdate", "birth date"],
    "mrn": ["mrn", "person id", "patient id", "medical record number", "chart number", "member id"],
    "gender": ["sex at birth", "gender", "birth sex", "assigned sex at birth", "biological sex"],
    "phone": ["phone", "cell", "cell phone", "mobile", "mobile phone", "primary phone", "person phone"],
    "email": ["email", "email address", "person email", "patient email"],
    "language": ["language", "preferred language", "person language", "primary language"],
    "home_phone": ["home phone"],
    "work_phone": ["work phone"],
    "middle_name": ["middle name", "mid name", "middle initial"],
}

# ============================
# Utilities
# ============================

import tkinter as tk
from tkinter import filedialog

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())

def _best_match_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    norm_to_orig = {_norm(c): c for c in df.columns.astype(str)}
    cand_norm = [_norm(c) for c in candidates]
    for c in cand_norm:  # exact
        if c in norm_to_orig:
            return norm_to_orig[c]
    for norm_col, orig in norm_to_orig.items():  # contains
        for c in cand_norm:
            if c and c in norm_col:
                return orig
    return None

def infer_column_map(df: pd.DataFrame, extra_aliases: Optional[Dict[str, List[str]]] = None) -> Dict[str, Optional[str]]:
    alias = COLUMN_ALIASES.copy()
    if extra_aliases:
        for k, v in extra_aliases.items():
            alias[k] = list({*alias.get(k, []), *v})
    mapping: Dict[str, Optional[str]] = {}
    for key, cand in alias.items():
        mapping[key] = _best_match_column(df, cand)
    return mapping

def _split_full_name(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    s = series.astype(str)
    parts = s.str.split(r",\s*", n=1, expand=True)
    if parts.shape[1] == 2:
        last = parts[0].fillna("")
        first = parts[1].fillna("")
    else:
        toks = s.str.split(r"\s+")
        first = toks.str[:-1].str.join(" ").fillna("")
        last = toks.str[-1].fillna("")
    return first, last

def _to_yyyymmdd(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%Y%m%d")

# ==================================================
# Core normalizer: DataFrame -> Artera schema DF
# ==================================================

def build_artera_upload_from_df(
    df: pd.DataFrame,
    column_map: Optional[Dict[str, Optional[str]]] = None,
    *,
    language_recode: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError("Input DataFrame is empty.")

    work = df.copy()
    if column_map is None:
        column_map = infer_column_map(work)

    dob_col = column_map.get("dob")
    mrn_col = column_map.get("mrn")
    first_col = column_map.get("first_name")
    last_col = column_map.get("last_name")
    full_col = column_map.get("full_name")

    if not dob_col or not mrn_col:
        raise KeyError(f"Missing required columns (DOB/MRN). Inferred mapping: {column_map}")

    if not (first_col and last_col):
        if not full_col:
            raise KeyError("Need either ('First Name' & 'Last Name') or a single 'Name' column to split.")
        work["__first"], work["__last"] = _split_full_name(work[full_col])
        first_col, last_col = "__first", "__last"

    phone_col = column_map.get("phone")
    home_phone_col = column_map.get("home_phone")
    work_phone_col = column_map.get("work_phone")
    email_col = column_map.get("email")
    lang_col = column_map.get("language")
    gender_col = column_map.get("gender")
    mid_col = column_map.get("middle_name")

    if language_recode and lang_col and lang_col in work.columns:
        work[lang_col] = work[lang_col].replace(language_recode)

    upload = pd.DataFrame({
        "personLastName": work[last_col],
        "personMidName": work[mid_col] if mid_col and mid_col in work.columns else pd.NA,
        "personFirstName": work[first_col],
        "personCellPhone": work[phone_col] if phone_col and phone_col in work.columns else pd.NA,
        "personHomePhone": work[home_phone_col] if home_phone_col and home_phone_col in work.columns else pd.NA,
        "personWorkPhone": work[work_phone_col] if work_phone_col and work_phone_col in work.columns else pd.NA,
        "personPrefLanguage": work[lang_col] if lang_col and lang_col in work.columns else pd.NA,
        "dob": _to_yyyymmdd(work[dob_col]),
        "gender": work[gender_col] if gender_col and gender_col in work.columns else pd.NA,
        "personID": work[mrn_col].astype(str),
        "PersonEmail": work[email_col] if email_col and email_col in work.columns else pd.NA,
    })
    return upload

# =========================================================
# Excel crawler: Excel path -> infer -> normalize -> CSV
# =========================================================

def build_artera_upload_from_excel(
    xlsx_path: str | Path,
    *,
    sheet_name: Optional[str] = None,
    extra_aliases: Optional[Dict[str, List[str]]] = None,
    language_recode: Optional[Dict[str, str]] = None,
    csv_outdir: str | Path = ".",
    file_prefix: str = "SBNC_Outreach_",
    today: Optional[datetime] = None,
) -> Dict[str, object]:
    xlsx_path = Path(xlsx_path)
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Excel file not found: {xlsx_path}")

    if today is None:
        today = datetime.today()
    stamp = today.strftime("%Y%m%d")

    if sheet_name:
        frames = {sheet_name: pd.read_excel(xlsx_path, sheet_name=sheet_name)}
    else:
        frames = pd.read_excel(xlsx_path, sheet_name=None)

    best_sheet = None
    best_df = None
    best_score = -1
    best_map = None

    for sname, df in frames.items():
        df = df.copy()
        df.columns = [str(c) for c in df.columns]
        cmap = infer_column_map(df, extra_aliases=extra_aliases)

        score = 0
        for k in ("dob", "mrn"):
            if cmap.get(k):
                score += 3
        if cmap.get("first_name") and cmap.get("last_name"):
            score += 2
        elif cmap.get("full_name"):
            score += 1

        if score > best_score:
            best_score = score
            best_sheet = sname
            best_df = df
            best_map = cmap

    if best_df is None:
        raise ValueError("No suitable sheet found (need DOB and MRN present).")

    upload = build_artera_upload_from_df(best_df, column_map=best_map, language_recode=language_recode)

    csv_outdir = Path(csv_outdir)
    csv_outdir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_outdir / f"{file_prefix}{today.strftime('%Y%m%d')}.csv"
    upload.to_csv(csv_path, index=False)

    return {
        "upload": upload,
        "column_map": best_map,
        "sheet_name": best_sheet,
        "csv_path": str(csv_path),
    }

# ============================
# Robust path resolver
# ============================

def _resolve_xlsx_path(user_input: str) -> Path:
    raw = (user_input or "").strip().strip('"').strip("'")
    if not raw:
        raise FileNotFoundError("No path provided.")
    # Fix "C:\Users\Desktop\..." typo (missing username)
    if re.match(r"^[A-Za-z]:\\Users\\Desktop(\\|$)", raw):
        raw = str(Path.home() / raw.split("\\Users\\Desktop\\", 1)[1])

    p_in = Path(raw)
    if p_in.suffix == "":
        p_in = p_in.with_suffix(".xlsx")

    home = Path.home()
    name = p_in.name
    candidates: List[Path] = []

    candidates.append(p_in)
    candidates.append(Path(raw).expanduser())
    if not p_in.is_absolute():
        candidates.append(home / p_in)

    parts = Path(raw).parts
    if parts and parts[0].lower() == "desktop":
        after_desktop = Path(*parts[1:]) if len(parts) > 1 else Path(name)
        if after_desktop.suffix == "":
            after_desktop = after_desktop.with_suffix(".xlsx")
        candidates.append(home / "Desktop" / after_desktop)

    for od in home.glob("OneDrive*/Desktop"):
        candidates.append(od / name)
        if parts and parts[0].lower() == "desktop":
            candidates.append(od / after_desktop)

    seen = set()
    uniq: List[Path] = []
    for c in candidates:
        try:
            key = str(c.resolve(strict=False)).lower()
        except Exception:
            key = str(c).lower()
        if key not in seen:
            seen.add(key)
            uniq.append(c)

    for c in uniq:
        if c.expanduser().exists():
            return c.expanduser()

    tried = "\n  - " + "\n  - ".join(str(c.expanduser()) for c in uniq)
    raise FileNotFoundError(f"Excel file not found. Paths tried:{tried}")

# ============================
# Simple file picker
# ============================

def choose_excel_file() -> Optional[str]:
    try:
        root = tk.Tk()
        root.withdraw()
        try:
            root.call('wm', 'attributes', '.', '-topmost', True)
        except Exception:
            pass
        path = filedialog.askopenfilename(
            title="Select Excel file",
            initialdir=str(Path.home() / "Desktop"),
            filetypes=[("Excel files", "*.xlsx *.xlsm *.xlsb *.xls"), ("All files", "*.*")]
        )
        root.destroy()
        return str(Path(path).resolve()) if path else None
    except Exception:
        return None

# ============================
# SFTP helpers (Paramiko)
# ============================

# pip install paramiko
import paramiko
from stat import S_ISDIR

def _print_or_verify_hostkey(transport: paramiko.Transport, known_fingerprint: Optional[str] = None):
    key = transport.get_remote_server_key()
    fp = key.get_fingerprint().hex(":")
    print(f"🔐 Server host key fingerprint: {fp}")
    if known_fingerprint:
        if fp.lower() != known_fingerprint.lower():
            raise RuntimeError(
                f"Server fingerprint mismatch!\nExpected: {known_fingerprint}\nGot:      {fp}"
            )

def _ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir: str):
    """
    Recursively create remote_dir if it doesn't exist.
    """
    if remote_dir in ("", "/", "."):
        return
    parts = []
    p = Path(remote_dir.replace("\\", "/"))
    for part in p.parts:
        parts.append(part)
        current = "/".join(parts).replace("//", "/")
        if current in ("", "/"):
            continue
        try:
            attr = sftp.stat(current)
            if not S_ISDIR(attr.st_mode):
                raise NotADirectoryError(f"Remote path exists and is not a directory: {current}")
        except FileNotFoundError:
            sftp.mkdir(current)

def _sftp_put_with_progress(sftp: paramiko.SFTPClient, local_path: str, remote_path: str):
    size = Path(local_path).stat().st_size
    sent = 0

    def _cb(bytes_sent, total):
        nonlocal sent
        sent = bytes_sent
        # simple single-line progress
        pct = 0 if total == 0 else int((bytes_sent / total) * 100)
        sys.stdout.write(f"\r⬆️  Uploading {Path(local_path).name}: {pct}% ({bytes_sent}/{total} bytes)")
        sys.stdout.flush()

    sftp.put(local_path, remote_path, callback=lambda b, t=size: _cb(b, t))
    sys.stdout.write("\n")

def upload_via_sftp(
    local_file: str,
    *,
    host: Optional[str] = None,
    port: int = 22,
    username: Optional[str] = None,
    password: Optional[str] = None,
    pkey_path: Optional[str] = None,
    pkey_passphrase: Optional[str] = None,
    remote_dir: str = "/",
    known_fingerprint: Optional[str] = None,
    timeout: int = 20,
):
    """
    Uploads `local_file` to SFTP server.
    - If pkey_path is provided, key auth is used; otherwise password auth is used.
    - Optionally verify server host key via `known_fingerprint` (hex with colons).
    """
    local_file = str(Path(local_file).resolve())

    # Allow env overrides (useful for unattended runs)
    host = host or os.getenv("SFTP_HOST")
    username = username or os.getenv("SFTP_USER")
    password = password or os.getenv("SFTP_PASSWORD")
    pkey_path = pkey_path or os.getenv("SFTP_PKEY")
    pkey_passphrase = pkey_passphrase or os.getenv("SFTP_PKEY_PASSPHRASE")
    if not host or not username:
        raise ValueError("Missing SFTP host/username. Provide args or set SFTP_HOST/SFTP_USER env vars.")

    # Build transport, authenticate
    addr = (host, port)
    sock = socket.create_connection(addr, timeout=timeout)
    transport = paramiko.Transport(sock)
    transport.connect(None)  # start kex
    _print_or_verify_hostkey(transport, known_fingerprint)

    pkey = None
    if pkey_path:
        pkey_path = str(Path(pkey_path).expanduser())
        try:
            pkey = paramiko.RSAKey.from_private_key_file(pkey_path, password=pkey_passphrase)
        except paramiko.ssh_exception.SSHException:
            # Try other key types
            try:
                pkey = paramiko.Ed25519Key.from_private_key_file(pkey_path, password=pkey_passphrase)
            except Exception:
                pkey = paramiko.ECDSAKey.from_private_key_file(pkey_path, password=pkey_passphrase)

    transport.auth_publickey(username, pkey) if pkey else transport.auth_password(username, password)

    with paramiko.SFTPClient.from_transport(transport) as sftp:
        _ensure_remote_dir(sftp, remote_dir)
        remote_path = (Path(remote_dir.replace("\\", "/")) / Path(local_file).name).as_posix()
        _sftp_put_with_progress(sftp, local_file, remote_path)
        print(f"✅ Uploaded to sftp://{host}{remote_path}")

    transport.close()

# ============================
# Main (picker first + Desktop out + SFTP)
# ============================

def _desktop_outdir() -> Path:
    d = Path.home() / "Desktop" / "Artera SFTP Uploads"
    d.mkdir(parents=True, exist_ok=True)
    return d

if __name__ == "__main__":
    try:
        print("=== Artera Upload Builder ===")

        # Choose Excel
        xlsx_path_str = choose_excel_file()
        if not xlsx_path_str:
            print("📎 File picker unavailable or canceled.")
            user_in = input("📂 Paste the Excel path: ").strip()
            xlsx_path = _resolve_xlsx_path(user_in)
        else:
            xlsx_path = Path(xlsx_path_str)
            if not xlsx_path.exists():
                xlsx_path = _resolve_xlsx_path(xlsx_path_str)

        sheet = input("🗂️  Optional sheet name (Enter = auto): ").strip() or None

        outdir = _desktop_outdir()  # Always Desktop/Artera SFTP Uploads
        print(f"📁 Output will be saved to: {outdir}")

        prefix = "SBNC_Outreach_"  # fixed prefix per your preference
        language_recode = {"Spanish; Castilian": "Spanish"}

        result = build_artera_upload_from_excel(
            xlsx_path=xlsx_path,
            sheet_name=sheet,
            csv_outdir=outdir,
            file_prefix=prefix,
            language_recode=language_recode,
        )

        print("\n✅ Upload CSV created successfully!")
        print(f"   Saved to: {result['csv_path']}")
        print(f"   Sheet used: {result['sheet_name']}")
        print("   Inferred column map:")
        for k, v in result["column_map"].items():
            print(f"     {k:15} -> {v}")

        # ---------- SFTP UPLOAD ----------
        print("\n=== SFTP Upload ===")
        # Pull defaults from env if available; otherwise prompt.
        host = os.getenv("SFTP_HOST") or input("🔌 SFTP host (e.g., sftp.arterahealth.com): ").strip()
        port_str = os.getenv("SFTP_PORT") or input("🔢 SFTP port [22]: ").strip() or "22"
        try:
            port = int(port_str)
        except ValueError:
            port = 22

        username = os.getenv("SFTP_USER") or input("👤 Username: ").strip()

        auth_mode = (os.getenv("SFTP_AUTH") or input("🔑 Auth mode [password/key]: ").strip().lower() or "password")
        password = None
        pkey_path = None
        pkey_pass = None

        if auth_mode.startswith("key"):
            pkey_path = os.getenv("SFTP_PKEY") or input("📄 Path to private key (e.g., ~/.ssh/id_rsa): ").strip()
            pkey_pass = os.getenv("SFTP_PKEY_PASSPHRASE") or (input("🔏 Key passphrase (Enter if none): ") or None)
        else:
            password = os.getenv("SFTP_PASSWORD") or input("🔒 Password: ").strip()

        remote_dir = os.getenv("SFTP_REMOTE_DIR") or input("📂 Remote directory (e.g., /uploads/artera): ").strip() or "/"
        known_fp = os.getenv("SFTP_FINGERPRINT") or (input("🧾 Server fingerprint (optional, colon-hex): ").strip() or None)

        upload_via_sftp(
            local_file=result["csv_path"],
            host=host,
            port=port,
            username=username,
            password=password,
            pkey_path=pkey_path,
            pkey_passphrase=pkey_pass,
            remote_dir=remote_dir,
            known_fingerprint=known_fp,
        )

        print("🎉 All done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
