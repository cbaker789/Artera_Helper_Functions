from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ============================
# Column alias dictionaries
# ============================

COLUMN_ALIASES: Dict[str, List[str]] = {
    "full_name": ["Name","name", "patient name", "member name", "full name"],
    "first_name": ["first name", "given name", "patient first name", "first_name"],
    "last_name": ["last name", "surname", "family name", "patient last name","last_name"],
    "dob": ["dob", "date of birth", "birthdate", "birth date", "Date_of_birth"],
    "mrn": ["mrn", "person id", "patient id", "medical record number", "chart number", "member id"],
    "gender": ["Sex","sex at birth", "gender", "birth sex", "assigned sex at birth", "biological sex"],
    "phone": ["phone", "cell", "cell phone", "mobile", "mobile phone", "primary phone", "person phone"],
    "email": ["email", "email address", "person email", "patient email"],
    "language": ["language", "preferred language", "person language", "primary language"],
    # Optional extras:
    "home_phone": ["home phone"],
    "work_phone": ["work phone"],
    "middle_name": ["middle name", "mid name", "middle initial"],
}

# ============================
# Utilities
# ============================

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())

def _best_match_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Return the original column name from df that best matches the candidate list.
    Strategy:
      1) exact (normalized) match
      2) contains match (candidate token contained in normalized column)
    """
    if df is None or df.empty:
        return None

    norm_to_orig = {_norm(c): c for c in df.columns.astype(str)}
    cand_norm = [_norm(c) for c in candidates]

    # exact match
    for c in cand_norm:
        if c in norm_to_orig:
            return norm_to_orig[c]

    # contains match
    for norm_col, orig in norm_to_orig.items():
        for c in cand_norm:
            if c and c in norm_col:
                return orig

    return None

def infer_column_map(
    df: pd.DataFrame,
    extra_aliases: Optional[Dict[str, List[str]]] = None
) -> Dict[str, Optional[str]]:
    alias = COLUMN_ALIASES.copy()
    if extra_aliases:
        for k, v in extra_aliases.items():
            alias[k] = list({*alias.get(k, []), *v})

    mapping: Dict[str, Optional[str]] = {}
    for key, cand in alias.items():
        mapping[key] = _best_match_column(df, cand)
    return mapping

def _split_full_name(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Split a single 'Name' column into first/last.
    Prefers 'LAST, First'; falls back to space-split with last token as last name.
    """
    s = series.astype(str)

    # Try LAST, First
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
    """Coerce date-like strings to YYYYMMDD (string). Invalid -> <NA>."""
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
    """
    Normalize a DataFrame into the Artera SFTP CSV schema by inferring or using a provided column map.

    Output columns:
      personLastName, personMidName, personFirstName,
      personCellPhone, personHomePhone, personWorkPhone,
      personPrefLanguage, dob, gender, personID, PersonEmail
    """
    if df is None or df.empty:
        raise ValueError("Input DataFrame is empty.")

    work = df.copy()
    if column_map is None:
        column_map = infer_column_map(work)

    # Required: DOB + MRN, and either (first+last) OR (full_name)
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

    # Optional fields
    phone_col = column_map.get("phone")
    home_phone_col = column_map.get("home_phone")
    work_phone_col = column_map.get("work_phone")
    email_col = column_map.get("email")
    lang_col = column_map.get("language")
    gender_col = column_map.get("gender")
    mid_col = column_map.get("middle_name")

    # Optional language recode
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
    """
    Crawl an Excel file (optionally a specific sheet), infer columns, normalize to the Artera schema,
    and dump a CSV. Returns: {'upload', 'column_map', 'sheet_name', 'csv_path'}.
    """
    xlsx_path = Path(xlsx_path)
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Excel file not found: {xlsx_path}")

    if today is None:
        today = datetime.today()
    stamp = today.strftime("%Y%m%d")

    # Load sheet(s)
    if sheet_name:
        frames = {sheet_name: pd.read_excel(xlsx_path, sheet_name=sheet_name)}
    else:
        frames = pd.read_excel(xlsx_path, sheet_name=None)

    # Pick best sheet by presence of DOB/MRN + names
    best_sheet = None
    best_df = None
    best_score = -1
    best_map = None

    scoring_keys = ["dob", "mrn"]

    for sname, df in frames.items():
        df = df.copy()
        df.columns = [str(c) for c in df.columns]
        cmap = infer_column_map(df, extra_aliases=extra_aliases)

        score = 0
        for k in scoring_keys:
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

    # Normalize & export
    upload = build_artera_upload_from_df(best_df, column_map=best_map, language_recode=language_recode)

    csv_outdir = Path(csv_outdir)
    csv_outdir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_outdir / f"{file_prefix}{stamp}.csv"
    upload.to_csv(csv_path, index=False)

    return {
        "upload": upload,
        "column_map": best_map,
        "sheet_name": best_sheet,
        "csv_path": str(csv_path),
    }

# ============================
# File picking & path resolve
# ============================

import tkinter as tk
from tkinter import filedialog

def pick_excel_path() -> str:
    try:
        root = tk.Tk()
        root.withdraw()
        path = filedialog.askopenfilename(
            title="Select Excel file",
            filetypes=[("Excel files", "*.xlsx *.xlsm *.xlsb *.xls"), ("All files", "*.*")]
        )
        return path or ""
    except Exception:
        return ""

def _resolve_xlsx_path(user_input: str) -> Path:
    """
    Resolve a user-entered Excel path robustly:
      - trims/strips quotes and whitespace
      - preserves provided extension; if none, appends .xlsx
      - expands ~
      - fixes malformed 'C:\\Users\\Desktop\\...' by rewriting to HOME\\Desktop\\...
      - collapses duplicated '\\Users\\<you>\\Users\\<you>\\...' segments
      - if relative like 'Desktop\\file', resolves against HOME/Desktop
      - tries common OneDrive Desktop locations
      - deduplicates candidates and returns the first existing
    Raises FileNotFoundError listing all tried paths on failure.
    """
    raw = (user_input or "").strip().strip('"').strip("'")
    if not raw:
        raise FileNotFoundError("No path provided.")

    home = Path.home()
    me = home.name

    # Fix missing backslash after drive letter like 'C:users\...'
    raw = re.sub(r"^([A-Za-z]):(?!\\)", r"\\1:\\", raw)

    # Fix malformed 'C:\\Users\\Desktop\\...' (missing username)
    if re.match(r"^[A-Za-z]:\\Users\\Desktop(\\|$)", raw, flags=re.IGNORECASE):
        tail = raw.split("\\Users\\Desktop\\", 1)[1] if "\\Users\\Desktop\\" in raw else ""
        raw = str(home / "Desktop" / tail)

    # Collapse duplicated '\Users\<me>\Users\<me>\'
    dup_pat = re.compile(rf"(\\Users\\{re.escape(me)})(?:\\Users\\{re.escape(me)})+(\\|$)", flags=re.IGNORECASE)
    raw = dup_pat.sub(rf"\1\2", raw)

    p_in = Path(raw)

    # Ensure an Excel extension if none was given
    if p_in.suffix == "":
        p_in = p_in.with_suffix(".xlsx")

    name = p_in.name

    candidates: List[Path] = []

    # 1) As given (absolute or relative to CWD)
    candidates.append(p_in)

    # 2) Expand ~
    candidates.append(Path(raw).expanduser())

    # 3) If relative, try relative to HOME
    if not p_in.is_absolute():
        candidates.append(home / p_in)

    # 4) Handle inputs starting with 'Desktop\...'
    parts = Path(raw).parts
    if parts and parts[0].lower() == "desktop":
        after_desktop = Path(*parts[1:]) if len(parts) > 1 else Path(name)
        if after_desktop.suffix == "":
            after_desktop = after_desktop.with_suffix(".xlsx")
        candidates.append(home / "Desktop" / after_desktop)

    # 5) Try common OneDrive Desktop paths
    for od in home.glob("OneDrive*/Desktop"):
        # If input was absolute, prefer its basename under OneDrive Desktop
        candidates.append(od / name)
        if parts and parts[0].lower() == "desktop":
            candidates.append(od / after_desktop)

    # Deduplicate while preserving order
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

    # Return the first that exists
    for c in uniq:
        if c.expanduser().exists():
            return c.expanduser()

    tried = "\n  - " + "\n  - ".join(str(c.expanduser()) for c in uniq)
    raise FileNotFoundError(f"Excel file not found. Paths tried:{tried}")

# ============================
# Main
# ============================
xlsx_path_str = None

# ============================
# File chooser (click-to-choose)
# ============================
import os
from typing import Optional

EXCEL_EXTS = {".xlsx", ".xlsm", ".xlsb", ".xls"}

def _first_existing(paths):
    for p in paths:
        try:
            if p and Path(p).exists():
                return str(Path(p))
        except Exception:
            pass
    return None

def _likely_initial_dirs() -> list[str]:
    home = Path.home()
    candidates = [
        home / "Desktop",
        # OneDrive Desktops (personal / org)
        *[p for p in home.glob("OneDrive*/Desktop")],
        home / "Downloads",
        home / "Documents",
        home,
    ]
    return [str(p) for p in candidates if p.exists()]

def choose_excel_file() -> Optional[str]:
    """
    Open a native file dialog to choose an Excel file.
    Returns the absolute path string or None if the user cancels.
    """
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        # Bring dialog to front on Windows
        try:
            root.call('wm', 'attributes', '.', '-topmost', True)
        except Exception:
            pass

        # Pick a good initial directory
        initialdir = _first_existing(_likely_initial_dirs())

        path = filedialog.askopenfilename(
            title="Select Excel file",
            initialdir=initialdir or None,
            filetypes=[
                ("Excel files", "*.xlsx *.xlsm *.xlsb *.xls"),
                ("All files", "*.*"),
            ],
        )
        root.destroy()
        if not path:
            return None

        sel = Path(path)
        if sel.suffix.lower() not in EXCEL_EXTS:
            # still allow if user picked a valid Excel (some environments hide extensions)
            if sel.exists():
                return str(sel.resolve())
            return None
        return str(sel.resolve())

    except Exception:
        # Headless or Tk not available -> no picker
        return None


# ============================
# Main (picker-first workflow)
# ============================

if __name__ == "__main__":
    import sys

    try:
        print("=== Artera Upload Builder ===")
        # Always save to Desktop/Artera SFTP Uploads
        desktop = Path.home() / "Desktop" / "Artera SFTP Uploads"
        desktop.mkdir(parents=True, exist_ok=True)
        outdir = str(desktop)
        print(f"📁 Output will be saved to: {outdir}")

        # 1) Try the click-to-choose dialog first
        xlsx_path_str = choose_excel_file()

        # 2) If the user cancels or picker isn’t available, fall back to manual entry
        if not xlsx_path_str:
            print("📎 File picker was unavailable or canceled.")
            user_in = input("📂 Paste the full path to the Excel file (or press Enter to try again): ").strip()
            if not user_in:
                print("❌ No file selected.")
                sys.exit(1)
            # Reuse your robust resolver for typed paths
            xlsx_path = _resolve_xlsx_path(user_in)
        else:
            xlsx_path = Path(xlsx_path_str)
            if not xlsx_path.exists():
                # Extremely rare, but handle just in case (network/redirect)
                xlsx_path = _resolve_xlsx_path(xlsx_path_str)

        sheet = input("🗂️  Optional sheet name (press Enter to auto-detect): ").strip()

        # Always save to Desktop/Artera SFTP Uploads
        desktop = Path.home() / "Desktop" / "Artera SFTP Uploads"
        desktop.mkdir(parents=True, exist_ok=True)
        outdir = str(desktop)
        print(f"📁 Output will be saved to: {outdir}")

        prefix = input("🏷️  File prefix (default='SBNC_Outreach_') : ").strip() or "SBNC_Outreach_"

        language_recode = {"Spanish; Castilian": "Spanish"}

        result = build_artera_upload_from_excel(
            xlsx_path=xlsx_path,
            sheet_name=sheet if sheet else None,
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

    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
