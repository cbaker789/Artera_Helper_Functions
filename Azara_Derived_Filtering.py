#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path
import sys
from typing import Optional, List, Dict

import pandas as pd

# ============================
# Config / Regex
# ============================

APPT_TYPE_REGEX = (
    r"ECM|Medication Purchase|Walk|Bloodwork|Care Management|Lab Only|"
    r"Vaccination|Speciman|Injection|Chrio|Acu|Podiatry"
)
APPT_LOC_REGEX = r"Dental|Bridge"

# ============================
# Helpers (shared with other script-style)
# ============================

def _resolve_data_path(user_input: str, *, allow_csv: bool = True) -> Path:
    """
    Resolve a user-entered Excel/CSV path robustly:
      - trims/strips quotes and whitespace
      - adds .xlsx if missing AND there's no extension (falls back to .csv if allow_csv=False? no)
      - expands ~
      - if relative like 'Desktop\\file', resolves against HOME and HOME\\Desktop
      - fixes common typo 'C:\\Users\\Desktop\\...'
      - tries common OneDrive Desktop locations
    Returns the first existing Path; raises FileNotFoundError with all candidates if none exist.
    """
    raw = (user_input or "").strip().strip('"').strip("'")
    if not raw:
        raise FileNotFoundError("No path provided.")

    # Fix common typo: "C:\Users\Desktop\..."
    if re.match(r"^[A-Za-z]:\\Users\\Desktop(\\|$)", raw):
        tail = raw.split("\\Users\\Desktop\\", 1)[-1]
        raw = str(Path.home() / "Desktop" / tail)

    p_in = Path(raw)

    # If no extension, prefer .xlsx; also consider .csv if allowed
    candidates: List[Path] = []
    if p_in.suffix == "":
        candidates.append(p_in.with_suffix(".xlsx"))
        if allow_csv:
            candidates.append(p_in.with_suffix(".csv"))
    else:
        candidates.append(p_in)

    home = Path.home()
    parts = Path(raw).parts
    name = p_in.name

    # Expand ~
    expanded = Path(raw).expanduser()
    if expanded.suffix == "":
        candidates.append(expanded.with_suffix(".xlsx"))
        if allow_csv:
            candidates.append(expanded.with_suffix(".csv"))
    else:
        candidates.append(expanded)

    # If relative, try HOME/<path>
    if not p_in.is_absolute():
        for base in [home]:
            if p_in.suffix == "":
                candidates.append((base / p_in).with_suffix(".xlsx"))
                if allow_csv:
                    candidates.append((base / p_in).with_suffix(".csv"))
            else:
                candidates.append(base / p_in)

    # If starts with 'Desktop', try HOME/Desktop/<...>
    if parts and parts[0].lower() == "desktop":
        after_desktop = Path(*parts[1:]) if len(parts) > 1 else Path(name)
        if after_desktop.suffix == "":
            candidates.append((home / "Desktop" / after_desktop).with_suffix(".xlsx"))
            if allow_csv:
                candidates.append((home / "Desktop" / after_desktop).with_suffix(".csv"))
        else:
            candidates.append(home / "Desktop" / after_desktop)

    # Try OneDrive Desktop
    for od in home.glob("OneDrive*/Desktop"):
        if p_in.suffix == "":
            candidates.append(od / (p_in.name + ".xlsx"))
            if allow_csv:
                candidates.append(od / (p_in.name + ".csv"))
        else:
            candidates.append(od / name)
        if parts and parts[0].lower() == "desktop":
            if after_desktop.suffix == "":
                candidates.append((od / after_desktop).with_suffix(".xlsx"))
                if allow_csv:
                    candidates.append((od / after_desktop).with_suffix(".csv"))
            else:
                candidates.append(od / after_desktop)

    # Deduplicate preserving order
    seen: set[str] = set()
    uniq: List[Path] = []
    for c in candidates:
        key = str(c.expanduser()).lower()
        if key not in seen:
            seen.add(key)
            uniq.append(c)

    for c in uniq:
        if c.expanduser().exists():
            return c.expanduser()

    tried = "\n  - " + "\n  - ".join(str(c.expanduser()) for c in uniq)
    raise FileNotFoundError(f"Data file not found. Paths tried:{tried}")

def pick_data_path() -> str:
    """Open a file dialog to pick an Excel/CSV file. Returns '' on cancel."""
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        path = filedialog.askopenfilename(
            title="Select Excel or CSV file",
            filetypes=[
                ("Excel files", "*.xlsx *.xlsm *.xlsb *.xls"),
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
        )
        return path or ""
    except Exception:
        return ""

def to_datetime_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Safely coerce a column to datetime (keeps NaT if parse fails or col missing)."""
    if col not in df.columns:
        return pd.Series(pd.NaT, index=df.index)
    return pd.to_datetime(df[col], errors="coerce")

def read_input(path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
    if not path.exists():
        sys.exit(f"❌ Input file not found: {path}")
    if path.suffix.lower() in {".xlsx", ".xls", ".xlsm", ".xlsb"}:
        if sheet:
            return pd.read_excel(path, sheet_name=sheet)
        else:
            # Default: use the first sheet
            df_dict = pd.read_excel(path, sheet_name=None)
            # Pick the first sheet
            first_sheet = next(iter(df_dict))
            return df_dict[first_sheet]
    elif path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    else:
        sys.exit("❌ Unsupported file type. Please provide .xlsx, .xls, .xlsm, .xlsb, or .csv")

# ============================
# Core filtering logic
# ============================

def build_outreach_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    Python translation of the given R dplyr pipeline.
    """
    df = df.copy()

    # Date boundary
    x90_days_ago = pd.Timestamp.today().normalize() - pd.Timedelta(days=90)

    # Ensure key date columns are datetime
    df["Most Recent Encounter Date"] = to_datetime_col(df, "Most Recent Encounter Date")
    df["Next Appointment Date"] = to_datetime_col(df, "Next Appointment Date")

    # Uppercase Name (if exists), split into last/first by ", "
    if "Name" in df.columns:
        df["Name"] = df["Name"].astype(str).str.upper()
        split = df["Name"].str.split(", ", n=1, expand=True)
        if split.shape[1] == 2:
            df["Last Name"] = split[0]
            df["First Name"] = split[1]
        else:
            df["Last Name"] = split[0]
            df["First Name"] = pd.NA

    # Rename Date of Birth -> DOB if present
    if "Date of Birth" in df.columns and "DOB" not in df.columns:
        df = df.rename(columns={"Date of Birth": "DOB"})

    # Filter out deceased
    if "Deceased" in df.columns:
        df = df[df["Deceased"] == "N"]

    # Appointment-based predicates
    cond_date_na = (
        df["Next Appointment Date"].isna()
        if "Next Appointment Date" in df.columns else pd.Series(True, index=df.index)
    )

    if "Next Appointment Location" in df.columns:
        cond_loc = df["Next Appointment Location"].astype(str).str.contains(
            APPT_LOC_REGEX, flags=re.IGNORECASE, regex=True, na=False
        )
    else:
        cond_loc = pd.Series(False, index=df.index)

    if "Next Appointment Type" in df.columns:
        cond_type = df["Next Appointment Type"].astype(str).str.contains(
            APPT_TYPE_REGEX, flags=re.IGNORECASE, regex=True, na=False
        )
    else:
        cond_type = pd.Series(False, index=df.index)

    df = df[cond_date_na | cond_loc | cond_type]

    # Recode Language: 'Spanish; Castilian' -> 'Spanish'
    if "Language" in df.columns:
        df["Language"] = df["Language"].replace({"Spanish; Castilian": "Spanish"})

    # MRN to string
    if "MRN" in df.columns:
        df["MRN"] = df["MRN"].astype(str)

    # Most Recent Encounter Date <= x90_days_ago
    if "Most Recent Encounter Date" in df.columns:
        df = df[df["Most Recent Encounter Date"] <= x90_days_ago]

    # Distinct rows
    df = df.drop_duplicates()

    return df

# ============================
# Main with dual mode (CLI or interactive)
# ============================

def Azara_Filtering_Logic():
    parser = argparse.ArgumentParser(
        description="Build Outreach List (Python translation of R dplyr pipeline). "
                    "If arguments are omitted, an interactive prompt with file picker will be used."
    )
    parser.add_argument("--input", type=Path, help="Path to input Excel/CSV file")
    parser.add_argument("--sheet", type=str, default=None, help="Sheet name (Excel only)")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path to write result (.xlsx or .csv). If omitted, interactive mode can choose.",
    )
    parser.add_argument(
        "--preview",
        type=int,
        default=20,
        help="Rows to print when no output is supplied (default: 20).",
    )
    args, unknown = parser.parse_known_args()

    interactive = not (args.input or args.output or args.sheet or unknown)

    try:
        if interactive:
            print("=== Azara Outreach Filter ===")
            user_in = input("📂 Enter the path to the Excel/CSV file (press Enter to browse): ").strip()
            if not user_in:
                user_in = pick_data_path()
            if not user_in:
                raise FileNotFoundError("No path selected or provided.")

            data_path = _resolve_data_path(user_in, allow_csv=True)

            sheet = None
            if data_path.suffix.lower() in {".xlsx", ".xls", ".xlsm", ".xlsb"}:
                sheet_txt = input("🗂️  Optional sheet name (press Enter to use default/first): ").strip()
                sheet = sheet_txt if sheet_txt else None

            default_outdir = Path.home() / "Desktop"
            outdir_txt = input(f"📁 Output directory (default='{default_outdir}') : ").strip()
            outdir = Path(outdir_txt) if outdir_txt else default_outdir
            outdir.mkdir(parents=True, exist_ok=True)

            # Choose output format & filename
            default_name = "Azara_Outreach_Filter.xlsx"
            name_txt = input(f"🏷️  Output file name (default='{default_name}') : ").strip()
            outname = name_txt if name_txt else default_name
            outpath = outdir / outname

            df_in = read_input(data_path, sheet=sheet)
            df_out = build_outreach_list(df_in)

            if outpath.suffix.lower() in {".xlsx", ".xls"}:
                df_out.to_excel(outpath, index=False)
            elif outpath.suffix.lower() == ".csv":
                df_out.to_csv(outpath, index=False)
            else:
                # fallback: write excel
                outpath = outpath.with_suffix(".xlsx")
                df_out.to_excel(outpath, index=False)

            print(f"\n✅ Wrote {len(df_out):,} rows to {outpath}")
            if sheet:
                print(f"   Sheet used: {sheet}")

        else:
            # CLI mode (backwards-compatible)
            if not args.input:
                sys.exit("❌ Please provide --input or run without args for interactive mode.")

            # If user passed a raw, possibly ambiguous path via --input, resolve like the other script
            input_path = args.input
            try:
                input_path = _resolve_data_path(str(args.input), allow_csv=True)
            except Exception:
                # Fall back to arg path if resolve fails but file exists
                if not args.input.exists():
                    raise

            df_in = read_input(input_path, sheet=args.sheet)
            df_out = build_outreach_list(df_in)

            if args.output:
                out = args.output
                out.parent.mkdir(parents=True, exist_ok=True)
                if out.suffix.lower() in {".xlsx", ".xls"}:
                    df_out.to_excel(out, index=False)
                elif out.suffix.lower() == ".csv":
                    df_out.to_csv(out, index=False)
                else:
                    sys.exit("❌ --output must be .xlsx, .xls, or .csv")
                print(f"✅ Wrote {len(df_out):,} rows to {out}")
            else:
                pd.set_option("display.max_columns", 0)
                print(df_out.head(args.preview))
                print(f"\n(Showing first {min(args.preview, len(df_out))} of {len(df_out):,} rows)")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

# ---- Entrypoint ----
if __name__ == "__main__":
    Azara_Filtering_Logic()
