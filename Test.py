#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path
import sys
from typing import Optional

import pandas as pd


APPT_TYPE_REGEX = (
    r"ECM|Medication Purchase|Walk|Bloodwork|Care Management|Lab Only|"
    r"Vaccination|Speciman|Injection|Chrio|Acu|Podiatry"
)
APPT_LOC_REGEX = r"Dental|Bridge"


def to_datetime_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Safely coerce a column to datetime (keeps NaT if parse fails or col missing)."""
    if col not in df.columns:
        return pd.Series(pd.NaT, index=df.index)
    return pd.to_datetime(df[col], errors="coerce")


def build_outreach_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    Python translation of the given R dplyr pipeline.

    Steps:
      - x90_Days_Ago = today - 90 days
      - Uppercase Name, split into 'Last Name', 'First Name' on ', '
      - Rename 'Date of Birth' -> 'DOB' (if present)
      - Filter Deceased == 'N'
      - Keep rows where:
            Next Appointment Date is NA
         OR Next Appointment Location matches /Dental|Bridge/i
         OR Next Appointment Type matches APPT_TYPE_REGEX (case-insensitive)
      - Recode Language: 'Spanish; Castilian' -> 'Spanish'
      - Cast MRN to string
      - Filter Most Recent Encounter Date <= x90_Days_Ago
      - Distinct rows (drop duplicates)
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
        # Split into two columns if possible
        split = df["Name"].str.split(", ", n=1, expand=True)
        # Expand may return 1 column if no comma; handle safely
        if split.shape[1] == 2:
            df["Last Name"] = split[0]
            df["First Name"] = split[1]
        else:
            # If split failed, still create columns to mirror R behavior
            df["Last Name"] = split[0]
            df["First Name"] = pd.NA

    # Rename Date of Birth -> DOB if present
    if "Date of Birth" in df.columns and "DOB" not in df.columns:
        df = df.rename(columns={"Date of Birth": "DOB"})

    # Filter out deceased
    if "Deceased" in df.columns:
        df = df[df["Deceased"] == "N"]

    # Build appointment-based predicate
    # 1) Next Appointment Date is NA
    cond_date_na = df["Next Appointment Date"].isna() if "Next Appointment Date" in df.columns else pd.Series(True, index=df.index)

    # 2) Next Appointment Location matches Dental|Bridge (case-insensitive)
    if "Next Appointment Location" in df.columns:
        cond_loc = df["Next Appointment Location"].astype(str).str.contains(
            APPT_LOC_REGEX, flags=re.IGNORECASE, regex=True, na=False
        )
    else:
        cond_loc = pd.Series(False, index=df.index)

    # 3) Next Appointment Type matches APPT_TYPE_REGEX (case-insensitive)
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

    # MRN to string (character)
    if "MRN" in df.columns:
        df["MRN"] = df["MRN"].astype(str)

    # Filter Most Recent Encounter Date <= x90_days_ago
    if "Most Recent Encounter Date" in df.columns:
        df = df[df["Most Recent Encounter Date"] <= x90_days_ago]

    # Distinct rows
    df = df.drop_duplicates()

    return df


def read_input(path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
    if not path.exists():
        sys.exit(f"❌ Input file not found: {path}")
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet)
    elif path.suffix.lower() in {".csv"}:
        return pd.read_csv(path)
    else:
        sys.exit("❌ Unsupported file type. Please provide .xlsx, .xls, or .csv")


def Azara_Filtering_Logic():
    parser = argparse.ArgumentParser(
        description="Build Outreach List (Python translation of R dplyr pipeline)."
    )
    parser.add_argument("input", type=Path, help="Path to input Excel/CSV file")
    parser.add_argument(
        "--sheet", type=str, default=None, help="Sheet name (for Excel inputs only)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path to write result (.xlsx or .csv). If omitted, prints preview.",
    )
    parser.add_argument(
        "--preview",
        type=int,
        default=20,
        help="Number of rows to print when no --output is supplied (default: 20).",
    )
    args = parser.parse_args()

    df_in = read_input(args.input, sheet=args.sheet)
    df_out = build_outreach_list(df_in)

    if args.output:
        if args.output.suffix.lower() in {".xlsx", ".xls"}:
            df_out.to_excel(args.output, index=False)
        elif args.output.suffix.lower() == ".csv":
            df_out.to_csv(args.output, index=False)
        else:
            sys.exit("❌ --output must be .xlsx, .xls, or .csv")
        print(f"✅ Wrote {len(df_out):,} rows to {args.output}")
    else:
        pd.set_option("display.max_columns", 0)
        print(df_out.head(args.preview))
        print(f"\n(Showing first {min(args.preview, len(df_out))} of {len(df_out):,} rows)")


if __name__ == "__main__":
    Azara_Filtering_Logic()
