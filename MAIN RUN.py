from  Azara_Derived_Filtering import Azara_Filtering_Logic

from pathlib import Path
from SFTP_FileZilla_Scrubber import  build_artera_upload_from_excel,pick_excel_path,_resolve_xlsx_path, build_artera_upload_from_df

# ---- Main
# This will run the file pickup and template filtering
if __name__ == "__main__": # this will run the file pickup and dump for the SFTP format
    import sys

    try:
        print("=== Artera Upload Builder ===")
        user_in = input("📂 Enter the path to the Excel file (press Enter to browse): ").strip()

        if not user_in:
            # Open a file picker; if user cancels, this remains ""
            user_in = pick_excel_path()

        if not user_in:
            raise FileNotFoundError("No path selected or provided.")

        xlsx_path = _resolve_xlsx_path(user_in)

        sheet = input("🗂️  Optional sheet name (press Enter to auto-detect): ").strip()

        default_outdir = Path.home() / "Desktop"  # <-- DEFAULT: Desktop
        outdir = input(f"📁 Output directory for CSV (default='{default_outdir}') : ").strip() or default_outdir

        prefix = input("🏷️  File prefix (default='SBNC_Outreach') : ").strip() or "SBNC_Outreach"

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
        sys.exit(1) # This wi;;









# ---- Entrypoint ----
if __name__ == "__main__":
    Azara_Filtering_Logic()
