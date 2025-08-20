

import os
import paramiko
from SFTP_FileZilla_Scrubber import build_artera_upload_from_excel, pick_excel_path, _resolve_xlsx_path


def _print_progress(transferred: int, total: int) -> None:
    pct = int((transferred / total) * 100) if total else 0
    print(f"\rUploading... {transferred}/{total} bytes ({pct}%)", end="", flush=True)


def Filezilla_Upload() -> None:
    try:
        print("=== Artera SFTP Uploader ===")

        # Scrubber-style path input with OS picker fallback
        user_in = input("📂 Enter the path to the Excel file (press Enter to browse): ").strip()
        if not user_in:
            user_in = pick_excel_path()
            if not user_in:
                print("No file selected. Exiting.")
                return

        xlsx_path = _resolve_xlsx_path(user_in)

        # Match the Scrubber flow: optional sheet/outdir/prefix prompts
        sheet = input("🗂️  Optional sheet name (press Enter to auto-detect): ").strip()
        outdir = input("📁 Output directory for CSV (default='.') : ").strip() or "."
        prefix = input("🏷️  File prefix (default='SBNC_Outreach') : ").strip() or "SBNC_Outreach"

        language_recode = {"Spanish; Castilian": "Spanish"}

        # Build CSV with your existing helper
        result = build_artera_upload_from_excel(
            xlsx_path=xlsx_path,
            sheet_name=sheet if sheet else None,
            csv_outdir=outdir,
            file_prefix=prefix,
            language_recode=language_recode,
        )

        csv_filepath = result["csv_path"]
        print("\n✅ Upload CSV created successfully!")
        print(f"   Saved to: {csv_filepath}")
        print(f"   Sheet used: {result['sheet_name']}")
        print("   Inferred column map:")
        for k, v in result["column_map"].items():
            print(f"     {k:15} -> {v}")

        # Remote target
        remote_dir = "/uploads/prod"
        remote_path = f"{remote_dir}/" + os.path.basename(csv_filepath)

        # Confirm before uploading
        confirm = input(f"\nProceed with upload of '{csv_filepath}' to '{remote_path}'? (y/n): ")
        if confirm.lower() != 'y':
            print("Upload cancelled by user.")
            return

        # SFTP credentials
        host = "sftp.wellapp.com"
        username = "SantaBarbaraNC"
        password = "Green4grass!"

        transport = None
        try:
            transport = paramiko.Transport((host, 22))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            # Ensure remote directory exists (create if needed, including parents)
            try:
                sftp.chdir(remote_dir)
            except IOError:
                parts = remote_dir.strip("/").split("/")
                path = ""
                for part in parts:
                    path += "/" + part
                    try:
                        sftp.chdir(path)
                    except IOError:
                        sftp.mkdir(path)
                        sftp.chdir(path)

            # Upload with progress callback
            print(f"Uploading to {remote_path} ...")
            sftp.put(csv_filepath, remote_path, callback=_print_progress)
            print("\n✅ Upload complete.")
            print(f"Remote: {remote_path}")

        finally:
            if transport:
                transport.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

Filezilla_Upload()