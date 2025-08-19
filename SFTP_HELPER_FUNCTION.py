





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
