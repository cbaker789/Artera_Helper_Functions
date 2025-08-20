#!/usr/bin/env python3
from __future__ import annotations

import threading
import traceback
from pathlib import Path
from typing import Optional, Dict

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# --- Your project imports ---
try:
    from SFTP_FileZilla_Scrubber import (
        build_artera_upload_from_excel,
        pick_excel_path,
        _resolve_xlsx_path,
        build_artera_upload_from_df,  # kept import for parity
    )
except Exception as e:
    raise RuntimeError(
        "Failed to import from SFTP_FileZilla_Scrubber. "
        "Ensure this file is importable from your PYTHONPATH."
    ) from e

try:
    from Azara_Derived_Filtering import Azara_Filtering_Logic  # optional
except Exception:
    Azara_Filtering_Logic = None  # type: ignore


# ------------------------------
# Utility: run function on a thread and pipe logs to UI
# ------------------------------
class Worker:
    def __init__(self, ui_log_fn, on_done_fn=None):
        self._ui_log = ui_log_fn
        self._on_done = on_done_fn

    def run(self, target, *args, **kwargs):
        def _wrap():
            try:
                target(*args, **kwargs)
            except Exception as ex:
                tb = traceback.format_exc()
                self._ui_log(f"\n❌ Error: {ex}\n{tb}")
                messagebox.showerror("Error", f"{ex}")
            finally:
                if self._on_done:
                    self._on_done()

        t = threading.Thread(target=_wrap, daemon=True)
        t.start()


# ------------------------------
# Main App
# ------------------------------
class SBNCApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SBNC Utilities")
        self.geometry("840x620")
        self.minsize(780, 560)

        try:
            self.iconbitmap(default="")  # no-op if you don't have an .ico
        except Exception:
            pass

        self._build_ui()

    # ---- UI Builders
    def _build_ui(self):
        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        # Tab 1: Artera Upload Builder
        self.tab_artera = ttk.Frame(notebook)
        notebook.add(self.tab_artera, text="Artera Upload Builder")

        # Tab 2: Azara Filtering
        self.tab_azara = ttk.Frame(notebook)
        notebook.add(self.tab_azara, text="Azara Filtering Logic")

        # Footer status
        self.status = tk.StringVar(value="Ready")
        status_bar = ttk.Label(self, textvariable=self.status, anchor="w")
        status_bar.pack(fill="x", padx=10, pady=(0, 10))

        self._build_tab_artera(self.tab_artera)
        self._build_tab_azara(self.tab_azara)

    def _build_tab_artera(self, root: ttk.Frame):
        # Inputs frame
        frm = ttk.LabelFrame(root, text="Inputs")
        frm.pack(fill="x", padx=10, pady=10)

        # Excel path
        self.var_xlsx = tk.StringVar()
        ttk.Label(frm, text="Excel File:").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        ent_xlsx = ttk.Entry(frm, textvariable=self.var_xlsx, width=70)
        ent_xlsx.grid(row=0, column=1, sticky="we", padx=8, pady=6)
        ttk.Button(frm, text="Browse…", command=self._browse_excel).grid(row=0, column=2, padx=8, pady=6)

        # Sheet name (optional)
        self.var_sheet = tk.StringVar()
        ttk.Label(frm, text="Sheet (optional):").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        ttk.Entry(frm, textvariable=self.var_sheet, width=30).grid(row=1, column=1, sticky="w", padx=8, pady=6)

        # Output directory
        self.var_outdir = tk.StringVar(value=str(Path.home() / "Desktop"))
        ttk.Label(frm, text="Output Directory:").grid(row=2, column=0, sticky="w", padx=8, pady=6)
        ent_out = ttk.Entry(frm, textvariable=self.var_outdir, width=70)
        ent_out.grid(row=2, column=1, sticky="we", padx=8, pady=6)
        ttk.Button(frm, text="Browse…", command=self._browse_outdir).grid(row=2, column=2, padx=8, pady=6)

        # File prefix
        self.var_prefix = tk.StringVar(value="SBNC_Outreach")
        ttk.Label(frm, text="File Prefix:").grid(row=3, column=0, sticky="w", padx=8, pady=6)
        ttk.Entry(frm, textvariable=self.var_prefix, width=30).grid(row=3, column=1, sticky="w", padx=8, pady=6)

        # Language recode (fixed example)
        self.var_recode_spanish = tk.BooleanVar(value=True)
        ttk.Checkbutton(frm, text="Recode 'Spanish; Castilian' → 'Spanish'",
                        variable=self.var_recode_spanish).grid(row=4, column=1, sticky="w", padx=8, pady=(6, 10))

        frm.columnconfigure(1, weight=1)

        # Action buttons
        btns = ttk.Frame(root)
        btns.pack(fill="x", padx=10, pady=(0, 10))

        self.btn_run_artera = ttk.Button(btns, text="Build CSV", command=self._on_run_artera)
        self.btn_run_artera.pack(side="left")

        ttk.Button(btns, text="Use Picker to Choose Excel", command=self._use_internal_picker).pack(side="left", padx=10)

        # Log area
        self.txt_log_artera = tk.Text(root, height=16, wrap="word")
        self.txt_log_artera.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self._log_artera("Ready.")

    def _build_tab_azara(self, root: ttk.Frame):
        frm = ttk.LabelFrame(root, text="Azara Filtering Logic")
        frm.pack(fill="x", padx=10, pady=10)

        desc = (
            "Runs Azara_Derived_Filtering.Azara_Filtering_Logic() if available.\n"
            "This will execute your existing filtering workflow."
        )
        ttk.Label(frm, text=desc, justify="left").pack(anchor="w", padx=10, pady=10)

        self.btn_run_azara = ttk.Button(frm, text="Run Azara Filtering", command=self._on_run_azara)
        self.btn_run_azara.pack(anchor="w", padx=10, pady=(0, 10))

        # Log area
        self.txt_log_azara = tk.Text(root, height=20, wrap="word")
        self.txt_log_azara.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self._log_azara("Ready.")

    # ---- Logging helpers
    def _log_artera(self, msg: str):
        self.txt_log_artera.insert("end", msg + "\n")
        self.txt_log_artera.see("end")

    def _log_azara(self, msg: str):
        self.txt_log_azara.insert("end", msg + "\n")
        self.txt_log_azara.see("end")

    # ---- Browsers
    def _browse_excel(self):
        path = filedialog.askopenfilename(
            title="Select Outreach Excel File",
            filetypes=[("Excel files", "*.xlsx *.xls")],
        )
        if path:
            self.var_xlsx.set(path)

    def _browse_outdir(self):
        path = filedialog.askdirectory(title="Select Output Folder")
        if path:
            self.var_outdir.set(path)

    def _use_internal_picker(self):
        """Call your existing pick_excel_path() to get a path (shows native dialog)."""
        try:
            path = pick_excel_path()
            if path:
                self.var_xlsx.set(path)
                self._log_artera(f"Picked Excel via internal picker: {path}")
            else:
                self._log_artera("No file selected via internal picker.")
        except Exception as e:
            self._log_artera(f"❌ Error using internal picker: {e}")
            messagebox.showerror("Picker Error", str(e))

    # ---- Actions
    def _on_run_artera(self):
        # Disable button while running
        self._toggle_running(self.btn_run_artera, running=True)
        self.status.set("Running Artera Upload Builder…")
        self._log_artera("=== Artera Upload Builder ===")

        xlsx_in = self.var_xlsx.get().strip()
        sheet = self.var_sheet.get().strip()
        outdir = self.var_outdir.get().strip()
        prefix = self.var_prefix.get().strip() or "SBNC_Outreach"

        language_recode: Optional[Dict[str, str]] = {"Spanish; Castilian": "Spanish"} if self.var_recode_spanish.get() else None

        def task():
            try:
                if not xlsx_in:
                    raise FileNotFoundError("No Excel path provided. Choose a file or use the internal picker.")

                # Resolve Excel path using your helper (supports your smart logic)
                xlsx_path = _resolve_xlsx_path(xlsx_in)

                outdir_path = Path(outdir) if outdir else (Path.home() / "Desktop")
                outdir_path.mkdir(parents=True, exist_ok=True)

                self._log_artera(f"Input Excel: {xlsx_path}")
                self._log_artera(f"Sheet: {sheet or '(auto-detect)'}")
                self._log_artera(f"Output directory: {outdir_path}")
                self._log_artera(f"Prefix: {prefix}")
                if language_recode:
                    self._log_artera(f"Language recode map: {language_recode}")

                result = build_artera_upload_from_excel(
                    xlsx_path=xlsx_path,
                    sheet_name=sheet if sheet else None,
                    csv_outdir=outdir_path,
                    file_prefix=prefix,
                    language_recode=language_recode,
                )

                # Success info
                self._log_artera("\n✅ Upload CSV created successfully!")
                self._log_artera(f"   Saved to: {result.get('csv_path')}")
                self._log_artera(f"   Sheet used: {result.get('sheet_name')}")
                self._log_artera("   Inferred column map:")
                for k, v in (result.get("column_map") or {}).items():
                    self._log_artera(f"     {k:15} -> {v}")

                messagebox.showinfo("Success", f"CSV created:\n{result.get('csv_path')}")
            finally:
                self._toggle_running(self.btn_run_artera, running=False)
                self.status.set("Ready")

        Worker(self._log_artera).run(task)

    def _on_run_azara(self):
        if Azara_Filtering_Logic is None:
            messagebox.showwarning(
                "Unavailable",
                "Azara_Filtering_Logic() was not found.\n\n"
                "Ensure Azara_Derived_Filtering.py is importable and try again."
            )
            return

        self._toggle_running(self.btn_run_azara, running=True)
        self.status.set("Running Azara Filtering Logic…")
        self._log_azara("=== Azara Filtering Logic ===")

        def task():
            try:
                Azara_Filtering_Logic()
                self._log_azara("✅ Azara Filtering Logic completed.")
                messagebox.showinfo("Done", "Azara Filtering Logic completed.")
            finally:
                self._toggle_running(self.btn_run_azara, running=False)
                self.status.set("Ready")

        Worker(self._log_azara).run(task)

    # ---- Utils
    def _toggle_running(self, button: ttk.Button, running: bool):
        try:
            button.configure(state="disabled" if running else "normal")
        except Exception:
            pass


def main():
    app = SBNCApp()
    app.mainloop()


if __name__ == "__main__":
    main()