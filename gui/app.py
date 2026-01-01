import tkinter as tk
from tkinter import filedialog, messagebox, ttk, simpledialog
import zipfile
import os
from config.settings import config
from core.lakehouse import Lakehouse
from core.prescriptive import Prescriptive
from core.storage import storage


class Application:
    """GUI application for K-pop concert budget recommendations"""

    def __init__(self, root):
        self.root = root
        self.root.title("Sistem Rekomendasi Konser K-Pop")
        self.root.geometry("900x700")

        self.lakehouse = Lakehouse()
        self.df_gold = None
        self.df_display = None
        self.budget = None
        self.files_uploaded = []
        self.current_timestamp = None
        self.sort_reverse = {}

        self._build_ui()

    def _build_ui(self):
        """Build user interface components"""
        tk.Label(
            self.root,
            text="SISTEM REKOMENDASI KONSER K-POP",
            font=("Arial", 16, "bold"),
            bg="#1a237e",
            fg="white",
        ).pack(fill="x", pady=10)

        btn_frame = tk.Frame(self.root)
        btn_frame.pack(pady=15)

        tk.Button(
            btn_frame,
            text="Load CSV (Bronze)",
            command=self._load_csv,
            font=("Arial", 11),
            bg="#4CAF50",
            fg="white",
            width=20,
        ).pack(side="left", padx=5)

        self.btn_download = tk.Button(
            btn_frame,
            text="Download Files (ZIP)",
            command=self._download_zip,
            state="disabled",
            font=("Arial", 11),
            bg="#2196F3",
            fg="white",
            width=20,
        )
        self.btn_download.pack(side="left", padx=5)

        self.btn_analyze = tk.Button(
            btn_frame,
            text="Analisis Prescriptive",
            command=self._run_prescriptive,
            state="disabled",
            font=("Arial", 11),
            bg="#FF9800",
            fg="white",
            width=20,
        )
        self.btn_analyze.pack(side="left", padx=5)

        self.btn_reset = tk.Button(
            btn_frame,
            text="Reset",
            command=self._reset,
            font=("Arial", 11),
            bg="#F44336",
            fg="white",
            width=15,
        )
        self.btn_reset.pack(side="left", padx=5)

        result_frame = tk.Frame(self.root)
        result_frame.pack(fill="both", expand=True, padx=20, pady=10)

        scroll = tk.Scrollbar(result_frame)
        scroll.pack(side="right", fill="y")

        self.tree = ttk.Treeview(result_frame, yscrollcommand=scroll.set, height=15)
        scroll.config(command=self.tree.yview)

        self.tree["columns"] = (
            "Nama Konser",
            "Lokasi",
            "Total",
            "Affordability",
            "Score",
        )
        self.tree.column("#0", width=0, stretch=tk.NO)

        col_map = {
            "Nama Konser": "nama_konser",
            "Lokasi": "lokasi",
            "Total": "total_pengeluaran",
            "Affordability": "affordability",
            "Score": "prescriptive_score",
        }

        for col in self.tree["columns"]:
            self.tree.heading(
                col, text=col, command=lambda c=col: self._sort_column(col_map[c], c)
            )
            self.tree.column(col, anchor="center", width=150)
            self.sort_reverse[col] = False

        self.tree.pack(fill="both", expand=True)

        self.status = tk.Label(
            self.root, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W
        )
        self.status.pack(side=tk.BOTTOM, fill=tk.X)

    def _load_csv(self):
        """Load CSV and process through lakehouse layers"""
        path = filedialog.askopenfilename(
            title="Pilih file CSV mentah (Bronze Layer)",
            filetypes=[("CSV files", "*.csv")],
        )

        if not path:
            return

        try:
            self.status.config(text="Processing Bronze → Silver → Gold...")
            self.root.update()

            df_bronze = self.lakehouse.ingest_bronze(path)
            df_silver = self.lakehouse.transform_silver(df_bronze)

            budget = simpledialog.askinteger(
                "Budget Anda",
                "Masukkan budget dalam Rupiah (contoh: 5000000):",
                parent=self.root,
                minvalue=1,
            )

            if budget is None:
                self.status.config(text="Budget tidak dimasukkan")
                return

            self.budget = budget
            self.df_gold, _ = self.lakehouse.aggregate_gold(df_silver, budget)

            self.current_timestamp = self.lakehouse._ts()
            self.df_display = self.df_gold.copy()
            self._track_files()
            self._display_data()

            self.btn_analyze.config(state="normal")
            self.btn_download.config(state="normal")
            self.status.config(text=f"Lakehouse ready | Budget: Rp{budget:,}")

        except Exception as e:
            messagebox.showerror("Error", f"Gagal memproses data:\n{str(e)}")
            self.status.config(text="Error")

    def _track_files(self):
        """
        Track the most recent files from each lakehouse layer for ZIP download.

        Collects the latest CSV file from bronze, silver, and gold layers.
        Files are sorted by modification time (newest first) to ensure we get
        the files from the current processing session.

        Behavior depends on storage mode:
        - Local mode: Scans data/ folders and gets latest file by mtime
        - MinIO mode: Lists objects in bucket folders and gets latest by name
        """
        self.files_uploaded = []

        if config["use_local"]:
            for layer in ["bronze", "silver", "gold"]:
                layer_path = getattr(self.lakehouse, f"local_{layer}")
                if os.path.exists(layer_path):
                    files = [
                        os.path.join(layer_path, f)
                        for f in os.listdir(layer_path)
                        if f.endswith(".csv")
                    ]
                    files.sort(key=os.path.getmtime, reverse=True)
                    if files:
                        self.files_uploaded.append(files[0])
        else:
            for folder in [
                config["bronze_folder"],
                config["silver_folder"],
                config["gold_folder"],
            ]:
                files = storage.list_files(folder)
                if files:
                    files.sort(reverse=True)
                    self.files_uploaded.append(files[0])

    def _download_zip(self):
        """
        Create and download a ZIP archive containing all lakehouse files.

        Collects bronze, silver, and gold layer files into a single ZIP archive.
        The ZIP preserves the folder structure (bronze/, silver/, gold/).
        ZIP filename uses the same timestamp as the processed files.

        Process:
        1. Prompt user for save location with timestamped filename
        2. Create ZIP with deflate compression
        3. For local mode: Add files directly from filesystem
        4. For MinIO mode: Download from MinIO then add to ZIP
        5. Preserve folder hierarchy in ZIP structure
        """
        if not self.files_uploaded:
            messagebox.showwarning("Warning", "Tidak ada file untuk didownload")
            return

        save_path = filedialog.asksaveasfilename(
            title="Simpan ZIP file",
            defaultextension=".zip",
            filetypes=[("ZIP files", "*.zip")],
            initialfile=f"kpop_lakehouse_{self.current_timestamp}.zip",
        )

        if not save_path:
            return

        try:
            self.status.config(text="Creating ZIP file...")
            self.root.update()

            with zipfile.ZipFile(save_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                if config["use_local"]:
                    for filepath in self.files_uploaded:
                        arcname = os.path.join(
                            os.path.basename(os.path.dirname(filepath)),
                            os.path.basename(filepath),
                        )
                        zipf.write(filepath, arcname)
                else:
                    for obj_name in self.files_uploaded:
                        data = storage.download(obj_name)
                        zipf.writestr(obj_name, data)

            self.status.config(text=f"ZIP saved: {save_path}")
            messagebox.showinfo(
                "Success", f"Files downloaded successfully!\n\nSaved to:\n{save_path}"
            )

        except Exception as e:
            messagebox.showerror("Error", f"Gagal membuat ZIP:\n{str(e)}")
            self.status.config(text="Error creating ZIP")

    def _display_data(self):
        """
        Display current data in tree view.

        Shows data from df_display which contains either:
        - Original gold layer data (after initial load)
        - Filtered and ranked prescriptive analysis results (after analysis)
        """
        for row in self.tree.get_children():
            self.tree.delete(row)

        for _, row in self.df_display.iterrows():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["nama_konser"],
                    row["lokasi"],
                    f"Rp{row['total_pengeluaran']:,.0f}",
                    row["affordability"],
                    (
                        f"{row.get('prescriptive_score', 0):.2f}"
                        if "prescriptive_score" in row
                        else "-"
                    ),
                ),
            )

    def _run_prescriptive(self):
        """
        Run prescriptive analytics and update display with ranked results.

        Updates df_display with top 10 feasible concerts ranked by score.
        Subsequent sorts will operate on this filtered/ranked dataset.
        """
        engine = Prescriptive(self.df_gold, self.budget)
        result = engine.calc_scores()

        if result is None:
            messagebox.showinfo("Hasil", "Tidak ada konser dalam budget Anda")
            return

        optimal, ranked = result

        self.df_display = ranked.head(10).copy()

        for row in self.tree.get_children():
            self.tree.delete(row)

        for _, row in self.df_display.iterrows():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["nama_konser"],
                    row["lokasi"],
                    f"Rp{row['total_pengeluaran']:,.0f}",
                    row["affordability"],
                    f"{row['prescriptive_score']:.3f}",
                ),
            )

        msg = (
            f"REKOMENDASI OPTIMAL (Score: {optimal['prescriptive_score']:.3f})\n\n"
            f"Konser: {optimal['nama_konser']}\n"
            f"Lokasi: {optimal['lokasi']}\n"
            f"Tanggal: {optimal['tanggal']}\n"
            f"Total Biaya: Rp{optimal['total_pengeluaran']:,.0f}\n"
            f"Sisa Budget: Rp{optimal['sisa_budget']:,.0f}\n"
            f"Merchandise: Rp{optimal['merchandise']:,.0f}\n\n"
            f"Breakdown Score:\n"
            f"• Cost Efficiency: {optimal['score_cost']:.2f}\n"
            f"• Budget Remaining: {optimal['score_remaining']:.2f}\n"
            f"• Experience Value: {optimal['score_experience']:.2f}"
        )

        messagebox.showinfo("Prescriptive Analytics Result", msg)
        self.status.config(text=f"Optimal: {optimal['nama_konser']}")

    def _reset(self):
        """
        Reset application state to initial conditions.

        Clears all data and UI elements:
        - Resets data variables (df_gold, df_display, budget, files, timestamp)
        - Clears tree view display
        - Disables download and analyze buttons
        - Resets status bar

        Does not clear local/MinIO storage files - only in-memory state.
        """
        self.df_gold = None
        self.df_display = None
        self.budget = None
        self.files_uploaded = []
        self.current_timestamp = None

        for row in self.tree.get_children():
            self.tree.delete(row)

        self.btn_download.config(state="disabled")
        self.btn_analyze.config(state="disabled")
        self.status.config(text="Ready")

        messagebox.showinfo("Reset", "Aplikasi telah direset. Silakan load CSV baru.")

    def _sort_column(self, df_col, display_col):
        """
        Sort table by clicked column header.

        Toggles sort direction on each click:
        - First click: ascending order
        - Second click: descending order
        - Third click: ascending again (cycle continues)

        Sorts df_display (current view) not df_gold (original):
        - Before analysis: sorts all gold layer data
        - After analysis: sorts only the top 10 ranked results

        Args:
            df_col: DataFrame column name to sort by
            display_col: Display column name for tracking sort state

        Special handling for numeric columns (Total, Score):
        - Sorts by actual numeric value, not string representation
        """
        if self.df_display is None:
            return

        self.sort_reverse[display_col] = not self.sort_reverse[display_col]

        sorted_df = self.df_display.copy()

        if df_col == "prescriptive_score" and df_col not in sorted_df.columns:
            sorted_df[df_col] = 0

        sorted_df = sorted_df.sort_values(
            by=df_col, ascending=not self.sort_reverse[display_col]
        )

        for row in self.tree.get_children():
            self.tree.delete(row)

        for _, row in sorted_df.iterrows():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["nama_konser"],
                    row["lokasi"],
                    f"Rp{row['total_pengeluaran']:,.0f}",
                    row["affordability"],
                    (
                        f"{row.get('prescriptive_score', 0):.2f}"
                        if "prescriptive_score" in row
                        else "-"
                    ),
                ),
            )
