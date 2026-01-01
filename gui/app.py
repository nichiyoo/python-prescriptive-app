import tkinter as tk
from tkinter import filedialog, messagebox, ttk, simpledialog
import zipfile
import os
from config.settings import config
from core.lakehouse import Lakehouse
from core.prescriptive import Prescriptive
from core.storage import storage


class KPopApp:
    """GUI application for K-pop concert budget recommendations"""

    def __init__(self, root):
        self.root = root
        self.root.title("Sistem Rekomendasi Konser K-Pop")
        self.root.geometry("900x700")

        self.lakehouse = Lakehouse()
        self.df_gold = None
        self.budget = None
        self.files_uploaded = []
        self.current_timestamp = None

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

        for col in self.tree["columns"]:
            self.tree.heading(col, text=col)
            self.tree.column(col, anchor="center", width=150)

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
            self._track_files()
            self._display_data()

            self.btn_analyze.config(state="normal")
            self.btn_download.config(state="normal")
            self.status.config(text=f"Lakehouse ready | Budget: Rp{budget:,}")

        except Exception as e:
            messagebox.showerror("Error", f"Gagal memproses data:\n{str(e)}")
            self.status.config(text="Error")

    def _track_files(self):
        """Track uploaded files for download"""
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
        """Download all lakehouse files as ZIP"""
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
        """Display data in tree view"""
        for row in self.tree.get_children():
            self.tree.delete(row)

        for _, row in self.df_gold.iterrows():
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
        """Run prescriptive analytics"""
        engine = Prescriptive(self.df_gold, self.budget)
        result = engine.calc_scores()

        if result is None:
            messagebox.showinfo("Hasil", "Tidak ada konser dalam budget Anda")
            return

        optimal, ranked = result

        for row in self.tree.get_children():
            self.tree.delete(row)

        for _, row in ranked.head(10).iterrows():
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
