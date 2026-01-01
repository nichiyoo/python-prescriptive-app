import pandas as pd  # Untuk baca CSV dan manipulasi data
import numpy as np  # Untuk operasi matematika
import tkinter as tk  # Untuk buat GUI
from tkinter import filedialog, messagebox, ttk, simpledialog  # Komponen GUI
import os  # Untuk operasi file/folder
from datetime import datetime  # Untuk timestamp


class DataLakehouse:
    def __init__(self):
        # Setup path untuk 3 layer data
        self.bronze_path = "data/bronze/"
        self.silver_path = "data/silver/"
        self.gold_path = "data/gold/"
        self._ensure_directories()  # Buat folder jika belum ada

    def _ensure_directories(self):
        # Pastikan semua folder data ada
        for path in [self.bronze_path, self.silver_path, self.gold_path]:
            os.makedirs(path, exist_ok=True)

    def ingest_to_bronze(self, source_csv):
        # Simpan data mentah ke Bronze layer
        df = pd.read_csv(source_csv)  # Baca file CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Buat timestamp
        bronze_file = os.path.join(self.bronze_path, f"konser_raw_{timestamp}.csv")
        df.to_csv(bronze_file, index=False)  # Simpan file
        print(f"Bronze layer: {bronze_file}")
        return df

    def transform_to_silver(self, df_bronze):
        # Bersihkan data untuk Silver layer
        required_cols = [
            "nama_konser",
            "lokasi",
            "tanggal",
            "harga_tiket",
            "biaya_transport",
            "biaya_akomodasi",
            "merchandise",
            "total_pengeluaran",
        ]

        # Cek kolom wajib ada semua
        if not all(col in df_bronze.columns for col in required_cols):
            raise ValueError(f"Kolom tidak lengkap. Diperlukan: {required_cols}")

        df_silver = df_bronze.copy()  # Copy data agar tidak merusak original

        # Convert tipe data
        df_silver["tanggal"] = pd.to_datetime(
            df_silver["tanggal"], errors="coerce"
        )  # Jadikan datetime

        # Jadikan kolom numerik jadi tipe angka
        numeric_cols = [
            "harga_tiket",
            "biaya_transport",
            "biaya_akomodasi",
            "merchandise",
            "total_pengeluaran",
        ]
        for col in numeric_cols:
            df_silver[col] = pd.to_numeric(df_silver[col], errors="coerce")

        # Hapus data yang tidak lengkap atau salah
        df_silver = df_silver.dropna(
            subset=["nama_konser", "total_pengeluaran"]
        )  # Hapus baris kosong
        df_silver = df_silver[
            df_silver["total_pengeluaran"] >= 0
        ]  # Hapus nilai negatif

        # Simpan ke Silver layer
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        silver_file = os.path.join(self.silver_path, f"konser_cleaned_{timestamp}.csv")
        df_silver.to_csv(silver_file, index=False)
        print(f"Silver layer: {silver_file}")

        return df_silver

    def aggregate_to_gold(self, df_silver, user_budget):
        # Analisis data untuk Gold layer
        df_gold = df_silver.copy()

        # Tambah kolom skor efisiensi
        df_gold["efficiency_score"] = df_gold["total_pengeluaran"] / (
            df_gold["total_pengeluaran"].max() + 1
        )

        # Kategorikan berdasarkan budget user
        df_gold["affordability"] = df_gold["total_pengeluaran"].apply(
            lambda x: (
                "Sangat Terjangkau"
                if x <= user_budget * 0.5
                else (
                    "Terjangkau"
                    if x <= user_budget * 0.8
                    else ("Limit" if x <= user_budget else "Tidak Terjangkau")
                )
            )
        )

        # Hitung statistik per lokasi
        location_stats = (
            df_gold.groupby("lokasi")
            .agg({"total_pengeluaran": ["mean", "min", "max", "count"]})
            .round(0)
        )

        # Simpan ke Gold layer
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        gold_file = os.path.join(self.gold_path, f"konser_analytics_{timestamp}.csv")
        df_gold.to_csv(gold_file, index=False)
        print(f"Gold layer: {gold_file}")

        return df_gold, location_stats


class PrescriptiveEngine:
    def __init__(self, df, user_budget):
        self.df = df.copy()  # Copy data
        self.budget = user_budget  # Budget user

    def calculate_prescriptive_score(self):
        # Filter konser yang terjangkau
        feasible = self.df[self.df["total_pengeluaran"] <= self.budget].copy()

        if feasible.empty:  # Kalau tidak ada yang terjangkau
            return None

        # Hitung 3 jenis skor:
        # 1. Skor biaya (40%)
        feasible["score_cost"] = 1 - (
            feasible["total_pengeluaran"] / feasible["total_pengeluaran"].max()
        )

        # 2. Skor sisa budget (30%)
        feasible["sisa_budget"] = self.budget - feasible["total_pengeluaran"]
        feasible["score_remaining"] = (
            feasible["sisa_budget"] / feasible["sisa_budget"].max()
        )

        # 3. Skor pengalaman dari merchandise (30%)
        feasible["score_experience"] = (
            feasible["merchandise"] / feasible["merchandise"].max()
        )

        # Gabungkan semua skor dengan bobot
        feasible["prescriptive_score"] = (
            0.4 * feasible["score_cost"]
            + 0.3 * feasible["score_remaining"]
            + 0.3 * feasible["score_experience"]
        )

        # Cari konser dengan skor tertinggi
        optimal_idx = feasible["prescriptive_score"].idxmax()
        optimal = feasible.loc[optimal_idx]

        return optimal, feasible.sort_values("prescriptive_score", ascending=False)


class KPopBudgetApp:
    def __init__(self, root):
        self.root = root  # Window utama GUI
        self.root.title("Sistem Rekomendasi Konser K-Pop")  # Judul window
        self.root.geometry("900x700")  # Ukuran window

        # Buat komponen GUI
        tk.Label(
            root,
            text="SISTEM REKOMENDASI KONSER K-POP",
            font=("Arial", 16, "bold"),
            bg="#1a237e",
            fg="white",
        ).pack(fill="x", pady=10)

        # Frame untuk tombol
        btn_frame = tk.Frame(root)
        btn_frame.pack(pady=15)

        # Tombol untuk load CSV
        tk.Button(
            btn_frame,
            text="Load CSV (Bronze)",
            command=self.load_csv,
            font=("Arial", 11),
            bg="#4CAF50",
            fg="white",
            width=20,
        ).pack(side="left", padx=5)

        # Tombol analisis (awalnya disabled)
        self.btn_recommend = tk.Button(
            btn_frame,
            text="Analisis Prescriptive",
            command=self.run_prescriptive,
            state="disabled",
            font=("Arial", 11),
            bg="#FF9800",
            fg="white",
            width=20,
        )
        self.btn_recommend.pack(side="left", padx=5)

        # Frame untuk tabel hasil
        self.result_frame = tk.Frame(root)
        self.result_frame.pack(fill="both", expand=True, padx=20, pady=10)

        # Scrollbar untuk tabel
        self.tree_scroll = tk.Scrollbar(self.result_frame)
        self.tree_scroll.pack(side="right", fill="y")

        # Tabel (Treeview) untuk tampilkan data
        self.tree = ttk.Treeview(
            self.result_frame, yscrollcommand=self.tree_scroll.set, height=15
        )
        self.tree_scroll.config(command=self.tree.yview)

        # Setup kolom tabel
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

        # Status bar di bawah
        self.status = tk.Label(root, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.status.pack(side=tk.BOTTOM, fill=tk.X)

        # Inisialisasi lakehouse dan variabel
        self.lakehouse = DataLakehouse()
        self.df_gold = None  # Untuk simpan data gold
        self.user_budget = None  # Untuk simpan budget user

    def load_csv(self):
        # Dialog pilih file CSV
        file_path = filedialog.askopenfilename(
            title="Pilih file CSV mentah (Bronze Layer)",
            filetypes=[("CSV files", "*.csv")],
        )
        if not file_path:  # Kalau user cancel
            return

        try:
            self.status.config(text="Processing Bronze → Silver → Gold...")
            self.root.update()

            # Proses data melalui 3 layer
            df_bronze = self.lakehouse.ingest_to_bronze(file_path)  # Ke Bronze
            df_silver = self.lakehouse.transform_to_silver(df_bronze)  # Ke Silver

            # Minta input budget dari user
            budget = simpledialog.askinteger(
                "Budget Anda",
                "Masukkan budget dalam Rupiah (contoh: 5000000):",
                parent=self.root,
                minvalue=1,
            )
            if budget is None:  # Kalau user cancel
                self.status.config(text="Budget tidak dimasukkan")
                return

            self.user_budget = budget  # Simpan budget

            # Ke Gold layer dengan budget user
            self.df_gold, _ = self.lakehouse.aggregate_to_gold(df_silver, budget)

            # Tampilkan data di tabel
            self.display_data()
            self.btn_recommend.config(state="normal")  # Aktifkan tombol analisis
            self.status.config(text=f"Lakehouse ready | Budget: Rp{budget:,}")

        except Exception as e:
            messagebox.showerror("Error", f"Gagal memproses data:\n{str(e)}")
            self.status.config(text="Error")

    def display_data(self):
        # Hapus data lama di tabel
        for row in self.tree.get_children():
            self.tree.delete(row)

        # Masukkan data baru ke tabel
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

    def run_prescriptive(self):
        # Jalankan analisis preskriptif
        engine = PrescriptiveEngine(self.df_gold, self.user_budget)
        result = engine.calculate_prescriptive_score()

        if result is None:  # Kalau tidak ada yang terjangkau
            messagebox.showinfo("Hasil", "Tidak ada konser dalam budget Anda")
            return

        optimal, ranked = result  # Ambil hasil optimal dan ranking

        # Update tabel dengan ranking
        for row in self.tree.get_children():
            self.tree.delete(row)

        for _, row in ranked.head(10).iterrows():  # Tampilkan 10 terbaik
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

        # Tampilkan popup dengan rekomendasi optimal
        msg = (
            f"REKOMENDASI OPTIMAL (Prescriptive Score: {optimal['prescriptive_score']:.3f})\n\n"
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


if __name__ == "__main__":
    # Jalankan aplikasi
    try:
        root = tk.Tk()  # Buat window utama
        app = KPopBudgetApp(root)  # Inisialisasi app
        root.mainloop()  # Mulai GUI loop
    except Exception as e:
        print(f"GUI Error: {e}")
        print("Jalankan di environment dengan GUI support (bukan Colab)")
