import pandas as pd
from datetime import datetime
from io import BytesIO
from config.settings import config
from core.minio import MinioStorage
from core.localstorage import LocalStorage


class Lakehouse:
    """Data lakehouse with bronze/silver/gold layers"""

    def __init__(self):
        self.bronze = config["bronze_folder"]
        self.silver = config["silver_folder"]
        self.gold = config["gold_folder"]
        self.storage = LocalStorage() if config["use_local"] else MinioStorage()

    def _ts(self):
        """Generate timestamp for filenames"""
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def _save(self, df, filename, folder):
        """
        Save DataFrame to configured storage.
        """
        buffer = BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        obj_name = self.storage.upload(buffer, filename, folder)
        print(f"Saved to {obj_name}")

    def ingest_bronze(self, csv_path):
        """Load raw CSV into bronze layer"""
        df = pd.read_csv(csv_path)
        filename = f"konser_raw_{self._ts()}.csv"
        self._save(df, filename, self.bronze)
        return df

    def transform_silver(self, df_bronze):
        """
        Clean and transform raw data for silver layer.

        Transformation steps:
        1. Validate required columns exist
        2. Convert 'tanggal' to datetime (coerce errors to NaT)
        3. Convert numeric columns to proper numeric types (coerce errors to NaN)
        4. Remove rows with missing concert name or total cost
        5. Remove rows with negative total cost (data quality filter)
        6. Save cleaned data to silver layer

        Returns cleaned DataFrame ready for analytics.
        """
        req_cols = [
            "nama_konser",
            "lokasi",
            "tanggal",
            "harga_tiket",
            "biaya_transport",
            "biaya_akomodasi",
            "merchandise",
            "total_pengeluaran",
        ]

        if not all(col in df_bronze.columns for col in req_cols):
            raise ValueError(f"Missing columns. Required: {req_cols}")

        df = df_bronze.copy()
        df["tanggal"] = pd.to_datetime(df["tanggal"], errors="coerce")

        num_cols = [
            "harga_tiket",
            "biaya_transport",
            "biaya_akomodasi",
            "merchandise",
            "total_pengeluaran",
        ]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["nama_konser", "total_pengeluaran"])
        df = df[df["total_pengeluaran"] >= 0]

        filename = f"konser_cleaned_{self._ts()}.csv"
        self._save(df, filename, self.silver)
        return df

    def aggregate_gold(self, df_silver, budget):
        """
        Create analytics-ready gold layer with derived metrics.

        Adds calculated columns for business intelligence:

        1. efficiency_score: Normalized cost ratio (0-1 scale)
           - Lower cost concerts get higher scores
           - Formula: cost / (max_cost + 1)

        2. affordability: Budget-based categorization
           - "Sangat Terjangkau": <= 50% of budget
           - "Terjangkau": <= 80% of budget
           - "Limit": <= 100% of budget
           - "Tidak Terjangkau": > budget

        3. location_stats: Aggregated statistics by location
           - Mean, min, max, count of total_pengeluaran per location

        Returns (gold_df, location_stats) tuple.
        """
        df = df_silver.copy()

        df["efficiency_score"] = df["total_pengeluaran"] / (
            df["total_pengeluaran"].max() + 1
        )

        df["affordability"] = df["total_pengeluaran"].apply(
            lambda x: (
                "Sangat Terjangkau"
                if x <= budget * 0.5
                else (
                    "Terjangkau"
                    if x <= budget * 0.8
                    else "Limit" if x <= budget else "Tidak Terjangkau"
                )
            )
        )

        loc_stats = (
            df.groupby("lokasi")
            .agg(
                {
                    "total_pengeluaran": [
                        "mean",
                        "min",
                        "max",
                        "count",
                    ]
                }
            )
            .round(0)
        )

        filename = f"konser_analytics_{self._ts()}.csv"
        self._save(df, filename, self.gold)
        return df, loc_stats
