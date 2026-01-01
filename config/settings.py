import os
from dotenv import load_dotenv

load_dotenv()

config = {
    "minio_endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "minio_secure": os.getenv("MINIO_SECURE", "false").lower() == "true",
    "minio_bucket": os.getenv("MINIO_BUCKET", "kpop-budget"),
    "bronze_folder": os.getenv("BRONZE_FOLDER", "bronze/"),
    "silver_folder": os.getenv("SILVER_FOLDER", "silver/"),
    "gold_folder": os.getenv("GOLD_FOLDER", "gold/"),
    "local_data_path": os.getenv("LOCAL_DATA_PATH", "data/"),
    "use_local": os.getenv("USE_LOCAL_STORAGE", "true").lower() == "true",
    "presigned_expiry": int(os.getenv("PRESIGNED_EXPIRY", "3600")),
}
