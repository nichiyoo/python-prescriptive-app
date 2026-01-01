import os
from config.settings import config


class LocalStorage:
    """
    Local storage client with file operations.
    """

    def __init__(self):
        self.local_data_path = config["local_data_path"]
        self.bronze = config["bronze_folder"]
        self.silver = config["silver_folder"]
        self.gold = config["gold_folder"]

        self.local_bronze = f"{self.local_data_path}{self.bronze}"
        self.local_silver = f"{self.local_data_path}{self.silver}"
        self.local_gold = f"{self.local_data_path}{self.gold}"
        self._ensure_local()

    def local_path(self, folder):
        """Get local path for a folder"""
        return os.path.join(self.local_data_path, folder)

    def _ensure_local(self):
        """Create local directories if needed"""
        for path in [self.local_bronze, self.local_silver, self.local_gold]:
            os.makedirs(path, exist_ok=True)

    def upload(self, data, filename, folder):
        """
        Upload file to local folder.

        Creates a file in the local storage at the specified folder location.
        Handles both BytesIO objects and raw data formats. Creates the target
        folder if it doesn't exist.

        Returns full path to the created file.
        """
        folder_path = os.path.join(self.local_data_path, folder)
        os.makedirs(folder_path, exist_ok=True)
        local_path = os.path.join(folder_path, filename)

        if hasattr(data, "seek") and hasattr(data, "read"):
            data.seek(0)
            with open(local_path, "wb") as f:
                f.write(data.read())
        else:
            with open(local_path, "w" if isinstance(data, str) else "wb") as f:
                if isinstance(data, str):
                    f.write(data)
                else:
                    f.write(data)

        return local_path

    def download(self, filepath):
        """Download file as bytes"""
        with open(filepath, "rb") as f:
            return f.read()

    def delete(self, filepath):
        """Delete file from local storage"""
        if os.path.exists(filepath):
            os.remove(filepath)
            return True
        return False

    def list_files(self, folder):
        """List files in folder"""
        folder_path = os.path.join(self.local_data_path, folder)
        if os.path.exists(folder_path):
            return [
                os.path.join(folder, f)
                for f in os.listdir(folder_path)
                if os.path.isfile(os.path.join(folder_path, f))
            ]
        return []
