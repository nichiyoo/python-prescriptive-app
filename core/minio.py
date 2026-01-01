from minio import Minio
from datetime import timedelta
from config.settings import config


class MinioStorage:
    """
    MinIO storage client with file operations.
    """

    def __init__(self):
        self.client = Minio(
            endpoint=config["minio_endpoint"],
            access_key=config["minio_access_key"],
            secret_key=config["minio_secret_key"],
            secure=config["minio_secure"],
        )
        self.bucket = config["minio_bucket"]
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Create bucket if not exists"""
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def upload(self, data, filename, folder):
        """
        Upload file to MinIO folder.

        Creates an object in the MinIO bucket with the specified filename
        in the given folder. The data is read from a BytesIO object and
        uploaded with CSV content type.

        Returns object name in MinIO bucket.
        """
        obj_name = f"{folder}{filename}"

        data.seek(0, 2)
        size = data.tell()
        data.seek(0)

        self.client.put_object(
            bucket_name=self.bucket,
            object_name=obj_name,
            data=data,
            length=size,
            content_type="text/csv",
        )
        return obj_name

    def get_url(self, obj_name):
        """
        Generate pre-signed download URL for temporary file access.

        Creates a time-limited URL that allows direct download from MinIO
        without requiring authentication. URL expires based on config
        (default: 1 hour).

        This is the recommended approach for file downloads as it:
        - Reduces server load (direct MinIO access)
        - Provides temporary access without permanent permissions
        - Works well with browser downloads
        """
        return self.client.presigned_get_object(
            bucket_name=self.bucket,
            object_name=obj_name,
            expires=timedelta(seconds=config["presigned_expiry"]),
        )

    def download(self, obj_name):
        """Download file as bytes"""
        resp = self.client.get_object(self.bucket, obj_name)
        data = resp.read()
        resp.close()
        resp.release_conn()
        return data

    def delete(self, obj_name):
        """Delete file from MinIO"""
        self.client.remove_object(self.bucket, obj_name)
        return True

    def list_files(self, folder):
        """List files in folder"""
        objs = self.client.list_objects(self.bucket, prefix=folder, recursive=True)
        return [obj.object_name for obj in objs]
