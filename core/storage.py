from minio import Minio
from datetime import timedelta
from config.settings import config


class Storage:
    """MinIO storage client with file operations"""

    def __init__(self):
        if not config["use_local"]:
            self.client = Minio(
                endpoint=config["minio_endpoint"],
                access_key=config["minio_access_key"],
                secret_key=config["minio_secret_key"],
                secure=config["minio_secure"],
            )
            self.bucket = config["minio_bucket"]
            self._ensure_bucket()
        else:
            self.client = None

    def _ensure_bucket(self):
        """Create bucket if not exists"""
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def upload(self, data, filename, folder):
        """Upload file to MinIO folder"""
        if not self.client:
            return None

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
        """Generate pre-signed download URL"""
        if not self.client:
            return None

        return self.client.presigned_get_object(
            bucket_name=self.bucket,
            object_name=obj_name,
            expires=timedelta(seconds=config["presigned_expiry"]),
        )

    def download(self, obj_name):
        """Download file as bytes"""
        if not self.client:
            return None

        resp = self.client.get_object(self.bucket, obj_name)
        data = resp.read()
        resp.close()
        resp.release_conn()
        return data

    def delete(self, obj_name):
        """Delete file from MinIO"""
        if not self.client:
            return False

        self.client.remove_object(self.bucket, obj_name)
        return True

    def list_files(self, folder):
        """List files in folder"""
        if not self.client:
            return []

        objs = self.client.list_objects(self.bucket, prefix=folder, recursive=True)
        return [obj.object_name for obj in objs]


storage = Storage()
