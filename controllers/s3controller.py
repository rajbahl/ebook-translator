import logging
import asyncio
import aiobotocore
import aioboto3
import os
from minio import Minio
from minio.error import ResponseError

logger = logging.getLogger("S3Controller")


class S3Controller:
    def __init__(self):
        self._loop = None
        self._minio_client = None

    def get_loop(self):
        return self._loop

    def set_loop(self, loop):
        self._loop = loop

    async def add_s3_connection(self):
        s3_access_key_id = os.getenv("S3_KEY_ID", "")
        s3_access_password = os.getenv("S3_PASSWORD", "")
        s3_host = os.getenv("S3_HOST", "minio.dev.muchneededllc.com")
        s3_port = int(os.getenv("S3_PORT", 9000))
        if self._loop is not None:
            self._minio_client = Minio(s3_host + ":" + str(s3_port), access_key=s3_access_key_id,
                                       secret_key=s3_access_password,
                                       secure=False)

        else:
            logger.error("Loop is unset when trying to open s3")
            raise Exception("Loop is unset when trying to open s3")

    async def download_file(self, bucket: str, location: str):
        if self._minio_client is None:
            logger.error("Error: Could not find minio client.")
            raise Exception("Error: Could not find minio client.")
        else:
            try:
                logger.debug("bucket: " + bucket + " location: " + location)
                data = self._minio_client.get_object(bucket_name=bucket, object_name=location)
                with open(location, 'wb') as file_data:
                    for d in data.stream(32 * 1024):
                        file_data.write(d)
            except ResponseError as e:
                logger.error("Error writing file. " + str(e))
            logger.info("Finished Download.")
