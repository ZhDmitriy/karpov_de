from aiobotocore.session import get_session
from contextlib import asynccontextmanager
import asyncio


S3_YANDEX_OBJECT_STORAGE_KEY = "YCAJEOl-sSVeK3kUgt6mIPa5-"
S3_YANDEX_OBJECT_STORAGE_SECRET_KEY = "YCNQKiYnMZxD8lq9X9kZUalGknrk00Hniymlb6R2"
S3_YANDEX_OBJECT_STORAGE_ENDPOINT = "https://storage.yandexcloud.net/"


class S3Client:
    """ YANDEX OBJECT STORAGE S3 client """

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket_name: str):
        self.config = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": endpoint_url
        }
        self.bucket_name = bucket_name
        self.session = get_session()

    @asynccontextmanager
    async def get_client(self):
        async with self.session.create_client("s3", **self.config) as client:
            yield client

    async def upload_file(self, file_path: str):
        object_name = file_path.split("\\")[-1]
        async with self.get_client() as client:
            with open(file_path, 'rb') as file:
                await client.put_object(
                    Bucket=self.bucket_name,
                    Key=object_name,
                    Body=file
                )


async def main():
    s3_client = S3Client(
        access_key=S3_YANDEX_OBJECT_STORAGE_KEY,
        secret_key=S3_YANDEX_OBJECT_STORAGE_SECRET_KEY,
        endpoint_url=S3_YANDEX_OBJECT_STORAGE_ENDPOINT,
        bucket_name="mapreduce"
    )

    await s3_client.upload_file(file_path="D:\\mapreduce\\yellow_tripdata_2020-01.csv")


if __name__ == '__main__':
    asyncio.run(main())