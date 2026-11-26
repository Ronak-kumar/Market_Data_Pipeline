import boto3
import boto3.session
from pathlib import Path
from shared.observability import get_logger
from cloud import cloud_registry, DefaultCloud
import os

logger = get_logger(__name__)

@cloud_registry.register("aws")
class S3BucketManager(DefaultCloud):
    def __init__(self):
        logger.debug("Initializing S3BucketManager")
        # Create your own session
        # self.my_session = boto3.session.Session()

        self.my_session = boto3.session.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-south-1'))

        # Now we can create low-level clients or resource clients from our custom session
        # self.sqs = self.my_session.client('sqs')
        self.s3_client = self.my_session.client("s3")
        # self.s3 = self.my_session.resource('s3')
        logger.debug("S3 clients initialized")

    def upload_files(self, filepath: Path, bucket_name: str, destination_prefix: str) -> None:
        logger.info("Uploading file to S3", extra={"filepath": str(filepath), "bucket": bucket_name, "prefix": destination_prefix})
        destination_postfix = "/".join(filepath.parts[-4:])
        try:
            self.s3_client.upload_file(
                str(filepath),
                bucket_name,
                f"{destination_prefix.rstrip('/')}/{destination_postfix}"
            )
            logger.info("File uploaded successfully", extra={"filepath": str(filepath), "bucket": bucket_name, "destination": f"{destination_prefix.rstrip('/')}/{destination_postfix}"})
        except Exception as e:
            logger.error("S3 upload failed", extra={"filepath": str(filepath), "bucket": bucket_name, "error": str(e)}, exc_info=True)
            raise