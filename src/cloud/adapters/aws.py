import boto3
import boto3.session
from pathlib import Path
from shared.observability import get_logger
from cloud import cloud_registry, DefaultCloud

logger = get_logger(__name__)


@cloud_registry.register("aws")
class S3BucketManager(DefaultCloud):
    def __init__(self):
        logger.debug("Initializing S3BucketManager")
        # Create your own session
        self.my_session = boto3.session.Session()

        # Now we can create low-level clients or resource clients from our custom session
        self.sqs = self.my_session.client('sqs')
        self.s3_client = self.my_session.client("s3")
        self.s3 = self.my_session.resource('s3')
        logger.debug("S3 clients initialized")

    def upload_files(self, filepath: Path, bucket_name: str, destination_prefix: str) -> None:
        logger.info("Uploading file to S3", extra={"filepath": str(filepath), "bucket": bucket_name, "prefix": destination_prefix})
        destination_postfix = "/".join(filepath.parts[-2:])
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