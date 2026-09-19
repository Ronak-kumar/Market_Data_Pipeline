import boto3
import boto3.session
from pathlib import Path
from shared.observability import get_logger

logger = get_logger(__name__)


class S3BucketManager:
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


# S3BucketManager().upload_files(filepath="D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/shared/utils/cache/2026-08-25/MCX_FO.parquet", bucket_name="marketdata-pipeline", destination_prefix="bronze_cache_storage_market_data/upstox/")