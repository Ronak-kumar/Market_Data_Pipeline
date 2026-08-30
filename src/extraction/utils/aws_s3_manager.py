import boto3
import boto3.session
import requests

class S3BucketManager:
    def __init__(self):
        # Create your own session
        self.my_session = boto3.session.Session()

        # Now we can create low-level clients or resource clients from our custom session
        self.sqs = self.my_session.client('sqs')
        self.s3_client = self.my_session.client("s3")
        self.s3 = self.my_session.resource('s3')

    def upload_files(self, filepath: str, bucket_name: str, destination_prefix: str) -> requests.Response:
        destination_postfix = "/".join(filepath.parts[-2:])
        response = self.s3_client.upload_file(
            filepath,
            bucket_name,
            destination_prefix + destination_postfix,)

        return response



# S3BucketManager().upload_files(filepath="D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/2026-08-25/MCX_FO.parquet", bucket_name="marketdata-pipeline", destination_prefix="bronze_cache_storage_market_data/upstox/")