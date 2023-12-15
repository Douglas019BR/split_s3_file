import boto3


class AWSService:
    def __init__(self, service):
        self.client = boto3.client(
            service,
            aws_access_key_id="aws_access_key_id",
            aws_secret_access_key="aws_secret_access_key",
            region_name="us-east-1",
        )


class AWSS3Service(AWSService):  # pragma: no cover
    def __init__(self):
        super().__init__("s3")

    def get_object(self, Bucket, Key):
        return self.client.get_object(
            Bucket=Bucket,
            Key=Key,
        )

    def put_object(self, Bucket, Key, Body):
        return self.client.put_object(Bucket=Bucket, Key=Key, Body=Body)
