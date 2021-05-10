"""S3 Filestore Class."""

import os.path
import logging

import boto3
from botocore.exceptions import ClientError


from dbtease.filestores.base import Filestore


class S3Filestore(Filestore):
    """S3 Filestore Connection."""

    def __init__(self, path, profile=None):
        # trim the initial off it
        if path.lower().startswith("s3://"):
            path = path[5:]
        self.bucket, _, self.path = path.partition("/")
        # add a trailing "/" if it doesn't exist.
        if not self.path.endswith("/"):
            path += "/"
        # Optionally accept a profile argument
        self.profile = profile

    def upload_files(self, *paths: str):
        session = boto3.Session(profile_name=self.profile)
        s3_client = session.client('s3')
        for path in paths:
            _, fname = os.path.split(path)
            try:
                response = s3_client.upload_file(path, self.bucket, self.path + fname)
            except ClientError as e:
                logging.error(e)
                logging.error(response)
                raise e
