import os
import re
import subprocess
from typing import List, Optional
from google.cloud import storage

def remove_from_bucket(files: str|List[str], 
                       bucket_id: Optional[str] = None) -> None:
    """Removes the file from the bucket

    :param file_path: Path to file to remove.
    :param bucket_id: The bucket id to remove the file from. Defaults to environment variable WORKSPACE_BUCKET.
    
    Example:
    --------
    remove_from_bucket('datasets/fitbit.csv')
    """
    if bucket_id == None:
        bucket_id = os.getenv('WORKSPACE_BUCKET')
    if isinstance(files, list):
        for file in files:
            subprocess.check_output(["gcloud", "storage", "rm", f'{bucket_id}/{file}'])
    else:
        bucket_name = bucket_id.replace("gs://", "")
    
        client = storage.Client()
        all_blobs = client.list_blobs(bucket_name)
        for blob in all_blobs:
            match = re.match(re.compile(files), blob.name)
            if match:
                blob.delete()