import os
import subprocess
from typing import List, Optional

def ls_bucket(target: Optional[str] = None, 
              bucket_id: Optional[str] = None, 
              return_list:Optional[bool]=False) -> None|List[str]:
    """List the files in the given directory in the given bucket
    
    :param target: Path to folder in the bucket to list the files. Defaults to workspace folder.
    :param bucket_id: The bucket id to list the files from. Defaults to environment variable WORKSPACE_BUCKET.
    :returns: If return_list is set to true a list of files from the given directory is returned

    Example:
    --------
    ls_bucket('datasets')
    """
    
    if bucket_id == None:
       bucket_id = os.getenv('WORKSPACE_BUCKET')
    
    if target == None:
        cmd = f"gsutil ls {bucket_id}"
    else:
        cmd = f"gsutil ls {bucket_id}/{target}"
    
    if return_list:
        return subprocess.check_output(cmd, shell=True).decode('utf-8').split("\n")[:-1]
    else:
        os.system(cmd)
