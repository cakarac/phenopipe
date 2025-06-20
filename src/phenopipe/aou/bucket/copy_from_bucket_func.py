import os
import subprocess
from typing import Optional, List
import concurrent.futures
import warnings
from .ls_bucket_func import ls_bucket

def copy_from_bucket(files: str|List[str],
                     target_folder: Optional[str] = "bucket_io",
                     bucket_id: Optional[str] = None) -> None:
    """
    Copies a file from specified bucket and path into the enviroment workspace.
    
    :param file_path: Path to the file to copy from bucket or a list of files. It accepts a string pattern.
    :param target_folder: Path of the folder to copy the files
    :param bucket_id: The bucket id to copy the file from. Defaults to the environment variable 'WORKSPACE_BUCKET'.
    
    Example:
    --------
    copy_from_bucket('datasets/fitbit.csv')
    """
    
    if bucket_id == None:
        bucket_id = os.getenv('WORKSPACE_BUCKET')
    if isinstance(files, str):
        files = ls_bucket(files, bucket_id=bucket_id, return_list=True)
    else:
        files = [f'{bucket_id}/{f}' for f in files]
    if len(files) == 0:
        raise ValueError("No mathching files with given pattern")
    
    target_files = [f.replace(bucket_id, target_folder) for f in files]
    for folder in set(["/".join(f.split("/")[:-1]).replace(bucket_id, target_folder) for f in files]):
        if not os.path.isdir(folder) and folder != "":
            os.makedirs(folder)
    
    def cp_file(file, target):
        subprocess.check_output(["gcloud", "storage", "cp", file, target])
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_file = {executor.submit(cp_file, f, t): (f, t) for f,t in zip(files, target_files)}
        for future in concurrent.futures.as_completed(future_to_file):
            f = future_to_file[future][0]
            t = future_to_file[future][1]
            try:
                res = future.result()
            except subprocess.CalledProcessError as e:
                warnings.warn(f"Failed to copy {f} to {t}")
            else:
                print(f"Succesfully copied {f} to {t}")