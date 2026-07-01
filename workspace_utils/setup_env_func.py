import subprocess
import os
from pathlib import Path
import json
import importlib


def setup_env():
    """
    Initialize the phenopipe project environment from project settings.

    This function configures the runtime environment by:
    1. Reading project settings from project_settings.json
    2. Setting environment variables for workspace resources
    3. Installing missing required packages

    The function must be called from the project root directory containing
    the project_settings.json file created by create_project().

    Parameters
    ----------
    None

    Returns
    -------
    None
        The function sets environment variables and installs packages as side effects.

    Raises
    ------
    FileNotFoundError
        If project_settings.json is not found in the current working directory.
    json.JSONDecodeError
        If project_settings.json is not valid JSON.
    KeyError
        If required keys are missing from project_settings.json
        (ws_id, dataset_cdr, bucket_id, bucket_loc, reqs).
    subprocess.CalledProcessError
        If pip installation of a required package fails.

    Environment Variables Set
    --------------------------
    - **WORKSPACE_CDR**: BigQuery dataset reference in format "{project_id}.{dataset_id}"
    - **WORKSPACE_BUCKET**: Google Cloud Storage bucket path in format "gs://{bucket_name}"
    - **BUCKET_LOC**: Relative path to bucket location within the project

    Dependencies
    --------
    Reads from `project_settings.json` which should contain:
    - ws_id (str): Google Cloud Project ID
    - dataset_cdr (str): BigQuery dataset ID
    - bucket_id (str): GCS bucket name
    - bucket_loc (str): Relative bucket location path
    - reqs (dict): Dictionary of {library_name: requirement_spec} pairs

    Examples
    --------
    >>> # Call from project root directory
    >>> setup_env()
    # Sets environment variables and installs missing packages

    >>> # Check environment variables after setup
    >>> import os
    >>> setup_env()
    >>> print(os.environ["WORKSPACE_CDR"])
    'my-project.my_dataset'
    >>> print(os.environ["WORKSPACE_BUCKET"])
    'gs://my-bucket'

    Notes
    -----
    - Must be called from the project root directory (where project_settings.json exists)
    - Typically called at the start of a project script or notebook
    - Only installs packages that are not already available in the current environment
    - Package installation uses pip; supports both PyPI packages and GitHub URLs
    - The environment variables persist for the duration of the Python process
    - When using in Jupyter notebooks, call this at the beginning of the first cell

    See Also
    --------
    create_project : Creates the project_settings.json file needed by this function
    """
    with open(Path(os.getcwd()) / "project_settings.json", "r") as f:
        settings = json.loads(f.read())
    os.environ["WORKSPACE_CDR"] = f"{settings['ws_id']}.{settings['dataset_cdr']}"
    os.environ["WORKSPACE_BUCKET"] = f"gs://{settings['bucket_id']}"
    os.environ["BUCKET_LOC"] = settings['bucket_loc']

    for lib, req in settings["reqs"].items():
        if importlib.util.find_spec(lib) is None:  
            print(subprocess.run(["pip", "install", req]))
