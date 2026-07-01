import os
import subprocess
import sys
import json

def create_project(project_name: str = None):
    """
    Create a new phenopipe project with interactive configuration.
    
    This function initializes a new project by:
    1. Prompting for a project name (if not provided)
    2. Validating that the project doesn't already exist
    3. Retrieving workspace resources (GCS bucket and BigQuery dataset)
    4. Gathering project configuration interactively
    5. Creating a project directory with a project_settings.json configuration file
    
    Parameters
    ----------
    project_name : str, optional
        The name of the project to create. If None, the user will be prompted
        to enter a project name via command line input. Default is None.
    
    Returns
    -------
    None
        The function creates a project directory and configuration file as a side effect.
    
    Raises
    ------
    ValueError
        If a directory with the specified project_name already exists.
    subprocess.CalledProcessError
        If the 'wb resource list' command fails or returns invalid data.
    IndexError
        If no GCS_BUCKET or BQ_DATASET/BIGQUERY_DATASET resource is found
        in the workspace resources.
    
    Interactive Prompts
    -------------------
    The function will prompt for the following configuration parameters:
    
    - **Project Name**: Name of the project (only if not provided as argument)
    - **Author Name**: Name of the project author (defaults to the bucket creator)
    - **Bucket ID**: Google Cloud Storage bucket name (defaults to workspace bucket)
    - **Bucket Location**: Relative path to bucket location (defaults to "../../")
    - **Dataset CDR**: BigQuery dataset ID for clinical data (defaults to workspace dataset)
    - **Workspace ID**: Google Cloud Project ID (defaults to workspace project)
    - **Create Requirements**: Whether to create a requirements file (yes/no, defaults to yes)
    - **Requirements**: If creating requirements file, iteratively prompt for package
      specifications (GitHub URLs or PyPI packages) and their library names
    
    Output
    ------
    Creates a directory with the project_name containing:
    - project_settings.json: Configuration file with all project settings including
      author, bucket information, dataset references, and package requirements
    
    Examples
    --------
    >>> # Interactive mode with project name argument
    >>> create_project("my_analysis_project")
    Enter author name (existing_user):
    Enter bucket id (workspace-bucket):
    ...
    
    >>> # Interactive mode without project name (will prompt for it)
    >>> create_project()
    Project Name: my_analysis_project
    Enter author name (existing_user):
    ...
    
    Notes
    -----
    - Requires 'wb' (Workbench) CLI tool to be installed and configured
    - The workspace must have at least one GCS bucket and one BigQuery dataset
    - Default values are extracted from the workspace environment
    - Requirements can include both GitHub repositories and PyPI packages
    - The function uses interactive input, so it cannot be used in non-interactive contexts
    """    
    if project_name is None:
        project_name = input("Project Name: ")

    if os.path.isdir(project_name):
        raise ValueError("Project already exists!")

    resources = json.loads(subprocess.run(
        ["wb", "resource", "list", "--format=json"],
        capture_output=True, text=True, check=True
    ).stdout)
    bucket_info = list(filter(lambda x: x["resourceType"] == "GCS_BUCKET", resources))[0]
    dataset_info = list(filter(lambda x: x["resourceType"] in ["BQ_DATASET", "BIGQUERY_DATASET"], resources))[0]

    user_name = bucket_info["createdBy"]
    user_name = input(f"Enter author name ({user_name}):") or user_name

    bucket_id = bucket_info["bucketName"]
    bucket_id = input(f"Enter bucket id ({bucket_id}):") or bucket_id

    bucket_loc = "../.."
    bucket_loc = input(f"Enter bucket location ({bucket_loc}):") or bucket_loc


    dataset_cdr = dataset_info["id"]
    dataset_cdr = input(f"Enter dataset cdr ({dataset_cdr}):") or dataset_cdr

    ws_id = dataset_info["projectId"]
    ws_id = input(f"Enter workspace ({ws_id}):") or ws_id

    create_requirements = "yes"
    create_requirements = input(f"Create requirements file (yes/no) (Default to yes): ") or create_requirements

    reqs = dict()
    if create_requirements == "yes":
        req = input("Please enter requirements or press enter to skip: ")
        if "github" in req:
            req_lib = input("Please enter library name or press enter to skip: ")
        else:
            req_lib = req
        while req:
            reqs.update({req_lib:req})
            req = input("Please enter requirements or press enter to skip: ")        
            if "github" in req:
                req_lib = input("Please enter library name or press enter to skip: ")
            else:
                req_lib = req
        

    setup_dict = {
        "project_name": project_name,
        "user_name": user_name,
        "bucket_id": bucket_id,
        "bucket_loc": bucket_loc,
        "dataset_cdr":dataset_cdr,
        "ws_id":ws_id,
        "reqs":reqs
    }
    os.makedirs(project_name)
    with open(f'{project_name}/project_settings.json', "w") as f:
        f.write(json.dumps(setup_dict))
