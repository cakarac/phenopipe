def get_big_query(query: str,
                  location: str,
                  bucket_id: Optional[str] = None,
                  default_dataset: Optional[str] = None) -> pl.DataFrame:
    '''
    Runs the given query and saves the resulting dataframe to the given bucket and location and returns the dataframe
    :param query: Query string to run with google big query.
    :param location: bucket folder to save the output.
    :param bucket_id: bucket id to save the result. Defaults to environment variable WORKSPACE_BUCKET.
    :param default_dataset: default dataset to validate table name. Defaults to environment variable WORKSPACE_CDR.
    '''
    if bucket_id is None:
        bucket_id = os.getenv("WORKSPACE_BUCKET")
    if default_dataset is None:
        default_dataset = os.getenv("WORKSPACE_CDR")
    
    client = bigquery.Client()
    res = client.query_and_wait(query, job_config = bigquery.job.QueryJobConfig(default_dataset=default_dataset))
    ex_res = client.extract_table(res._table, f'{bucket_id}/{location}')
    if ex_res.result().done():
        print(f"Given query is successfully saved into {location}")
    return pl.from_arrow(res.to_arrow())