from typing import Optional
import polars as pl
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.tasks.task import completion
from phenopipe.vocab.icds.conditions import HEART_FAILURE_ICDS
from phenopipe.query_builders import icd_inpatient_query, icd_outpatient_query

class HeartFailurePt(GetData):
    
    #: if query is large according to google cloud api
    large_query: Optional[bool] = False

    cache_type:str = "std"

    date_col:str = "heart_failure_first_icd_entry_date"
    
    @completion
    def complete(self):
        '''
        Query heart failure phenotype defined as at least 1 inpatient or 2 outpatient ICD codes
        :returns: 
        '''
        inpatient_query = icd_inpatient_query(HEART_FAILURE_ICDS)
        outpatient_query = icd_outpatient_query(HEART_FAILURE_ICDS)

        inpatient_df = self.env_vars["query_conn"].get_query_rows(inpatient_query, return_df=True)
        outpatient_df = self.env_vars["query_conn"].get_query_rows(outpatient_query, return_df=True)
        def process_query_df(df):
            if isinstance(df.collect_schema().get("condition_start_date"), pl.String):
                df = df.output.with_columns(pl.col("condition_start_date").str.to_date())
            df = df.sort("condition_start_date")
            df_count = df.unique(["person_id", "condition_start_date"]).group_by("person_id").len()
            return df_count
        
        in_out_count = process_query_df(inpatient_df).join(process_query_df(outpatient_df), on="person_id", how="full", coalesce=True, suffix="_out")
        in_out_count = in_out_count.with_columns(pl.col("^.*len.*$").fill_null(0))
        in_out_count = in_out_count.with_columns(((pl.col("len")>0) | (pl.col("len_out")>1)).alias("heart_failure_status"))
        heart_failure_df = inpatient_df.vstack(outpatient_df).unique(["person_id", "condition_start_date"]).join(in_out_count.select("person_id", "heart_failure_status"), on="person_id")
        heart_failure_df = heart_failure_df.group_by("person_id").agg(pl.col("condition_start_date").min())
        self.output = heart_failure_df
    def set_output_dtypes_and_names(self):
        self.output = self.output.rename({"condition_start_date":"heart_failure_first_icd_entry_date"})
        self.set_date_column_dtype()
