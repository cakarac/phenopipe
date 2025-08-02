import polars as pl
from phenopipe.tasks.get_data.fixed_query import FixedQuery

class GetSleep(FixedQuery):

    #: if query is large according to google cloud api
    large_query:bool = True
    
    query:str = "SLEEP_QUERY"

    def set_output_dtypes_and_names(self):
        super().set_date_column_dtype("date")
        if isinstance(self.output.collect_schema().get("is_main_sleep"), pl.String):
            self.output = self.output.with_columns(pl.col("is_main_sleep").replace_strict({"true": True, "false": False}))
        
