import polars as pl
from phenopipe.tasks.get_data.fixed_query import FixedQuery
from phenopipe.query_builders.fixed_queries import DEMOGRAPHICS_QUERY
from phenopipe.preprocess.recode_sex_func import recode_sex


class GetDemographics(FixedQuery):
    query: str = DEMOGRAPHICS_QUERY
    recode_sex: bool = False

    def set_output_dtypes_and_names(self):
        if isinstance(self.output.collect_schema().get("date_of_birth"), pl.String):
            self.output = self.output.with_columns(
                pl.col("date_of_birth").str.to_datetime("%Y-%m-%d %H:%M:%S %Z")
            )
        if isinstance(self.output.collect_schema().get("date_of_birth"), pl.Datetime):
            self.output = self.output.with_columns(pl.col("date_of_birth").dt.date())
        if self.recode_sex:
            self.output = recode_sex(self.output)
