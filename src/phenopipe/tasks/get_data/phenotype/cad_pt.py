import polars as pl
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.vocab.icds.conditions.cad_vocab import CAD_ICDS
from phenopipe.vocab.cpts import CAD_CPT_CODES
from phenopipe.query_builders import icd_condition_query, cpt_procedure_query


class CadPt(GetData):
    date_col: str = "cad_entry_date"

    def _complete(self):
        """
        Query CAD phenotype defined as at least 2 ICD codes (ICD9 or ICD10) AND at least 1 CPT procedure code
        """
        icd_query = icd_condition_query(CAD_ICDS)
        cpt_query = cpt_procedure_query(CAD_CPT_CODES)

        icd_df = self.env_vars["query_conn"].get_query(icd_query)
        cpt_df = self.env_vars["query_conn"].get_query(cpt_query)

        if isinstance(icd_df.collect_schema().get("condition_start_date"), pl.String):
            icd_df = icd_df.with_columns(
                pl.col("condition_start_date").str.to_date()
            )
        if isinstance(cpt_df.collect_schema().get("entry_date"), pl.String):
            cpt_df = cpt_df.with_columns(
                pl.col("entry_date").str.to_date()
            )

        icd_df = icd_df.with_columns(pl.col("person_id").cast(pl.String))
        cpt_df = cpt_df.with_columns(pl.col("person_id").cast(pl.String))

        icd_agg = (
            icd_df.group_by("person_id")
            .agg(
                pl.col("condition_start_date").len().alias("icd_count"),
                pl.col("condition_start_date").min().alias("icd_entry_date"),
            )
            .filter(pl.col("icd_count") >= 2)
            .select("person_id", "icd_entry_date")
        )

        cpt_agg = (
            cpt_df.group_by("person_id")
            .agg(pl.col("entry_date").min().alias("cpt_entry_date"))
            .select("person_id", "cpt_entry_date")
        )

        cad_df = (
            icd_agg.join(cpt_agg, on="person_id", how="inner")
            .with_columns(
                pl.min_horizontal("icd_entry_date", "cpt_entry_date").alias("cad_entry_date")
            )
            .select("person_id", "cad_entry_date")
        )

        self.output = cad_df

    def set_output_dtypes_and_names(self):
        self.set_date_column_dtype()
