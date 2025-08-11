from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.tasks.task import completion
from phenopipe.query_builders import icd_inpatient_query


class InpatientData(GetData):
    @completion
    def complete(self):
        """
        Generic class to query inpatient conditions
        """
        inpatient_query = icd_inpatient_query(self.inp_codes)

        self.output = self.env_vars["query_conn"].get_query_df(inpatient_query)

    def set_output_dtypes_and_names(self):
        self.output = self.output.rename(
            {"condition_start_date": self.date_col}
        ).select("person_id", self.date_col)
        self.set_date_column_dtype()
