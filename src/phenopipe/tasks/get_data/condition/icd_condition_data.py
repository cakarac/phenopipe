from typing import List
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.tasks.task import completion
from phenopipe.query_builders import icd_condition_query


class IcdConditionData(GetData):
    icd_codes: dict[str, List[str]]

    def _complete(self):
        """
        Generic icd condition occurance query phenotype
        """
        self.output = self.env_vars["query_conn"].get_query(
            icd_condition_query(self.icd_codes), self.lazy
        )

    def set_output_dtypes_and_names(self):
        self.output = self.output.rename(
            {"condition_start_date": self.date_col}
        ).select("person_id", self.date_col)
        self.set_date_column_dtype()
