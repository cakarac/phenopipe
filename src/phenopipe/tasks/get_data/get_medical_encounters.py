from typing import Optional
import inflection
import polars as pl
from pydantic import computed_field
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.tasks.task import completion

class GetMedicalEncounters(GetData):

    #: if query is large according to google cloud api
    large_query: Optional[bool] = False
    
    #: which medical encounter to return (first/last/all)
    time_select: str = "last"

    @computed_field
    @property
    def task_name(self) -> str:
        return inflection.underscore(f'{self.__class__.__name__}_{self.time_select}')
    
    @completion
    def complete(self):
        '''
        Query medical encounters (first/last/all) and update self.output with resulting dataframe
        '''
        '''Runs medical encounters query with given local and self.time_select (first/last/all)'''
        match self.time_select:
            case "last":
                q_select = "MAX"
            case "first":
                q_select = "MIN"
            case _:
                raise ValueError("Unknown select statement!")
        query = f'''
                    WITH ehr AS (
                    SELECT person_id, {q_select}(m.measurement_date) AS date
                    FROM `measurement` AS m
                    LEFT JOIN `measurement_ext` AS mm on m.measurement_id = mm.measurement_id
                    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
                    GROUP BY person_id

                    UNION DISTINCT

                    SELECT person_id, {q_select}(m.condition_start_date) AS date
                    FROM `condition_occurrence` AS m
                    LEFT JOIN `condition_occurrence_ext` AS mm on m.condition_occurrence_id = mm.condition_occurrence_id
                    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
                    GROUP BY person_id

                    UNION DISTINCT

                    SELECT person_id, {q_select}(m.procedure_date) AS date
                    FROM `procedure_occurrence` AS m
                    LEFT JOIN `procedure_occurrence_ext` AS mm on m.procedure_occurrence_id = mm.procedure_occurrence_id
                    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
                    GROUP BY person_id

                    UNION DISTINCT

                    SELECT person_id, {q_select}(m.visit_end_date) AS date
                    FROM `visit_occurrence` AS m
                    LEFT JOIN `visit_occurrence_ext` AS mm on m.visit_occurrence_id = mm.visit_occurrence_id
                    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
                    GROUP BY person_id

                    UNION DISTINCT

                    SELECT person_id, {q_select}(m.drug_exposure_start_date) AS date
                    FROM `drug_exposure` AS m
                    GROUP BY person_id
                    )

                    SELECT person_id, {q_select}(date) as {self.time_select}_medical_encounters_entry_date
                    FROM ehr
                    GROUP BY person_id
                '''
        self.output = self.query_conn.get_query(query, self.query_name, self.large_query)        
        if isinstance(self.output.collect_schema().get(f"{self.time_select}_medical_encounters_entry_date"), pl.String):
            self.output = self.output.with_columns(pl.col(f"{self.time_select}_medical_encounters_entry_date").str.to_date("%Y-%m-%d"))
        