from typing import List
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.tasks.task import completion
from phenopipe.vocab.concepts.survey_questions import SMOKING_CURRENT_CODES
from phenopipe.query_builders import survey_query

class SurveyPt(GetData):
    
    survey_codes: List[str]

    @completion
    def complete(self):
        '''
        Generic query for survey data
        '''
        survey_query_str = survey_query(self.survey_code)
        self.output = self.env_vars["query_conn"].get_query_df(survey_query_str,
                                                                self.task_name, 
                                                                self.lazy,
                                                                self.cache,
                                                                self.cache_local)
        
    def set_output_dtypes_and_names(self):
        self.output = self.output.rename({"condition_start_date":self.date_col}).select("person_id", self.date_col)
        self.set_date_column_dtype()
