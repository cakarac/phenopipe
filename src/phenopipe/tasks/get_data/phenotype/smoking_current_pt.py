from typing import Optional, List
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.vocab.concepts.survey_questions import SMOKING_CURRENT_CODES

class SmokingCurrentPt(GetData):
    
    #: if query is large according to google cloud api
    large_query: Optional[bool] = False
    
    date_col:str = f"smoking_current_entry_date"
    
    survey_codes:List[str] = SMOKING_CURRENT_CODES