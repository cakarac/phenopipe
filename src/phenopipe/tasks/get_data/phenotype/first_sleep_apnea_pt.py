from typing import Optional, Dict, List
from phenopipe.tasks.get_data.phenotype.base_pts import IcdConditionPt
from phenopipe.vocab.icds.conditions import SLEEP_APNEA_ICDS


class FirstSleepApneaPt(IcdConditionPt):
    '''
    Sleep apnea phenotype using icd condition occurance codes
    '''
    #: if query is large according to google cloud api
    large_query: Optional[bool] = False
    
    aggregate:str = "first"

    date_col:str = "first_sleep_apnea_entry_date"

    icd_codes:Dict[str, List[str]] = SLEEP_APNEA_ICDS
    