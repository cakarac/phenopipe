from typing import List
from phenopipe.tasks.get_data.phenotype.base_pts import IcdConditionPt
from phenopipe.vocab.icds.conditions import OBESITY_CODES

class FirstObesityPt(IcdConditionPt):
    aggregate:str = "first"
    date_col:str = "first_obesity_entry_date"
    icd_codes:List[str] = OBESITY_CODES
