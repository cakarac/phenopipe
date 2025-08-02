from typing import List
from phenopipe.tasks.get_data.phenotype.base_pts import MedicationsPt
from phenopipe.vocab.concepts.medications import STATINS_CODES

class FirstStatinsPt(MedicationsPt):
    aggregate:str = "first"
    date_col:str = "first_statins_entry_date"
    med_codes:List[str] = STATINS_CODES
