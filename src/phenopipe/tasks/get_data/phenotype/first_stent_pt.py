from typing import List
from phenopipe.tasks.get_data.phenotype.base_pts import ProcedurePt
from phenopipe.vocab.concepts.procedure import STENT_CODES

class FirstStentPt(ProcedurePt):
    aggregate:str = "first"
    date_col:str = "first_stent_entry_date"
    procedure_codes:List[str] = STENT_CODES

