from typing import List
from phenopipe.tasks.get_data.phenotype.base_pts import LabsPt
from phenopipe.vocab.labs import ALBUMIN_TERMS
class AllAlbuminPt(LabsPt):
    date_col:str = "all_albumin_entry_date"
    lab_terms:List[str] = ALBUMIN_TERMS
    val_col:str = "all_albumin_value"
    required_cols:List[str] = ["all_albumin_value"]
