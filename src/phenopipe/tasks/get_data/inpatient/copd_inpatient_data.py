from typing import List
from .inpatient_data import InpatientData
from phenopipe.vocab.icds.conditions import COPD_INP_CODES


class CopdInpatientData(InpatientData):
    date_col: str = "copd_inpatient_entry_date"
    inp_codes: List[str] = COPD_INP_CODES
