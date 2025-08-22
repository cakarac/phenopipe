from typing import List, Dict
from phenopipe.tasks.get_data.procedure import ProcedureData
from phenopipe.vocab.concepts.procedure import BARIATRIC_SURGERY_CODES


class FirstBariatricSurgeryData(ProcedureData):
    aggregate: str = "first"
    date_col: str = "first_bariatric_surgery_entry_date"
    lab_terms: List[str] = BARIATRIC_SURGERY_CODES
    state: Dict[str, List[str]] = {"aou": "parsed", "std_omop": "untested"}
