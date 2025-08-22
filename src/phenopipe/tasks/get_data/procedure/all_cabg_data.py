from typing import List, Dict
from phenopipe.tasks.get_data.procedure import ProcedureData
from phenopipe.vocab.concepts.procedure import CABG_CODES


class AllCabgData(ProcedureData):
    aggregate: str = "all"
    date_col: str = "all_cabg_entry_date"
    lab_terms: List[str] = CABG_CODES
    state: Dict[str, List[str]] = {"aou": "parsed", "std_omop": "untested"}
