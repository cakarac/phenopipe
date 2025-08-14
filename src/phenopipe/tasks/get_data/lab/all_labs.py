from typing import List, Dict
from .lab_data import LabData


class AllLabData(LabData):
    date_col: str = "lab_entry_date"
    lab_terms: Dict[str, List[str]] = {"concept_codes": None, "concept_names": None}
    val_col: str = "lab_value"
    required_cols: List[str] = ["lab_value"]
