from typing import Dict
from .survey_question_data import SurveyQuestionData


class CantAffordCareData(SurveyQuestionData):
    """Retrieve responses to Can't Afford Care questions from the Healthcare Access & Utilization survey."""

    date_col: str = "cant_afford_care_entry_date"
    val_col: str = "cant_afford_care"
    survey: str = "Healthcare Access & Utilization"
    question_prefix: str = "Can"
    state: Dict[str, list] = {"aou": "untested", "std_omop": "untested"}
