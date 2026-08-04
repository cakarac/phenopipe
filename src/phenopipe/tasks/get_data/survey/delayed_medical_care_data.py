from typing import Dict
from .survey_question_data import SurveyQuestionData


class DelayedMedicalCareData(SurveyQuestionData):
    """Retrieve responses to Delayed Medical Care questions from the Healthcare Access & Utilization survey."""

    date_col: str = "delayed_medical_care_entry_date"
    val_col: str = "delayed_medical_care"
    survey: str = "Healthcare Access & Utilization"
    question_prefix: str = "Delayed Medical Care"
    state: Dict[str, str] = {"aou": "untested", "std_omop": "untested"}
