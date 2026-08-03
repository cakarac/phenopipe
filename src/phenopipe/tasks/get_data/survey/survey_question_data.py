from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.query_builders import survey_questions_query


class SurveyQuestionData(GetData):
    """Retrieve survey responses filtered by survey name and question prefix."""

    survey: str  # exact match against survey name
    question_prefix: str  # matched with LIKE '<prefix>%' against question text

    def _complete(self):
        """
        Query for survey data filtered by survey name and question prefix.

        Returns:
            None: Sets self.output with the query result.
        """
        survey_query_str = survey_questions_query(self.survey, self.question_prefix)
        self.output = self.env_vars["query_conn"].get_query(
            survey_query_str, self.lazy
        )

    def set_output_dtypes_and_names(self):
        """
        Rename and select output columns, and set the date column dtype.

        Returns:
            None: Modifies self.output in place.
        """
        self.output = self.output.rename(
            {
                "survey_date": self.date_col,
                "survey_question": f"{self.val_col}_survey_question",
                "survey_response": f"{self.val_col}_survey_response",
            }
        ).select(
            "person_id",
            f"{self.val_col}_survey_question",
            f"{self.val_col}_survey_response",
            self.date_col,
        )
        self.set_date_column_dtype()
