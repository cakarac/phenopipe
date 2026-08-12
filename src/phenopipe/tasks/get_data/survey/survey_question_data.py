import polars as pl
from typing import Dict, Optional
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.query_builders import survey_questions_query


class SurveyQuestionData(GetData):
    """Retrieve survey responses filtered by survey name and question prefix."""

    survey: str  # exact match against survey name
    question_prefix: str  # matched with LIKE '<prefix>%' against question text
    answer_map: Optional[Dict[str, str]] = None  # optional mapping to recode survey responses
    binary_answer: Optional[str] = None  # regex pattern; if set, response column is replaced with boolean match result
    compute_score: bool = False  # if True, sums binary answer columns into a score (requires format="wide" and binary_answer)
    format: Optional[str] = None  # output format, use "wide" to pivot questions to columns

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
        if self.answer_map:
            self.output = self.output.replace(self.answer_map)
        if self.binary_answer:
            self.output = self.output.with_columns(
                pl.col(f"{self.val_col}_survey_response").str.contains(self.binary_answer).cast(pl.Int8)
            )

        if self.format == "wide":
            self.output = self.output.with_columns(
                pl.col(f"{self.val_col}_survey_question").str.replace_all(r"[^a-zA-Z0-9]+", "")
            )
            self.output = self.output.pivot(
                index=[self.person_col, self.date_col],
                values=f"{self.val_col}_survey_response"
            )
            if self.compute_score and self.binary_answer:
                question_cols = [c for c in self.output.columns if c not in [self.person_col, self.date_col]]
                self.output = self.output.with_columns(
                    pl.sum_horizontal([pl.col(c).cast(pl.Int8) for c in question_cols]).alias(f"{self.val_col}_score")
                )
