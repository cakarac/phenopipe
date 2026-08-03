
def survey_questions_query(survey:str, question_prefix:str):
    """Build a query to retrieve survey responses filtered by survey name and question prefix.

    Args:
        survey: The name of the survey to filter by (exact match).
        question_prefix: The prefix used to filter questions (matched with LIKE '<prefix>%').

    Returns:
        str: A SQL query string selecting person_id, survey_question, survey_response, and survey_date.
    """
    query = f"""
            SELECT
            survey.person_id,
            survey.question AS survey_question,
            survey.answer AS survey_response,
            CAST(survey.survey_datetime AS DATE) AS survey_date
        FROM
            `ds_survey` survey
        WHERE
            (
                survey.survey = '{survey}'
                AND
                survey.question LIKE '{question_prefix}%'
            )"""
    return query
