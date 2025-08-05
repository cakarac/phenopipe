from typing import List, Optional


def snomed_query(snomed_terms: Optional[List[int]] = None):
    if snomed_terms is None:
        snomed_terms_str = "1=1"
    else:
        snomed_terms_str = " OR ".join(
            [f"co.CONDITION_SOURCE_VALUE LIKE  '{sv}'" for sv in snomed_terms]
        )

    query = f"""
            SELECT DISTINCT co.person_id, co.condition_start_date,co.condition_source_value
            FROM
                condition_occurrence co
                INNER JOIN
                concept c
                ON (co.condition_source_concept_id = c.concept_id)
            WHERE
                c.VOCABULARY_ID LIKE 'SNOMED' AND
                ({snomed_terms_str})
        """
    return query
