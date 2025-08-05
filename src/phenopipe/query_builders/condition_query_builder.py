from typing import List, Optional


def condition_query(
    concept_codes: Optional[List[int]] = None, source_values: Optional[List[str]] = None
):
    if concept_codes is None and source_values is None:
        raise ValueError("Both concept codes and source values cannot be omitted.")
    if concept_codes is None:
        codes_str = "1<>1"
    else:
        codes_str = "c.condition_concept_id IN (" + ", ".join(concept_codes) + ")"

    if source_values is None:
        source_values_str = "1<>1"
    else:
        source_values_str = " OR ".join(
            [f"c.condition_source_value LIKE '{sv}'" for sv in source_values]
        )

    query = f"""
            SELECT person_id, condition_start_date, condition_source_value, condition_concept_id
            FROM `condition_occurrence` c WHERE ({source_values_str}) OR {codes_str}
            """
    return query
