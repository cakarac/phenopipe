from typing import List, Optional


def med_query(med_names: Optional[List[str]] = None):
    med_names_str = " OR ".join(
        [f"lower(c.concept_name) LIKE '%{mn}%'" for mn in med_names]
    )

    query = f"""
            SELECT DISTINCT d.person_id,d.drug_exposure_start_date
        FROM
        drug_exposure d
        INNER JOIN
        concept c
        ON (d.drug_concept_id = c.concept_id)
        WHERE ({med_names_str})
            """
    return query
