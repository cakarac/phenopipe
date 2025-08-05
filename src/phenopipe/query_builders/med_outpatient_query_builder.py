from typing import List, Optional
from phenopipe.vocab.concepts.visits import OUTPATIENT


def med_outpatient_query(med_names: Optional[List[str]] = None):
    med_names_str = " OR ".join(
        [f"lower(c.concept_name) LIKE '{mn}'" for mn in med_names]
    )
    visit_types = ", ".join(OUTPATIENT)

    query = f"""
            SELECT DISTINCT d.person_id, d.drug_exposure_start_date, c2.concept_name AS record_source
        FROM
        drug_exposure d
        INNER JOIN
        concept c
        ON (d.drug_concept_id = c.concept_id)
        INNER JOIN
        visit_occurrence v 
        ON (d.visit_occurrence_id = v.visit_occurrence_id)
        INNER JOIN
        concept c2
        ON (d.drug_type_concept_id = c2.concept_id)
        WHERE
        ({med_names_str}) AND
        v.visit_concept_id IN {visit_types}
            """
    return query
