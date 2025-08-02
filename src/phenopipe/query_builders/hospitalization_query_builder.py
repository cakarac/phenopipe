from typing import List, Optional

def hospitalization_query(source_values:Optional[List[str]]=None):
    
    if source_values is None:
        source_values_str = "1=1"
    else:
        source_values_str = " OR ".join([f"c.condition_source_value LIKE '{sv}'" for sv in source_values])

    query = f'''
            SELECT  co.person_id,
                    vo.visit_start_date AS hospitalization_entry_date,
                    co.condition_source_value AS hospitalization_icd_code
            FROM
                `condition_occurrence` co
                LEFT JOIN concept c ON (co.condition_source_concept_id = c.concept_id)
                LEFT JOIN `visit_occurrence` vo ON (co.visit_occurrence_id = vo.visit_occurrence_id)
            WHERE
                c.VOCABULARY_ID LIKE 'ICD%' AND
                (
                    (vo.visit_concept_id = 9201 OR vo.visit_concept_id = 9203) 
                    AND
                    (co.condition_type_concept_id = 38000200 OR co.condition_status_concept_id = 4230359)
                ) AND
                ({source_values_str})
            '''
    return query
