from .icd_clause_builder import icd_clause

def icd_procedure_query(icd_codes:dict[str, list]):
    icd9, icd10  = icd_clause(icd_codes=icd_codes)
    query = f'''
            SELECT DISTINCT po.person_id,
                  po.procedure_date AS icd_procedure_date,
                  c.concept_code AS icd_procedure_code
            FROM
                procedure_occurrence po
                INNER JOIN
                concept c
                ON (po.procedure_source_concept_id = c.concept_id)
            WHERE
                ({icd9} OR {icd10})
            '''
    return query
