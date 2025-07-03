from typing import Optional
import polars as pl
from phenopipe.tasks.get_data.get_data import GetData

class GetDemographics(GetData):

    #: name of the data query to run
    query_name: Optional[str] = "demographics"

    #: if query is large according to google cloud api
    large_query: Optional[bool] = False

    def complete(self):
        '''
        Query demographics data and update self.output with resulting dataframe
        '''
        local = self.cacher.get_local(self.query_name, self.location, self.large_query)
        if self.cache and self.cacher.get_cache(self.query_name, local, self.lazy):
            self.output = self.cacher.cached_output
        else:
            self.output = self.run_demographics_query(local=local)
        if isinstance(self.output.collect_schema().get("date_of_birth"), pl.String):
            self.output = self.output.with_columns(pl.col("date_of_birth").str.to_datetime("%Y-%m-%d %H:%M:%S %Z").dt.date())    
    def run_demographics_query(self, local):
        '''runs demographics query'''
        query = '''
                    SELECT
                        person.person_id,
                        person.birth_datetime as date_of_birth,
                        p_race_concept.concept_name as race,
                        p_ethnicity_concept.concept_name as ethnicity,
                        p_sex_at_birth_concept.concept_name as sex
                    FROM
                        `person` person
                    LEFT JOIN
                        `concept` p_race_concept
                            ON person.race_concept_id = p_race_concept.concept_id
                    LEFT JOIN
                        `concept` p_ethnicity_concept
                            ON person.ethnicity_concept_id = p_ethnicity_concept.concept_id
                    LEFT JOIN
                        `concept` p_sex_at_birth_concept
                            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id
                '''
        return self.query_func(query, bucket_id=self.bucket_id, location = local)
