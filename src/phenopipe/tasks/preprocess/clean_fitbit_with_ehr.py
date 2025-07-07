import datetime
import polars as pl
from phenopipe.tasks.preprocess import CleanFitbit

class CleanFitbitWithEhr(CleanFitbit):
    def complete(self):
        min_inputs_schemas: dict[str, dict] = {
                            "fitbit":{"person_id":int,
                                      "steps":int},
                            "demographics":{"person_id":int,
                                            "date_of_birth":datetime.date},
                            "wear_time":{"person_id":int,
                                         "wear_time":int},
                            "last_medical_encounter":{"person_id":int}}
        
        '''
        Clean fitbit daily activity summary dataframe with pre-determined thresholds and subsets with available last medical encounter cohort
        Inputs:
        -------
         - fitbit: daily activity dataframe with columns (at least) person_id, date, steps
         - demographics: demographics dataframe with columns (at least) person_id, date_of_birth
         - wear_time: wear_time dataframe with columns (at least) person_id, date, wear_time
         - medical_encounters_last: last medical encounters dataframe with columns (at least) person_id
        Output:
        -------
         - cleaned daily activity summary dataframe subsetted with cohort with medical encounters
        '''
        if not self.validate_min_inputs_schemas():
            raise ValueError("invalid inputs to clean fitbit data")
        super().complete()
        
        print("\nRemoving records with no medical encounters")
        lme = self.inputs["last_medical_encounters"]
        self.output = self.output.join(lme.select("person_id"), on=["person_id"])
        self.summarize_n(self.output)
