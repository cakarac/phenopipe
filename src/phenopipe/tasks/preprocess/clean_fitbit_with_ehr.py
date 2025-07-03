from phenopipe.tasks.preprocess import CleanFitbit
import polars as pl

class CleanFitbitWithEhr(CleanFitbit):
    def complete(self):
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
        if not self.validate_inputs():
            raise ValueError("invalid inputs to clean fitbit data")
        super().complete()
        
        print("Removing records with no medical encounters")
        lme = self.inputs["last_medical_encounters"]
        self.output = self.output.join(lme.select("person_id"), on=["person_id"])
        self.summarize_n(self.output)
    
    def validate_inputs(self):
        '''Validate the input dataframes'''
        super().validate_inputs()
        if "last_medical_encounters" not in list(self.inputs.keys()):
            raise ValueError("last medical encounters is needed to complete this task")    
        elif not (isinstance(self.inputs["last_medical_encounters"], pl.DataFrame)):
            raise ValueError("last medical encounter needs to be polars dataframes")
        elif not (
            ("person_id" in self.inputs["last_medical_encounters"])
        ):
            raise ValueError("some of the needed columns to finish this task is missing!")
        else:
            return True