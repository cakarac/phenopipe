import datetime
from phenopipe.tasks.preprocess.combine_fitbit_and_sleep import CombineFitbitAndSleep


class CombineFitbitSleepWDemo(CombineFitbitAndSleep):
    min_inputs_schemas: dict[str, dict] = {
        "fitbit": {
            "person_id": int,
            "date": datetime.date,
        },
        "sleep": {
            "person_id": int,
            "date": datetime.date,
            "is_main_sleep": bool,
        },
        "demographics": {
            "person_id": int,
        },
    }

    def _complete(self):
        """
        Combine fitbit, sleep, and demographics outputs.
        Inputs:
        -------
         - fitbit: fitbit dataframe with columns including person_id and date
         - sleep: sleep dataframe with columns including person_id, date, and is_main_sleep
         - demographics: demographics dataframe with columns including person_id

        Output:
        -------
         - if summarize=True: person-level summary with tracking start/end dates, tracking span,
           and demographic columns
         - if summarize=False: joined daily-level fitbit and sleep data with demographics
        """
        demographics = self.inputs["demographics"]
        super()._complete()
        self.output = self.output.join(demographics, on="person_id")
