import os
from subprocess import CalledProcessError
from phenopipe.bucket import ls_bucket, read_csv_from_bucket
from phenopipe.tasks.get_data.get_data import GetData

class GetFitbit(GetData):
    def complete(self):
        '''
        Query fitbit summary and update self.output with resulting dataframe
        '''
        if self.cache:
            try:
                ls_bucket(f"{self.location}/fitbit/fitbit_*.csv", return_list=True)
                self.output  = read_csv_from_bucket(f"{self.location}/fitbit/*", lazy=self.lazy)
                print("Cached fitbit data")
            except CalledProcessError as e:
                print("No cache found in given location!")
                self.run_fitbit_query()
        else:
            self.run_fitbit_query()
    def run_fitbit_query(self):
        query = '''
                SELECT
                    activity_summary.person_id,
                    activity_summary.date,
                    activity_summary.steps,
                    activity_summary.fairly_active_minutes,
                    activity_summary.lightly_active_minutes,
                    activity_summary.sedentary_minutes,
                    activity_summary.very_active_minutes
                FROM
                    `activity_summary` activity_summary
                '''
        self.output = self.query_func(query, bucket_id=self.bucket_id, location = f"{self.location}/fitbit/fitbit_*.csv")
