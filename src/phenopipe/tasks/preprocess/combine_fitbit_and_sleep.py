import datetime
import polars as pl
from phenopipe.tasks.task import Task


class CombineFitbitAndSleep(Task):
    summarize: bool = False  #: whether to return aggregated person-level output

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
    }

    def _complete(self):
        """
        Combine fitbit and sleep outputs.
        Inputs:
        -------
         - fitbit: fitbit dataframe with columns including person_id and date
         - sleep: sleep dataframe with columns including person_id, date, and is_main_sleep

        Output:
        -------
         - if summarize=True: person-level summary with tracking start/end dates and tracking span
         - if summarize=False: joined daily-level fitbit and sleep data
        """
        fitbit = self.inputs["fitbit"]
        sleep = self.inputs["sleep"]

        joined = fitbit.join(sleep, on=["person_id", "date"])

        if self.summarize:
            self.output = (
                joined.group_by("person_id")
                .agg(
                    pl.exclude("date", "is_main_sleep").mean(),
                    tracking_start_date=pl.col("date").min(),
                    tracking_end_date=pl.col("date").max(),
                    tracking_days_n=pl.col("date").count(),
                )
                .with_columns(
                    tracking_day_range=(
                        pl.col("tracking_end_date") - pl.col("tracking_start_date")
                    ).dt.total_days()
                )
            )
        else:
            self.output = joined