import datetime
import polars as pl
from phenopipe.tasks.task import Task, completion

class CleanSleep(Task):
    is_main_sleep: bool = True #: minimum wear time for subsetting
    minutes_min: int = 0 #: minimum steps for subsetting
    minutes_max: int = 1_440 #: maximum steps for subsetting
    age_min: int = 18 #: minimum age for subsetting
        
    min_inputs_schemas: dict[str, dict] = {"sleep":{"person_id":int,
                                                    "date":datetime.date,
                                                    "minute_asleep":int,
                                                    "is_main_sleep":bool},
                          "demographics":{"person_id":int,
                                          "date_of_birth":datetime.date}}
    @completion
    def complete(self):
        '''
        Clean daily sleep metrics summary dataframe with pre-determined thresholds
        Inputs:
        -------
         - sleep: daily sleep metrics dataframe with columns (at least) person_id, date, minute_asleep, is_main_sleep
         - demographics: demographics dataframe with columns (at least) person_id, date_of_birth
        
        Output:
        -------
         - cleaned daily sleep activity dataframe
        '''
        sleep = self.inputs["sleep"]
        demo = self.inputs["demographics"]
        
        df = (sleep
              .join(demo.select("person_id", "date_of_birth"), on= "person_id"))
        
        print("Initial Cohort")
        self.summarize_n(df)
        
        print(f"\nRemoving days where minutes asleep < {self.minutes_min}.")
        df = df.filter(pl.col("minute_asleep") >= self.minutes_min)
        self.summarize_n(df)
        
        print(f"\nRemoving days where minutes asleep > {self.minutes_max}.")
        df = df.filter(pl.col("minute_asleep") <= self.minutes_max)
        self.summarize_n(df)
        
        print(f"\nRemoving days where age < {self.age_min}.")
        df = df.filter((pl.col("date") - pl.col("date_of_birth")).dt.total_days()/365.25 >= self.age_min)
        self.summarize_n(df)
        
        if self.is_main_sleep:
            print("\nNon main sleep records are being removed.")
            df = df.filter(pl.col("is_main_sleep"))
        else:
            print("\nNon main sleep records are NOT filtered.")
        
        print("\nRemoving subjects > 30% days < 4 hours sleep")
        df = df.join(
                df.select("person_id", "minute_asleep")
                    .with_columns(pl.col("minute_asleep")< 4*60)
                    .group_by("person_id").mean(), 
            on="person_id").filter(pl.col("minute_asleep_right") <= 0.3).drop("minute_asleep_right")
        self.output = df

    def summarize_n(self, df):
        '''print cohort N and recorded days'''
        if isinstance(df, pl.LazyFrame):
            print("Skipping summarize N since inputs are Lazy.")
        else:
            print(f'N: {df.n_unique(subset="person_id")}')
            print(f'Days: {df.shape[0]}')