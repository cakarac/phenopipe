from phenopipe.tasks.task import Task
import polars as pl

class CleanFitbit(Task):
    wear_time_min: int =10 #: minimum wear time for subsetting
    steps_min: int = 100 #: minimum steps for subsetting
    steps_max: int = 45_000 #: maximum steps for subsetting
    age_min: int = 18 #: minimum age for subsetting
        
    min_inputs_schemas: dict[str, dict] = {"fitbit":{"person_id":int,
                                    "steps":int},
                          "demographics":{"person_id":int,
                                          "date_of_birth":datetime.date},
                          "wear_time":{"person_id":int,
                                       "wear_time":int}}
    def complete(self):
        '''
        Clean fitbit daily activity summary dataframe with pre-determined thresholds
        Inputs:
        -------
         - fitbit: daily activity dataframe with columns (at least) person_id, date, steps
         - demographics: demographics dataframe with columns (at least) person_id, date_of_birth
         - wear_time: wear_time dataframe with columns (at least) person_id, date, wear_time
        
        Output:
        -------
         - cleaned daily activity summary dataframe
        '''
        if not self.validate_min_inputs_schemas():
            raise ValueError("invalid inputs to clean fitbit data")
        fitbit = self.inputs["fitbit"]
        demo = self.inputs["demographics"]
        wear_time = self.inputs["wear_time"]

        df = (fitbit
              .join(demo.select("person_id", "date_of_birth"), on= "person_id")
              .join(wear_time.select("person_id", "date", "wear_time"), on=["person_id", "date"]))
        print("Initial Cohort")
        self.summarize_n(df)
        
        print("\nRemoving days where wear time < 10 hrs.")
        df = df.filter(pl.col("wear_time") >= self.wear_time_min)
        self.summarize_n(df)

        print("\nRemoving days where step count < 100.")
        df = df.filter(pl.col("steps") >= self.steps_min)
        self.summarize_n(df)
        
        print("\nRemoving days where step counts > 45,000.")
        df = df.filter(pl.col("steps") <= self.steps_max)
        self.summarize_n(df)
        
        print("\nRemoving days where age < 18.")
        df = df.filter((pl.col("date") - pl.col("date_of_birth")).dt.total_days()/365.25 >= self.age_min)
        self.summarize_n(df)
        
        self.output = df

    def summarize_n(self, df):
        '''print cohort N and recorded days'''
        if isinstance(df, pl.LazyFrame):
            print("Skipping summarize N since inputs are Lazy.")
        else:
            print(f'N: {df.n_unique(subset="person_id")}')
            print(f'Days: {df.shape[0]}')