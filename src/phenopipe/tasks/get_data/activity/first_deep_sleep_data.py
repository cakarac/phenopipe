from .sleep_levels_data import SleepLevelsData

class FirstDeepSleepData(SleepLevelsData):
    sql_aggregation = "first"
    sleep_levels = "deep"
