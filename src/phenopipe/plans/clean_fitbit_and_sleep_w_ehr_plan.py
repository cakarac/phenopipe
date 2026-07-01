clean_fitbit_and_sleep_w_ehr = {
    "env_vars": {
        "query_conn": {
            "module": "BigQueryConnection"
        }
    },
    "modules": {
        "activity": "phenopipe.tasks.get_data.activity",
        "preprocess": "phenopipe.tasks.preprocess",
    },
    "tasks": {
        "fitbit": {
            "name": "modules.activity.GetFitbit",
        },
        "sleep": {
            "name": "modules.activity.GetSleep",
        },
        "wear_time": {
            "name": "modules.activity.GetWearTime",
        },
        "demographics": {
            "name": "phenopipe.tasks.get_data.person_info.GetDemographics",
        },
        "last_medical_encounter": {
            "name": "phenopipe.tasks.get_data.GetMedicalEncounter",
            "select": "last",
        },
        "clean_fitbit_w_ehr": {
            "name": "modules.preprocess.CleanFitbitWithEhr",
            "inputs": {
                "fitbit": "fitbit",
                "wear_time": "wear_time",
                "demographics": "demographics",
                "last_medical_encounter": "last_medical_encounter",
            },
        },
        "clean_sleep_w_ehr": {
            "name": "modules.preprocess.CleanSleepWithEhr",
            "inputs": {
                "sleep": "sleep",
                "demographics": "demographics",
                "last_medical_encounter": "last_medical_encounter",
            },
        },
    },
}