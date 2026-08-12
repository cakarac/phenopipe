from phenopipe.tasks.preprocess.combine_pts import CombinePts
from phenopipe.tasks.get_data.condition import FirstMiData, FirstStrokeData, FirstPadData
from phenopipe.tasks.get_data.phenotype.cad_pt import CadPt
from phenopipe.tasks.get_data.phenotype.heart_failure_pt import HeartFailurePt


class CvePt(CombinePts):
    """
    Cardiovascular Events (CVE) phenotype: combination of MI, CAD, Stroke, Heart Failure, and PAD.
    Takes the earliest event date per person across all five conditions.
    """
    date_col: str = "cve_entry_date"
    input_tasks: dict = {
        "mi": FirstMiData(),
        "cad": CadPt(),
        "stroke": FirstStrokeData(),
        "heart_failure": HeartFailurePt(),
        "pad": FirstPadData(),
    }

