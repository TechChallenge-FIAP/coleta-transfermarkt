from typing import Dict, List

from models.clubs_profile import clubs_profile
from models.competitions_clubs import competitionsClubs
from models.util import SoccerInfo

PYDANTIC_MODELS: List[SoccerInfo] = [competitionsClubs, clubs_profile]

ID = {model.id: model for model in PYDANTIC_MODELS}


def identify_class(id: Dict[str, str]) -> SoccerInfo:
    if id["identifier"] in ID:
        return ID[id["identifier"]]
    else:
        raise Exception("Error")
