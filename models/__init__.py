from typing import Dict, List

from models.clubs_players import clubs_players
from models.clubs_profile import clubs_profile
from models.competitions_clubs import competitionsClubs
from models.players_injuries import players_injuries
from models.players_market_value import players_market_value
from models.players_profile import players_profile
from models.players_stats import players_stats
from models.players_transfers import players_transfers
from models.util import SoccerInfo

PYDANTIC_MODELS: List[SoccerInfo] = [
    competitionsClubs,
    clubs_profile,
    players_market_value,
    clubs_players,
    players_transfers,
    players_stats,
    players_injuries,
    players_profile,
]

ID = {model.id: model for model in PYDANTIC_MODELS}


def identify_class(id: Dict[str, str]) -> SoccerInfo:
    if id["identifier"] in ID:
        return ID[id["identifier"]]
    else:
        raise Exception("Error")
