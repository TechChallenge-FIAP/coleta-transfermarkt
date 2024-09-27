from typing import List, Optional

from pydantic import BaseModel, Field

from models.util import SoccerInfo


class Injury(BaseModel):
    season: Optional[str] = None
    injury: Optional[str] = None
    injuryFrom: Optional[str] = Field(None, alias="from")
    until: Optional[str] = None
    days: Optional[str] = None
    gamesMissed: Optional[str] = None
    gamesMissedClubs: Optional[List[str]] = None


class PlayersInjuries(BaseModel):
    id: str
    pageNumber: int
    lastPageNumber: int
    injuries: Optional[List[Injury]] = None
    updatedAt: str


players_injuries = SoccerInfo(
    id="PlayersInjuries",
    schema=PlayersInjuries,
    endpoint="players/{id}/injuries?page_number={page_number}",
)
