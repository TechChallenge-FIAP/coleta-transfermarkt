from typing import List, Optional

from pydantic import BaseModel

from models.util import SoccerInfo


class ClubsList(BaseModel):
    id: str
    name: str


class CompetitionClubs(BaseModel):
    id: str
    name: str
    seasonID: Optional[str]
    clubs: List[ClubsList]
    updatedAt: str


competitionsClubs = SoccerInfo(
    id="CompetitionClubs",
    schema=CompetitionClubs,
    endpoint="competitions/{id}/clubs?season_id={season}",
)
