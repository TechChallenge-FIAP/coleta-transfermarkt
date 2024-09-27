from typing import List, Optional

from pydantic import BaseModel

from models.util import SoccerInfo


class StatsInfo(BaseModel):
    competitionID: Optional[str] = None
    clubID: Optional[str] = None
    seasonID: Optional[str] = None
    competitionName: Optional[str] = None
    appearances: Optional[str] = None
    goals: Optional[str] = None
    assists: Optional[str] = None
    yellowCards: Optional[str] = None
    minutesPlayed: Optional[str] = None


class PlayersStats(BaseModel):
    id: str
    stats: Optional[List[StatsInfo]] = None
    updatedAt: str


players_stats = SoccerInfo(
    id="PlayersStats",
    schema=PlayersStats,
    endpoint="players/{id}/stats",
)
