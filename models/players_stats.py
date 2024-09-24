from typing import List, Optional

from pydantic import BaseModel

from models.util import SoccerInfo


class StatsInfo(BaseModel):
    competitionID: str
    clubID: str
    seasonID: str
    competitionName: str
    appearances: Optional[str] = None
    goals: Optional[str] = None
    assists: Optional[str] = None
    yellowCards: Optional[str] = None
    minutesPlayed: Optional[str] = None


class PlayerStats(BaseModel):
    id: str
    stats: List[StatsInfo]
    updatedAt: str


player_stats = SoccerInfo(
    id="PlayerStats",
    schema=PlayerStats,
    endpoint="players/{id}/stats",
)
