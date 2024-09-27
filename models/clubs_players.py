from typing import List, Optional

from pydantic import BaseModel

from models.util import SoccerInfo


class PlayerInfo(BaseModel):
    id: str
    name: Optional[str] = None
    position: Optional[str] = None
    dateOfBirth: Optional[str] = None
    age: Optional[str] = None
    nationality: Optional[List[str]] = None
    height: Optional[str] = None
    foot: Optional[str] = None
    joinedOn: Optional[str] = None
    joined: Optional[str] = None
    signedFrom: Optional[str] = None
    contract: Optional[str] = None
    marketValue: Optional[str] = None
    status: Optional[str] = None


class ClubsPlayers(BaseModel):
    id: str
    seasonID: Optional[str] = None
    players: Optional[List[PlayerInfo]] = None
    updatedAt: str


clubs_players = SoccerInfo(
    id="ClubsPlayers",
    schema=ClubsPlayers,
    endpoint="clubs/{id}/players?season_id={season}",
)
