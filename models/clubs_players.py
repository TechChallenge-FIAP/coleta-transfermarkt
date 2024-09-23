from typing import List, Optional

from pydantic import BaseModel

from models.util import SoccerInfo


class PlayerInfo(BaseModel):
    id: str
    name: str
    position: str
    dateOfBirth: str
    age: str
    nationality: List[str]
    height: Optional[str] = None
    foot: Optional[str] = None
    joinedOn: str
    joined: Optional[str] = None
    signedFrom: Optional[str] = None
    contract: Optional[str] = None
    marketValue: Optional[str] = None
    status: Optional[str] = None


class ClubsPlayers(BaseModel):
    id: str
    players: List[PlayerInfo]
    updatedAt: str


clubs_players = SoccerInfo(
    id="ClubsPlayers",
    schema=ClubsPlayers,
    endpoint="clubs/{id}/players",
)
