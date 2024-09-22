from typing import List, Optional

from pydantic import BaseModel

from models.util import SoccerInfo


class SquadInfo(BaseModel):
    size: str
    averageAge: str
    foreigners: str
    nationalTeamPlayers: str


class LeagueInfo(BaseModel):
    id: str
    name: str
    countryID: str
    countryName: str
    tier: str


class ClubsProfile(BaseModel):
    id: str
    url: str
    name: str
    officialName: str
    image: str
    addressLine1: str
    addressLine2: str
    addressLine3: str
    tel: str
    fax: str
    website: str
    foundedOn: str
    members: Optional[str] = None
    membersDate: Optional[str] = None
    otherSports: Optional[List[str]] = None
    colors: List[str]
    stadiumName: str
    stadiumSeats: str
    currentTransferRecord: str
    curentMarketValue: Optional[str] = None
    squad: SquadInfo
    league: LeagueInfo
    historicalCrests: Optional[List[str]] = None
    updatedAt: str


clubs_profile = SoccerInfo(
    id="ClubsProfile",
    schema=ClubsProfile,
    endpoint="clubs/{id}/profile",
)
