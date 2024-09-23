from typing import List, Optional

from pydantic import BaseModel

from models.util import SoccerInfo


class SquadInfo(BaseModel):
    size: Optional[str] = None
    averageAge: Optional[str] = None
    foreigners: Optional[str] = None
    nationalTeamPlayers: Optional[str] = None


class LeagueInfo(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    countryID: Optional[str] = None
    countryName: Optional[str] = None
    tier: Optional[str] = None


class ClubsProfile(BaseModel):
    id: str
    url: Optional[str] = None
    name: Optional[str] = None
    officialName: Optional[str] = None
    image: Optional[str] = None
    addressLine1: Optional[str] = None
    addressLine2: Optional[str] = None
    addressLine3: Optional[str] = None
    tel: Optional[str] = None
    fax: Optional[str] = None
    website: Optional[str] = None
    foundedOn: Optional[str] = None
    members: Optional[str] = None
    membersDate: Optional[str] = None
    otherSports: Optional[List[str]] = None
    colors: Optional[List[str]] = None
    stadiumName: Optional[str] = None
    stadiumSeats: Optional[str] = None
    currentTransferRecord: Optional[str] = None
    curentMarketValue: Optional[str] = None
    squad: Optional[SquadInfo] = None
    league: Optional[LeagueInfo] = None
    historicalCrests: Optional[List[str]] = None
    updatedAt: Optional[str] = None


clubs_profile = SoccerInfo(
    id="ClubsProfile",
    schema=ClubsProfile,
    endpoint="clubs/{id}/profile",
)
