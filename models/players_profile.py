from typing import List, Optional

from pydantic import BaseModel

from models.util import SoccerInfo


class PlaceOfBirth(BaseModel):
    city: Optional[str] = None
    country: Optional[str] = None


class Position(BaseModel):
    main: Optional[str] = None
    other: Optional[List] = None


class Club(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    joined: Optional[str] = None
    contractExpires: Optional[str] = None
    contractOption: Optional[str] = None


class Agent(BaseModel):
    name: Optional[str] = None


class PlayersProfile(BaseModel):
    id: str
    url: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    fullName: Optional[str] = None
    imageURL: Optional[str] = None
    dateOfBirth: Optional[str] = None
    placeOfBirth: Optional[PlaceOfBirth] = None
    age: Optional[str] = None
    height: Optional[str] = None
    citizenship: Optional[List] = None
    isRetired: Optional[bool] = None
    position: Optional[Position] = None
    foot: Optional[str] = None
    shirtNumber: Optional[str] = None
    club: Optional[Club] = None
    marketValue: Optional[str] = None
    agent: Optional[Agent] = None
    outfitter: Optional[str] = None
    socialMedia: Optional[List] = None
    updatedAt: Optional[str] = None


players_profile = SoccerInfo(
    id="PlayersProfile",
    schema=PlayersProfile,
    endpoint="players/{id}/profile",
)
