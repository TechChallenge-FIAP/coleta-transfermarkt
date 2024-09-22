from typing import List, Optional

from pydantic import BaseModel

from models.util import SoccerInfo


class MarketValue(BaseModel):
    age: str
    date: str
    clubName: str
    value: Optional[str] = None
    clubID: str


class PlayersMarketValue(BaseModel):
    id: str
    marketValue: Optional[str] = None
    marketValueHistory: Optional[List[MarketValue]] = None
    updatedAt: str


players_market_value = SoccerInfo(
    id="PlayersMarketValue",
    schema=PlayersMarketValue,
    endpoint="players/{id}/market_value",
)
