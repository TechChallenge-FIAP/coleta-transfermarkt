from typing import List, Optional

from pydantic import BaseModel, Field

from models.util import SoccerInfo


class ClubTransfer(BaseModel):
    clubID: str
    clubName: str


class Transfer(BaseModel):
    id: str
    clubFrom: Optional[ClubTransfer] = Field(None, alias="from")
    clubTo: Optional[ClubTransfer] = Field(None, alias="to")
    date: Optional[str] = None
    upcoming: Optional[bool] = None
    season: Optional[str] = None
    fee: Optional[str] = None


class PlayersTransfers(BaseModel):
    id: str
    transfers: Optional[List[Transfer]] = None
    updatedAt: str


players_transfers = SoccerInfo(
    id="PlayersTransfers",
    schema=PlayersTransfers,
    endpoint="players/{id}/transfers",
)
