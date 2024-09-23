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
    date: str
    upcoming: bool
    season: str
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
