from pydantic import BaseModel

from models.util import SoccerInfo


class PlayersInjuries(BaseModel):
    id: str
    pageNumber: int
    lastPageNumber: int
    updatedAt: str


players_injuries = SoccerInfo(
    id="PlayersInjuries",
    schema=PlayersInjuries,
    endpoint="players/{id}/injuries?page_number={page_number}",
)
