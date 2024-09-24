from pydantic import BaseModel

from models.util import SoccerInfo


class PlayerInjuries(BaseModel):
    id: str
    pageNumber: int
    lastPageNumber: int
    updatedAt: str


player_injuries = SoccerInfo(
    id="PlayerInjuries",
    schema=PlayerInjuries,
    endpoint="players/{id}/injuries?page_number={page_number}",
)
