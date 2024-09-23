from ast import List
from dataclasses import dataclass

from request.utils.RequestSoccer import RequestSoccer
from request.utils.utils import read_json


@dataclass
class ClubsPlayers:
    payload = {
        "id": {
            "identifier": "ClubsPlayers",
        },
        "season": ["2023", "2022", "2021"],
    }

    def get_club_ids(self) -> None:
        competition_clubs = read_json(
            "./amostra/landing/CompetitionClubs/CompetitionClubs.json"
        )
        clubs_lst: List[str] = list()
        for comp in competition_clubs:
            for clubs in comp.get("clubs"):
                clubs_lst.append(clubs.get("id"))

        clubs_lst = list(set(clubs_lst))
        self.payload["id"]["id"] = clubs_lst

    def run(self):
        self.get_club_ids()
        id = self.payload.get("id")
        main = RequestSoccer(id=id)
        main.run()
