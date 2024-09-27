from dataclasses import dataclass
from typing import List

from request.utils.RequestSoccer import RequestSoccer
from request.utils.utils import read_json


@dataclass
class ClubsProfile:
    payload = {
        "id": {
            "identifier": "ClubsProfile",
        }
    }

    def get_club_ids(self) -> None:
        competition_clubs = read_json(
            bucket_name="tech-challenge-3-landing-zone",
            path="CompetitionClubs/CompetitionClubs.json",
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
