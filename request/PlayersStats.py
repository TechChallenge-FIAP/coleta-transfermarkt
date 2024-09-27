from dataclasses import dataclass
from typing import List

from request.utils.RequestSoccer import RequestSoccer
from request.utils.utils import read_json


@dataclass
class PlayersStats:
    payload = {
        "id": {
            "identifier": "PlayersStats",
        }
    }

    def get_player_ids(self) -> None:
        club_players = read_json(
            bucket_name="tech-challenge-3-landing-zone",
            path="ClubsPlayers/ClubsPlayers.json",
        )
        players_lst: List[str] = list()
        for club in club_players:
            for player in club.get("players"):
                players_lst.append(player.get("id"))

        players_lst = list(set(players_lst))
        self.payload["id"]["id"] = players_lst

    def run(self):
        self.get_player_ids()
        id = self.payload.get("id")
        main = RequestSoccer(id=id)
        main.run()
