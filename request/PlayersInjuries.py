from dataclasses import dataclass
from typing import List

from request.utils.RequestSoccer import RequestSoccer
from request.utils.utils import read_json


@dataclass
class PlayersInjuries:
    payload = {
        "id": {
            "identifier": "PlayersInjuries",
        },
        "page_number": 1,
    }

    def get_player_ids(self) -> None:
        club_players = read_json("./amostra/landing/ClubsPlayers/ClubsPlayers.json")
        players_lst: List[str] = list()
        for club in club_players:
            for player in club.get("players"):
                players_lst.append(player.get("id"))

        players_lst = list(set(players_lst))
        self.payload["id"]["id"] = players_lst

    def fetch_all_pages(self, id: dict) -> None:
        last_page_reached = False
        current_page = 1

        while not last_page_reached:
            self.payload["page_number"] = current_page
            main = RequestSoccer(id=id, page_number=current_page)
            response = main.run()

            if response.get("lastPageNumber") == current_page:
                last_page_reached = True
            else:
                current_page += 1

    def run(self):
        self.get_player_ids()
        id = self.payload.get("id")
        self.fetch_all_pages(id=id)
