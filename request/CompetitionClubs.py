from dataclasses import dataclass

from request.utils.RequestSoccer import RequestSoccer


@dataclass
class CompetitionClubs:
    payload = {
        "id": {
            "identifier": "CompetitionClubs",
            "id": [
                "BRA1",
                "BRA2",
                "CB20",
                "FR1",
                "ES1",
                "GB1",
                "L1",
                "IT1",
                "NL1",
                "PO1",
            ],
        },
        "season": ["2023", "2022", "2021"],
    }

    def run(self):
        id = self.payload.get("id")
        season = self.payload.get("season")
        main = RequestSoccer(id=id, season=season)
        main.run()
