from request.RequestSoccer import RequestSoccer


def lambda_handler(event, context=None):
    id = event.get("id")
    season = event.get("season")
    main = RequestSoccer(id=id, season=season)
    main.run()


if __name__ == "__main__":
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

    lambda_handler(payload)
