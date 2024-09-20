from request.RequestSoccer import RequestSoccer


def lambda_handler(event, context=None):
    id = event.get("id")
    season = event.get("season")
    main = RequestSoccer(id=id, season=season)
    main.run()


if __name__ == "__main__":
    payload = {
        "id": {"identifier": "CompetitionClubs", "id": ["BRA1", "BRA2"]},
        "season": ["2023", "2022", "2021"],
    }

    lambda_handler(payload)
