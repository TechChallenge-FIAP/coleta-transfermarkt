from etl.raw.clubs_profile.job import ClubsProfileJob
from request.ClubsPlayers import ClubsPlayers
from request.ClubsProfile import ClubsProfile
from request.CompetitionClubs import CompetitionClubs
from request.PlayersMarketValue import PlayersMarketValue
from request.PlayersTransfers import PlayersTransfers


def lambda_handler(event=None, context=None):
    request()
    etl()


def request():
    CompetitionClubs().run()
    ClubsProfile().run()
    ClubsPlayers().run()
    PlayersMarketValue().run()
    PlayersTransfers().run()


def etl():
    ClubsProfileJob().main()


if __name__ == "__main__":
    lambda_handler()
