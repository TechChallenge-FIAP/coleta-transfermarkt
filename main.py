from request.ClubsPlayers import ClubsPlayers
from request.ClubsProfile import ClubsProfile
from request.CompetitionClubs import CompetitionClubs
from request.PlayersMarketValue import PlayersMarketValue
from request.PlayersTransfers import PlayersTransfers


def lambda_handler(event=None, context=None):
    CompetitionClubs().run()
    ClubsProfile().run()
    PlayersMarketValue().run()
    ClubsPlayers().run()
    PlayersTransfers().run()


if __name__ == "__main__":
    lambda_handler()
