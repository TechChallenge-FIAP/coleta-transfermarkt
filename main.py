import logging

from etl.curated.competitions_clubs.job import CuratedCompetitionsClubsJob
from etl.raw.clubs_profile.job import ClubsProfileJob
from etl.raw.competitions_clubs.job import RawCompetitionsClubsJob
from request.ClubsPlayers import ClubsPlayers
from request.ClubsProfile import ClubsProfile
from request.CompetitionClubs import CompetitionClubs
from request.PlayersInjuries import PlayersInjuries
from request.PlayersMarketValue import PlayersMarketValue
from request.PlayersStats import PlayersStats
from request.PlayersTransfers import PlayersTransfers


def lambda_handler(event=None, context=None):
    request()
    etl()


def request():
    logging.info("Requesting data")

    logging.info("Competition Clubs")
    CompetitionClubs().run()

    logging.info("Clubs Profile")
    ClubsProfile().run()

    logging.info("Clubs Players")
    ClubsPlayers().run()

    logging.info("Players Market Value")
    PlayersMarketValue().run()

    logging.info("Players Transfers")
    PlayersTransfers().run()

    logging.info("Players Stats")
    PlayersStats().run()

    logging.info("Players Injuries")
    PlayersInjuries().run()


def etl():
    ClubsProfileJob().main()
    RawCompetitionsClubsJob().main()
    CuratedCompetitionsClubsJob().main()


if __name__ == "__main__":
    lambda_handler()
