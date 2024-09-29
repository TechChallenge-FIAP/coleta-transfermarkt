import logging

from etl.curated.competitions_clubs.job import CuratedCompetitionsClubsJob
from etl.curated.players_injuries.job import CuratedPlayersInjuries
from etl.curated.players_market_value.job import CuratedPlayersMarketValueJob
from etl.curated.players_profile.job import CuratedPlayersProfileJob
from etl.curated.players_stats.job import CuratedPlayersStats
from etl.curated.players_transfers.job import CuratedPlayersTransfersJob
from etl.raw.clubs_profile.job import ClubsProfileJob
from etl.raw.competitions_clubs.job import RawCompetitionsClubsJob
from etl.raw.players_injuries.job import RawPlayersInjuries
from etl.raw.players_market_value.job import RawPlayersMarketValueJob
from etl.raw.players_profile.job import RawPlayersProfileJob
from etl.raw.players_stats.job import RawPlayersStats
from etl.raw.players_transfers.job import RawPlayersTransfersJob
from request.ClubsPlayers import ClubsPlayers
from request.ClubsProfile import ClubsProfile
from request.CompetitionClubs import CompetitionClubs
from request.PlayersInjuries import PlayersInjuries
from request.PlayersMarketValue import PlayersMarketValue
from request.PlayersProfile import PlayersProfile
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

    logging.info("Players Profile")
    PlayersProfile().run()

    logging.info("Players Stats")
    PlayersStats().run()

    logging.info("Players Injuries")
    PlayersInjuries().run()


def etl():
    ClubsProfileJob().main()
    RawCompetitionsClubsJob().main()
    CuratedCompetitionsClubsJob().main()
    RawPlayersProfileJob().main()
    CuratedPlayersProfileJob().main()
    RawPlayersInjuries().main()
    RawPlayersStats().main()
    CuratedPlayersInjuries().main()
    CuratedPlayersStats().main()
    RawPlayersMarketValueJob().main()
    RawPlayersTransfersJob().main()
    CuratedPlayersMarketValueJob().main()
    CuratedPlayersTransfersJob().main()


if __name__ == "__main__":
    lambda_handler()
