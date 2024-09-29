import logging

from etl.curated.clubs_players.job import CuratedClubsPlayersJob
from etl.curated.clubs_profile.job import CuratedClubsProfileJob
from etl.curated.competitions_clubs.job import CuratedCompetitionsClubsJob
from etl.curated.players_injuries.job import CuratedPlayersInjuries
from etl.curated.players_market_value.job import CuratedPlayersMarketValueJob
from etl.curated.players_profile.job import CuratedPlayersProfileJob
from etl.curated.players_stats.job import CuratedPlayersStats
from etl.curated.players_transfers.job import CuratedPlayersTransfersJob
from etl.raw.clubs_players.job import RawClubsPlayersJob
from etl.raw.clubs_profile.job import RawClubsProfileJob
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

logger = logging.getLogger(__name__)


def lambda_handler(event=None, context=None):
    logging.basicConfig(level=logging.INFO)
    request()
    etl()


def request():
    logger.info("---------------- Requesting data ----------------")

    logger.info("Competition Clubs")
    CompetitionClubs().run()

    logger.info("Clubs Profile")
    ClubsProfile().run()

    logger.info("Clubs Players")
    ClubsPlayers().run()

    logger.info("Players Market Value")
    PlayersMarketValue().run()

    logger.info("Players Transfers")
    PlayersTransfers().run()

    logger.info("Players Profile")
    PlayersProfile().run()

    logger.info("Players Stats")
    PlayersStats().run()

    logger.info("Players Injuries")
    PlayersInjuries().run()

    logger.info("---------------- End requesting data ----------------")


def etl():
    logger.info("---------------- ETL ----------------")

    logger.info("Clubs Profile")
    RawClubsProfileJob().main()
    CuratedClubsProfileJob().main()

    logger.info("Competition Clubs")
    RawCompetitionsClubsJob().main()
    CuratedCompetitionsClubsJob().main()

    logger.info("Players Profile")
    RawPlayersProfileJob().main()
    CuratedPlayersProfileJob().main()

    logger.info("Players Injuries")
    RawPlayersInjuries().main()
    CuratedPlayersInjuries().main()

    logger.info("Players Stats")
    RawPlayersStats().main()
    CuratedPlayersStats().main()

    logger.info("Players Market Value")
    RawPlayersMarketValueJob().main()
    CuratedPlayersMarketValueJob().main()

    logger.info("Players Transfers")
    RawPlayersTransfersJob().main()
    CuratedPlayersTransfersJob().main()

    logger.info("Clubs Players")
    RawClubsPlayersJob().main()
    CuratedClubsPlayersJob().main()

    logger.info("---------------- ETL END ----------------")


if __name__ == "__main__":
    lambda_handler()
