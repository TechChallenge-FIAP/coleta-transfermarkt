from dataclasses import dataclass

from etl.utils.utils import DefaultUtils


@dataclass
class RawClubsPlayersJob(DefaultUtils):
    def main(self):
        df_clubs_players = self.read_json("tech-challenge-3-landing-zone/ClubsPlayers/")

        self.write_parquet(
            df_clubs_players.repartition(1),
            "tech-challenge-3-raw/ClubsPlayers/",
        )
