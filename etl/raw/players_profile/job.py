from dataclasses import dataclass

from etl.utils.utils import DefaultUtils


@dataclass
class RawPlayersProfileJob(DefaultUtils):
    def main(self):
        df_players_profile = self.read_json(
            "tech-challenge-3-landing-zone/PlayersProfile/"
        )

        self.write_parquet(
            df_players_profile.repartition(1),
            "tech-challenge-3-raw/PlayersProfile/",
        )
