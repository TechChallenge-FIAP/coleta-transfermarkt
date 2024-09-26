from dataclasses import dataclass

from etl.utils.utils import DefaultUtils


@dataclass
class PlayersProfileJob(DefaultUtils):
    def main(self):
        df_players_profile = self.read_raw("amostra/landing/PlayersProfile/")

        self.write_csv(df_players_profile.repartition(1), "amostra/raw/ClubsProfile/")
