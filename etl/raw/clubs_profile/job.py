from dataclasses import dataclass

from etl.utils.utils import DefaultUtils


@dataclass
class ClubsProfileJob(DefaultUtils):
    def main(self):
        df_clubs_profile = self.read_raw("amostra/landing/ClubsProfile/")

        self.write_csv(df_clubs_profile.repartition(1), "amostra/raw/ClubsProfile/")
