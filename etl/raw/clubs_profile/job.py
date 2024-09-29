from dataclasses import dataclass

from etl.utils.utils import DefaultUtils


@dataclass
class RawClubsProfileJob(DefaultUtils):
    def main(self):
        df_clubs_profile = self.read_json("tech-challenge-3-landing-zone/ClubsProfile/")

        self.write_parquet(
            df_clubs_profile.repartition(1),
            "tech-challenge-3-raw/ClubsProfile/",
        )
