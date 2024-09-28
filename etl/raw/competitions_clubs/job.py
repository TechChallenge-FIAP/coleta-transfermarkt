from dataclasses import dataclass

from etl.utils.utils import DefaultUtils


@dataclass
class RawCompetitionsClubsJob(DefaultUtils):
    def main(self):
        df_competitions_clubs = self.read_json(
            "tech-challenge-3-landing-zone/CompetitionClubs/"
        )
        df_competitions_clubs.printSchema()
        df_competitions_clubs.show(10,truncate=False)

        # self.write_parquet(
        #     df_competitions_clubs.repartition(1),
        #     "tech-challenge-3-raw/CompetitionClubs/",
        # )
