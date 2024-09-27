from dataclasses import dataclass

from etl.curated.competitions_clubs.business_rules.business_rules import BusinessRules


@dataclass
class CuratedCompetitionsClubsJob(BusinessRules):
    def main(self):
        df_competitions_clubs = self.read_parquet(
            "tech-challenge-3-raw/CompetitionClubs/"
        )

        df_exploded = (
            df_competitions_clubs.transform(self.explode_out_rows)
            .transform(self.final_schema)
            .dropDuplicates()
        )

        self.write_parquet(
            df_exploded.repartition(1), "tech-challenge-3-curated/CompetitionClubs/"
        )
