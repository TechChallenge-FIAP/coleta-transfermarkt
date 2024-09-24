from dataclasses import dataclass

from etl.raw.competitions_clubs.business_rules.business_rules import BusinessRules


@dataclass
class CompetitionsClubsJob(BusinessRules):
    def main(self):
        df_competitions_clubs = self.read_raw("amostra/landing/CompetitionClubs/")

        df_exploded = (
            df_competitions_clubs.transform(self.explode_out_rows)
            .transform(self.final_schema)
            .dropDuplicates()
        )

        self.write_csv(df_exploded.repartition(1), "amostra/raw/CompetitionClubs/")
