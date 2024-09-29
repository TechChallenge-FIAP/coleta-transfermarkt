from dataclasses import dataclass

from etl.curated.clubs_profile.business_rules.business_rules import BusinessRules


@dataclass
class CuratedClubsProfileJob(BusinessRules):
    def main(self):
        df_clubs_profile = self.read_parquet("tech-challenge-3-raw/ClubsProfile/")

        df_clubs_profile = df_clubs_profile.transform(
            self.final_schema
        ).dropDuplicates()

        self.write_parquet(
            df_clubs_profile.repartition(1), "tech-challenge-3-curated/ClubsProfile/"
        )
