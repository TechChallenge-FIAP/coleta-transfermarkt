from dataclasses import dataclass

from etl.curated.players_profile.business_rules.business_rules import BusinessRules


@dataclass
class CuratedPlayersProfileJob(BusinessRules):
    def main(self):
        df_players_profile = self.read_parquet("tech-challenge-3-raw/PlayersProfile/")

        df_cleaned = (
            df_players_profile.transform(self.final_schema)
            .transform(self.fix_date, "actual_club_joined")
            .transform(self.fix_date, "actual_club_contract_expires")
            .dropDuplicates()
        )

        self.write_parquet(
            df_cleaned.repartition(1), "tech-challenge-3-curated/PlayersProfile/"
        )
