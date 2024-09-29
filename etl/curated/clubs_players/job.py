from dataclasses import dataclass

from etl.curated.clubs_players.business_rules.business_rules import BusinessRules


@dataclass
class CuratedClubsPlayersJob(BusinessRules):
    def main(self):
        df_clubs_players = self.read_parquet("tech-challenge-3-raw/ClubsPlayers/")

        df_exploded = (
            df_clubs_players.transform(self.explode_out_rows)
            .transform(self.final_schema)
            .transform(self.fix_date, "player_dob")
            .transform(self.fix_date, "player_joined_on")
            .transform(self.fix_date, "player_contract")
            .dropDuplicates()
        )

        self.write_parquet(
            df_exploded.repartition(1), "tech-challenge-3-curated/ClubsPlayers/"
        )
