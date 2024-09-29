from dataclasses import dataclass

from etl.curated.players_injuries.business_rules.business_rules import BusinessRules


@dataclass
class CuratedPlayersInjuries(BusinessRules):
    def main(self):
        df_players_injuries = self.read_parquet("tech-challenge-3-raw/PlayersInjuries/")

        df_exploded = (
            df_players_injuries.transform(self.explode_out_rows)
            .transform(self.final_schema)
            .transform(self.fix_date, "injury_from")
            .transform(self.fix_date, "until")
            .dropDuplicates()
        )

        self.write_parquet(
            df_exploded.repartition(1), "tech-challenge-3-curated/PlayersInjuries/"
        )
