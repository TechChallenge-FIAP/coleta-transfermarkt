from dataclasses import dataclass

from etl.curated.players_stats.business_rules.business_rules import BusinessRules


@dataclass
class CuratedPlayersStats(BusinessRules):
    def main(self):
        df_players_stats = self.read_parquet("tech-challenge-3-raw/PlayersStats/")

        df_exploded = (
            df_players_stats.transform(self.explode_out_rows)
            .transform(self.final_schema)
            .dropDuplicates()
        )

        self.write_parquet(
            df_exploded.repartition(1), "tech-challenge-3-curated/PlayersStats/"
        )
