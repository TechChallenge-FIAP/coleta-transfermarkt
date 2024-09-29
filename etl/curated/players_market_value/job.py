from dataclasses import dataclass

from etl.curated.players_market_value.business_rules.business_rules import BusinessRules


@dataclass
class CuratedPlayersMarketValueJob(BusinessRules):
    def main(self):
        df_players_market_value = self.read_parquet(
            "tech-challenge-3-raw/PlayersMarketValue/"
        )

        df_cleaned = (
            df_players_market_value.transform(self.explode_out_rows)
            .transform(self.final_schema)
            .transform(self.fix_date, "dt_market_value")
            .dropDuplicates()
        )

        self.write_parquet(
            df_cleaned.repartition(1), "tech-challenge-3-curated/PlayersMarketValue/"
        )
