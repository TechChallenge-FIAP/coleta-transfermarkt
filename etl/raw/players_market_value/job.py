from dataclasses import dataclass

from etl.raw.players_market_value.business_rules.business_rules import BusinessRules


@dataclass
class PlayersMarketValueJob(BusinessRules):
    def main(self):
        df_players_market_value = self.read_raw("amostra/landing/PlayersMarketValue/")

        df_exploded = (
            df_players_market_value.transform(self.explode_out_rows)
            .transform(self.final_schema)
            .dropDuplicates()
        )

        self.write_csv(df_exploded.repartition(1), "amostra/raw/PlayersMarketValue/")
