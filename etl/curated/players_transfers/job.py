from dataclasses import dataclass

from etl.curated.players_transfers.business_rules.business_rules import BusinessRules


@dataclass
class CuratedPlayersTransfersJob(BusinessRules):
    def main(self):
        df_players_transfers = self.read_parquet(
            "tech-challenge-3-raw/PlayersTransfers/"
        )

        df_cleaned = (
            df_players_transfers.transform(self.explode_out_rows)
            .transform(self.final_schema)
            .transform(self.fix_date, "dt_transfer")
            .dropDuplicates()
        )

        self.write_parquet(
            df_cleaned.repartition(1), "tech-challenge-3-curated/PlayersTransfers/"
        )
