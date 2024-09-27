from dataclasses import dataclass

from etl.raw.players_transfers.business_rules.business_rules import BusinessRules


@dataclass
class PlayersTransfersJob(BusinessRules):
    def main(self):
        df_players_transfers = self.read_raw("amostra/landing/PlayersTransfers/")

        df_exploded = (
            df_players_transfers.transform(self.explode_out_rows)
            .transform(self.final_schema)
            .dropDuplicates()
        )

        self.write_csv(df_exploded.repartition(1), "amostra/raw/PlayersTransfers/")
