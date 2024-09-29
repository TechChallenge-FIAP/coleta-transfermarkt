from dataclasses import dataclass

from etl.utils.utils import DefaultUtils


@dataclass
class RawPlayersTransfersJob(DefaultUtils):
    def main(self):
        df_players_transfers = self.read_json(
            "tech-challenge-3-landing-zone/PlayersTransfers/"
        )

        self.write_parquet(
            df_players_transfers.repartition(1),
            "tech-challenge-3-raw/PlayersTransfers/",
        )
