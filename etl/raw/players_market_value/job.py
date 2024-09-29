from dataclasses import dataclass

from etl.utils.utils import DefaultUtils


@dataclass
class RawPlayersMarketValueJob(DefaultUtils):
    def main(self):
        df_players_market_value = self.read_json(
            "tech-challenge-3-landing-zone/PlayersMarketValue/"
        )

        self.write_parquet(
            df_players_market_value.repartition(1),
            "tech-challenge-3-raw/PlayersMarketValue/",
        )
