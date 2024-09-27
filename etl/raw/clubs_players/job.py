from dataclasses import dataclass

from etl.raw.clubs_players.business_rules.business_rules_players import BusinessRulesPlayers



from dataclasses import dataclass
from pyspark.sql import DataFrame

@dataclass
class   ClubsPlayersJob(BusinessRulesPlayers):
    def main(self):
        # Leitura dos dados brutos
        df_players = self.read_raw("./amostra/landing/ClubsPlayers/ClubsPlayers.json")

        # Aplicação das transformações: explode e reesquema
        df_exploded = (
            df_players.transform(self.explode_out_rows)  # Expande a coluna 'players'
            .transform(self.final_schema)  # Aplica o esquema final
            .dropDuplicates()  # Remove duplicatas, se houver
        )

        # Escrita do DataFrame transformado como CSV
        self.write_csv(df_exploded.repartition(1), "etl/raw/ClubsPlayers/")

