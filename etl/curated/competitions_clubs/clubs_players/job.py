from dataclasses import dataclass


from etl.curated.competitions_clubs.clubs_players.business_rules_players import BusinessRulesPlayers


@dataclass
class CuratedClubsPlayersJob(BusinessRulesPlayers):
    def main(self):
        df_clubs_players = self.read_parquet(
            "tech-challenge-3-raw/ClubsPlayers/"
        )

        df_exploded = (
            df_clubs_players.transform(self.explode_out_rows)
            .transform(self.final_schema)
            .dropDuplicates()
        )
        
        df_exploded.show()

        # self.write_parquet(
        #     df_exploded.repartition(1), "tech-challenge-3-curated/ClubsPlayers/"
        # )
