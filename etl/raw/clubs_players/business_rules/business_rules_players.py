from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from etl.utils.utils import DefaultUtils


from dataclasses import dataclass


@dataclass
class BusinessRulesPlayers(DefaultUtils):
    
    def explode_out_rows(self, df: DataFrame) -> DataFrame:
        df_exploded = df.withColumn("ROWS", F.explode_outer("players"))
        return df_exploded

    def final_schema(self, df: DataFrame) -> DataFrame:
    
        return df.select(
            F.col("id").alias("club_id"),
            F.col("name").alias("club_name"),
            F.col("ROWS.id").alias("player_id"),
            F.col("ROWS.name").alias("player_name"),
            F.col("ROWS.position").alias("player_position"),
            F.col("ROWS.dateOfBirth").alias("player_dob"),
            F.col("ROWS.age").alias("player_age"),
            F.col("ROWS.nationality").alias("player_nationality"),
            F.col("ROWS.height").alias("player_height"),
            F.col("ROWS.foot").alias("player_foot"),
            F.col("ROWS.joinedOn").alias("player_joined_on"),
            F.col("ROWS.joined").alias("player_joined"),
            F.col("ROWS.signedFrom").alias("player_signed_from"),
            F.col("ROWS.contract").alias("player_contract"),
            F.col("ROWS.marketValue").alias("player_market_value"),
            F.col("ROWS.status").alias("player_status")
        )

