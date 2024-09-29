from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from etl.utils.utils import DefaultUtils


@dataclass
class BusinessRules(DefaultUtils):
    def explode_out_rows(self, df: DataFrame) -> DataFrame:
        df_exploded = df.withColumn("ROWS", F.explode_outer("marketValueHistory"))

        return df_exploded

    def final_schema(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("id").alias("player_id"),
            F.col("marketValue").alias("current_value"),
            F.col("ROWS.clubID").alias("club_id"),
            F.col("ROWS.clubName").alias("club_name"),
            F.col("ROWS.age").alias("player_age"),
            F.col("ROWS.date").alias("dt_market_value"),
            F.col("ROWS.value").alias("market_value"),
        )
