from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from etl.utils.utils import DefaultUtils


@dataclass
class BusinessRules(DefaultUtils):
    def explode_out_rows(self, df: DataFrame) -> DataFrame:
        df_exploded = df.withColumn("ROWS", F.explode_outer("transfers"))

        return df_exploded

    def final_schema(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("id").alias("player_id"),
            F.col("ROWS.id").alias("transfer_id"),
            F.col("ROWS.clubFrom.clubID").alias("club_from_id"),
            F.col("ROWS.clubFrom.clubName").alias("club_from_name"),
            F.col("ROWS.clubTo.clubID").alias("club_to_id"),
            F.col("ROWS.clubTo.clubName").alias("club_to_name"),
            F.col("ROWS.date").alias("dt_transfer"),
            F.col("ROWS.upcoming").alias("upcoming"),
            F.col("ROWS.season").alias("season_year"),
            F.col("ROWS.fee").alias("fee"),
        )
