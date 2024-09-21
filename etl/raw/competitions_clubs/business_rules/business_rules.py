from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from utils.utils import DefaultUtils


@dataclass
class BusinessRules(DefaultUtils):
    def explode_out_rows(self, df: DataFrame) -> DataFrame:
        df_exploded = df.withColumn("ROWS", F.explode_outer("clubs"))

        return df_exploded

    def final_schema(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("id").alias("competition_id"),
            F.col("name").alias("competition_name"),
            F.col("seasonID").alias("season_year"),
            F.col("ROWS.id").alias("club_id"),
            F.col("ROWS.name").alias("club_name"),
        )
